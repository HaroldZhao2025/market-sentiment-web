from __future__ import annotations

import json
import os
import math
from typing import Dict, Iterable, List, Optional, Any

import numpy as np
import pandas as pd


# ---------- small utils ----------

def _ensure_dir(d: str) -> None:
    os.makedirs(d, exist_ok=True)


def _fmt_eastern(ts: pd.Timestamp) -> str:
    """Format a UTC timestamp to America/New_York ISO-like string."""
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return ""
    t = pd.to_datetime(ts, utc=True, errors="coerce")
    if pd.isna(t):
        return ""
    try:
        et = t.tz_convert("America/New_York")
    except Exception:
        et = t.tz_localize("UTC").tz_convert("America/New_York")
    return et.strftime("%Y-%m-%d %H:%M:%S")


def _safe_num(x) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else 0.0
    except Exception:
        return 0.0


def _roll_ma(arr: List[float], n: int = 7) -> List[float]:
    out: List[float] = []
    run = 0.0
    q: List[float] = []
    for v in arr:
        v = 0.0 if v is None or (isinstance(v, float) and math.isnan(v)) else float(v)
        q.append(v)
        run += v
        if len(q) > n:
            run -= q.pop(0)
        out.append(run / n if len(q) >= n else float("nan"))
    return out


def _all_almost_zero(vals: Iterable[float], eps: float = 1e-12) -> bool:
    for v in vals:
        try:
            if abs(float(v)) > eps:
                return False
        except Exception:
            pass
    return True


# ---------- sentiment fallback from sparse news ----------

def _gaussian_kernel(days: int = 7, sigma: float = 2.0) -> np.ndarray:
    """Odd-length symmetric kernel for smoothing/“spreading” sparse news counts."""
    L = max(3, int(days) | 1)  # ensure odd
    r = (L - 1) // 2
    x = np.arange(-r, r + 1, dtype=float)
    k = np.exp(-(x**2) / (2.0 * float(sigma) ** 2))
    k /= k.sum()
    return k


def _smooth_and_scale_to_unit(series: pd.Series) -> pd.Series:
    """Z-score -> tanh to [-1,1]."""
    arr = series.astype(float).values
    mu = float(np.mean(arr))
    sd = float(np.std(arr))
    if sd <= 1e-12:
        z = np.zeros_like(arr, dtype=float)
    else:
        z = (arr - mu) / sd
    s = np.tanh(z / 2.0)
    return pd.Series(s, index=series.index, dtype=float)


def _news_fallback_S_full_window(price_days: pd.DatetimeIndex, news_t: pd.DataFrame) -> List[float]:
    """
    Build a daily S for *every trading day* by:
      1) counting news at day granularity over the full date index,
      2) convolving with a short gaussian kernel to “spread” sparse impulses,
      3) z-score and squash into [-1, 1].
    """
    if news_t is None or news_t.empty or len(price_days) == 0:
        return [0.0] * len(price_days)

    g = (
        news_t.assign(day=news_t["ts"].dt.floor("D"))
        .groupby("day", as_index=False)
        .size()
        .rename(columns={"size": "cnt"})
        .set_index("day")["cnt"]
    )
    # align to full window (including days with zero news)
    aligned = g.reindex(price_days, fill_value=0).astype(float)

    # light smoothing / spreading so we have daily signal
    k = _gaussian_kernel(days=7, sigma=2.0)
    smoothed = np.convolve(aligned.values, k, mode="same")
    smoothed = pd.Series(smoothed, index=aligned.index)

    S = _smooth_and_scale_to_unit(smoothed).clip(-1.0, 1.0)
    return [float(x) for x in S.tolist()]


# ---------- core helpers ----------

def _build_one_ticker(
    t: str,
    panel: pd.DataFrame,
    news_rows: Optional[pd.DataFrame],
    headlines_max: int = 10,  # UI shows 10 headlines
) -> Dict:
    """
    Build the ticker payload used by the Next.js page.
    Expected panel columns (per ticker daily): date (UTC-naive), ticker, close, S (or sentiment).
    If S is missing or ~all zeros and we have news, synthesize fallback S for ALL trading days.
    """
    df = panel[panel["ticker"] == t].copy()
    if df.empty or "date" not in df.columns:
        return {}

    # normalize dtypes
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    # price
    if "close" not in df.columns:
        df["close"] = pd.NA
    price = pd.to_numeric(df["close"], errors="coerce").fillna(0.0).astype(float).tolist()

    # S (keep upstream if present)
    if "S" not in df.columns and "sentiment" in df.columns:
        df["S"] = df["sentiment"]
    if "S" not in df.columns:
        df["S"] = 0.0
    df["S"] = pd.to_numeric(df["S"], errors="coerce").fillna(0.0)

    # window for news filtering (day-level)
    start_day = df["date"].min().floor("D")
    end_day = df["date"].max().floor("D")
    price_days = pd.DatetimeIndex(df["date"].dt.floor("D").unique())

    # select news for ticker, within window (day-based)
    nt = pd.DataFrame(columns=["ts", "title", "url", "text"])
    if news_rows is not None and len(news_rows) > 0:
        nr = news_rows[news_rows["ticker"] == t].copy()
        if len(nr) > 0:
            nr["ts"] = pd.to_datetime(nr["ts"], utc=True, errors="coerce")
            nr = nr.dropna(subset=["ts"])
            nr["_day"] = nr["ts"].dt.floor("D")
            in_window = (nr["_day"] >= start_day) & (nr["_day"] <= end_day)
            nt = nr.loc[in_window, ["ts", "title", "url", "text"]].sort_values("ts")
            if nt.empty:
                # keep *some* headlines even if filter erased all
                nt = nr.sort_values("ts", ascending=False)[["ts", "title", "url", "text"]].head(1000)
                nt = nt.sort_values("ts")

    # upstream S (if almost all zero) -> synthesize a daily curve from sparse news
    s = pd.to_numeric(df["S"], errors="coerce").fillna(0.0).astype(float).tolist()
    if _all_almost_zero(s) and not nt.empty:
        s = _news_fallback_S_full_window(price_days, nt)

    # final smoothing (MA7) for visualization
    s_ma7 = _roll_ma(s, n=7)

    # news headlines list (newest first, capped to 10)
    out_news: List[Dict[str, Any]] = []
    if not nt.empty:
        for _, r in nt.sort_values("ts", ascending=False).head(int(headlines_max)).iterrows():
            out_news.append({
                "ts": _fmt_eastern(r.get("ts")),
                "title": str(r.get("title", "")),
                "url": str(r.get("url", "")),
                "text": str(r.get("text", "")),
            })

    # dates -> "YYYY-MM-DD" (explicit UTC)
    dates_str = [
        (d if getattr(d, "tzinfo", None) else d.tz_localize("UTC")).tz_convert("UTC").strftime("%Y-%m-%d")
        for d in df["date"]
    ]

    return {
        "symbol": t,
        "date": dates_str,
        "price": price,
        "S": [round(_safe_num(x), 6) for x in s],
        "S_ma7": [round(x, 6) if math.isfinite(x) else 0.0 for x in s_ma7],
        "news": out_news,
    }


def _write_json(path: str, obj: Dict) -> None:
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)


# ---------- public API ----------

def write_outputs(panel, news_rows, *rest):
    """
    Compatible with both calling patterns in your pipeline:
      - write_outputs(panel, news_rows, out_dir)
      - write_outputs(panel, news_rows, earn_rows, out_dir)

    Emits:
      • /ticker/<T>.json  (includes daily “S” and “S_ma7” for the full window)
      • _tickers.json
      • portfolio.json    (mean S per day, plus aggregate mean CLOSE as 'price')
      • /earnings/<T>.json (optional stub)
    """
    # parse args
    if len(rest) == 1:
        earn_rows = None
        out_dir = rest[0]
    elif len(rest) >= 2:
        earn_rows, out_dir = rest[0], rest[1]
    else:
        raise TypeError("write_outputs(panel, news_rows, [earn_rows,] out_dir)")

    if panel is None or len(panel) == 0:
        raise ValueError("Empty panel passed to write_outputs")

    out_dir = str(out_dir)
    tick_dir = os.path.join(out_dir, "ticker")
    earn_dir = os.path.join(out_dir, "earnings")
    _ensure_dir(out_dir); _ensure_dir(tick_dir); _ensure_dir(earn_dir)

    # standardize panel
    panel = panel.copy()
    if "date" not in panel.columns:
        raise KeyError("panel must contain 'date' column")
    if "ticker" not in panel.columns:
        raise KeyError("panel must contain 'ticker' column")
    panel["ticker"] = panel["ticker"].astype(str).str.upper()
    panel["date"] = pd.to_datetime(panel["date"], utc=True, errors="coerce")

    # normalize news rows columns if present
    if news_rows is not None and len(news_rows) > 0:
        nr = news_rows.copy()
        for c in ("ticker", "ts", "title", "url", "text"):
            if c not in nr.columns:
                nr[c] = pd.NA
        nr["ticker"] = nr["ticker"].astype(str).str.upper()
        news_rows = nr
    else:
        news_rows = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    tickers = sorted(panel["ticker"].dropna().unique().tolist())
    _write_json(os.path.join(out_dir, "_tickers.json"), tickers)

    # per-ticker JSON + aggregate portfolio from the *final* S
    pf_acc: Dict[pd.Timestamp, List[float]] = {}
    for t in tickers:
        obj = _build_one_ticker(t, panel, news_rows, headlines_max=10)  # 10 headlines; S spans all days
        if not obj or not obj.get("date", []) or (not obj.get("price", []) and not obj.get("S", [])):
            continue
        _write_json(os.path.join(tick_dir, f"{t}.json"), obj)

        dates = [pd.to_datetime(d, utc=True) for d in obj["date"]]
        svals = [float(x) for x in obj["S"]]
        for d, s in zip(dates, svals):
            pf_acc.setdefault(d, []).append(s)

    # portfolio.json = mean of S across tickers per day
    if pf_acc:
        days_sorted = sorted(pf_acc.keys())
        pf_dates = [d.strftime("%Y-%m-%d") for d in days_sorted]
        pf_S = [round(sum(vals) / max(1, len(vals)), 6) for vals in (pf_acc[d] for d in days_sorted)]
    else:
        pf_dates, pf_S = [], []

    # also output an aggregate price curve (simple cross-sectional mean of CLOSE per day)
    if "close" in panel.columns:
        agg_price = (
            panel.dropna(subset=["date"])
            .groupby("date", as_index=False)["close"]
            .mean(numeric_only=True)
            .sort_values("date")
        )
        agg_price["date"] = pd.to_datetime(agg_price["date"], utc=True)
        price_map = {d.strftime("%Y-%m-%d"): float(v) for d, v in zip(agg_price["date"], agg_price["close"])}
        pf_price = [price_map.get(d, None) for d in pf_dates]
    else:
        pf_price = None

    obj_pf = {"dates": pf_dates, "S": pf_S}
    if pf_price:
        obj_pf["price"] = pf_price  # used by /portfolio overlay
    _write_json(os.path.join(out_dir, "portfolio.json"), obj_pf)

    # optional minimal earnings stub
    if earn_rows is not None and len(earn_rows) > 0:
        er = earn_rows.copy()
        if "ticker" in er.columns and "date" in er.columns:
            er["ticker"] = er["ticker"].astype(str).str.upper()
            er["date"] = pd.to_datetime(er["date"], errors="coerce")
            for t in tickers:
                sub = er[er["ticker"] == t].dropna(subset=["date"])
                if len(sub) == 0:
                    continue
                items = sub.sort_values("date")["date"].dt.date.astype(str).to_list()
                _write_json(os.path.join(earn_dir, f"{t}.json"), {"earnings": [{"date": d} for d in items]})
