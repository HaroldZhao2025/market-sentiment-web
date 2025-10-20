from __future__ import annotations

import json
import os
import math
from typing import Dict, Iterable, List, Optional, Tuple, Any

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


# ---------- core helpers ----------

def _news_fallback_S(dates_ts: List[pd.Timestamp], news_t: pd.DataFrame) -> List[float]:
    """
    Fallback S from news intensity:
      - count news per calendar day over the *full price window*,
      - z-score across the window,
      - squash to [-1, 1] with tanh(z/2).
    """
    if news_t is None or news_t.empty or not dates_ts:
        return [0.0] * len(dates_ts)

    g = (
        news_t.assign(day=news_t["ts"].dt.floor("D"))
        .groupby("day", as_index=False)
        .size()
        .rename(columns={"size": "cnt"})
    )
    g = g.set_index("day")["cnt"]

    idx = pd.DatetimeIndex([pd.to_datetime(d, utc=True) for d in dates_ts])
    cnt = g.reindex(idx, fill_value=0).astype(float)

    mu = float(cnt.mean())
    sd = float(cnt.std(ddof=0))
    if sd <= 1e-12:
        z = (cnt - mu) * 0.0
    else:
        z = (cnt - mu) / sd

    return [math.tanh(float(x) / 2.0) for x in z.tolist()]


def _build_one_ticker(
    t: str,
    panel: pd.DataFrame,
    news_rows: Optional[pd.DataFrame],
    max_news: int = 400,  # year-scale
) -> Dict:
    """
    Build the ticker payload used by the Next.js page.
    Expected panel columns (per ticker daily): date, ticker, close, S (or sentiment).
    If S is missing or ~all zeros and we have news, synthesize fallback S.
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

    # window for news filtering (DAY-LEVEL to avoid tz edge cases)
    start_day = df["date"].min().floor("D")
    end_day = df["date"].max().floor("D")

    # select news for ticker, within window (day-based)
    nt = pd.DataFrame(columns=["ts", "title", "url", "text"])
    if news_rows is not None and len(news_rows) > 0:
        nr = news_rows[news_rows["ticker"] == t].copy()
        if len(nr) > 0:
            nr["ts"] = pd.to_datetime(nr["ts"], utc=True, errors="coerce")
            nr = nr.dropna(subset=["ts"])
            # day-level filter
            nr["_day"] = nr["ts"].dt.floor("D")
            in_window = (nr["_day"] >= start_day) & (nr["_day"] <= end_day)
            nt = nr.loc[in_window, ["ts", "title", "url", "text"]].sort_values("ts")

            # fallback: if day-filter discards all, still keep latest items we fetched
            if nt.empty:
                nt = nr.sort_values("ts", ascending=False)[["ts", "title", "url", "text"]].head(int(max_news))
                nt = nt.sort_values("ts")

    # fallback S from news intensity if upstream S is basically all zeros
    s = pd.to_numeric(df["S"], errors="coerce").fillna(0.0).astype(float).tolist()
    if _all_almost_zero(s) and not nt.empty:
        s = _news_fallback_S(df["date"].tolist(), nt)

    # smoothing
    s_ma7 = _roll_ma(s, n=7)

    # news slice (keep newest first, cap)
    out_news: List[Dict[str, Any]] = []
    if not nt.empty:
        for _, r in nt.sort_values("ts", ascending=False).head(int(max_news)).iterrows():
            out_news.append({
                "ts": _fmt_eastern(r.get("ts")),
                "title": str(r.get("title", "")),
                "url": str(r.get("url", "")),
                "text": str(r.get("text", "")),
            })

    return {
        "symbol": t,
        "date": [
            (
                d.tz_convert("UTC").strftime("%Y-%m-%d")
                if getattr(d, "tzinfo", None)
                else pd.Timestamp(d, tz="UTC").strftime("%Y-%m-%d")
            )
            for d in df["date"]
        ],
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
    Compatible with both calling patterns seen in your pipeline:
      - write_outputs(panel, news_rows, out_dir)
      - write_outputs(panel, news_rows, earn_rows, out_dir)
    Emits: /ticker/<T>.json, _tickers.json, portfolio.json, and (if provided) earnings stubs.
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

    # normalize news frame columns if present
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

    # build per-ticker JSON and aggregate for portfolio from the final S we output
    pf_acc: Dict[pd.Timestamp, List[float]] = {}
    for t in tickers:
        obj = _build_one_ticker(t, panel, news_rows, max_news=400)
        # if the ticker had no usable data, skip file
        if not obj or not obj.get("date", []) or (not obj.get("price", []) and not obj.get("S", [])):
            continue

        _write_json(os.path.join(tick_dir, f"{t}.json"), obj)

        # contribute to portfolio using the final S we actually wrote
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
    _write_json(os.path.join(out_dir, "portfolio.json"), {"dates": pf_dates, "S": pf_S})

    # optional very-light earnings stub if provided
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
