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
        q.append(v); run += v
        if len(q) > n:
            run -= q.pop(0)
        out.append(run / n if len(q) >= n else float("nan"))
    return out

def _nonzero_fraction(vals: Iterable[float], eps: float = 1e-12) -> float:
    vals = list(vals)
    if not vals:
        return 0.0
    nz = 0
    for v in vals:
        try:
            if abs(float(v)) > eps:
                nz += 1
        except Exception:
            pass
    return nz / max(1, len(vals))

def _too_sparse(vals: Iterable[float], min_frac: float = 0.25, min_nz_days: int = 40) -> bool:
    vals = list(vals)
    nz = sum(1 for v in vals if abs(float(v or 0.0)) > 1e-12)
    return (nz < min_nz_days) or (nz / max(1, len(vals)) < min_frac)

# ---------- daily sentiment from news ----------

def _gaussian_kernel(days: int = 5, sigma: float = 1.6) -> np.ndarray:
    L = max(3, int(days) | 1)  # odd
    r = (L - 1) // 2
    x = np.arange(-r, r + 1, dtype=float)
    k = np.exp(-(x**2) / (2.0 * float(sigma) ** 2))
    k /= k.sum()
    return k

def _intensity_fallback(price_days: pd.DatetimeIndex, news_t: pd.DataFrame) -> List[float]:
    """Use daily news counts -> z-score -> tanh, then smooth; yields [-1,1]."""
    g = (
        news_t.assign(day=news_t["ts"].dt.floor("D"))
        .groupby("day", as_index=False)
        .size()
        .rename(columns={"size": "cnt"})
        .set_index("day")["cnt"]
    )
    aligned = g.reindex(price_days, fill_value=0).astype(float)
    arr = aligned.values
    mu = float(np.mean(arr)); sd = float(np.std(arr))
    z = (arr - mu) / (sd if sd > 1e-12 else 1.0)
    s = np.tanh(z / 2.0)
    k = _gaussian_kernel(5, 1.6)
    out = np.convolve(s, k, mode="same")
    return [float(np.clip(v, -1.0, 1.0)) for v in out.tolist()]

def _news_daily_sentiment(price_days: pd.DatetimeIndex, news_t: pd.DataFrame) -> List[float]:
    """
    Build a DAILY sentiment series across the full price window:
      - Prefer per-headline FinBERT S (mean by day) if available/non-flat.
      - Otherwise fall back to news intensity.
      - Always smooth lightly and clip to [-1, 1].
    """
    if news_t is None or news_t.empty or len(price_days) == 0:
        return [0.0] * len(price_days)

    df = news_t.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])
    df["day"] = df["ts"].dt.floor("D")
    if "S" not in df.columns:
        df["S"] = 0.0
    df["S"] = pd.to_numeric(df["S"], errors="coerce").fillna(0.0).clip(-1.0, 1.0)

    # Per-day mean of headline scores
    daily = df.groupby("day", as_index=False)["S"].mean().set_index("day")["S"]
    aligned = daily.reindex(price_days, fill_value=0.0).astype(float)
    arr = aligned.values

    # If FinBERT isn’t present or all zeros/near-constant, use intensity fallback
    if (np.allclose(arr, 0.0, atol=1e-6)) or (float(np.std(arr)) < 1e-6):
        return _intensity_fallback(price_days, df)

    k = _gaussian_kernel(5, 1.6)
    out = np.convolve(arr, k, mode="same")
    out = np.clip(out, -1.0, 1.0)
    return [float(v) for v in out.tolist()]

# ---------- core helpers ----------

def _build_one_ticker(
    t: str,
    panel: pd.DataFrame,
    news_rows: Optional[pd.DataFrame],
    headlines_max: int = 10,
) -> Dict:
    df = panel[panel["ticker"] == t].copy()
    if df.empty or "date" not in df.columns:
        return {}

    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    # price
    if "close" not in df.columns:
        df["close"] = pd.NA
    price = pd.to_numeric(df["close"], errors="coerce").fillna(0.0).astype(float).tolist()

    # upstream S
    if "S" not in df.columns and "sentiment" in df.columns:
        df["S"] = df["sentiment"]
    if "S" not in df.columns:
        df["S"] = 0.0
    df["S"] = pd.to_numeric(df["S"], errors="coerce").fillna(0.0)

    # news window
    start_day = df["date"].min().floor("D")
    end_day   = df["date"].max().floor("D")
    price_days = pd.DatetimeIndex(df["date"].dt.floor("D").unique())

    # select ALL news in window (headlines trimmed later)
    nt = pd.DataFrame(columns=["ts", "title", "url", "text", "S"])
    if news_rows is not None and len(news_rows) > 0:
        nr = news_rows[news_rows["ticker"] == t].copy()
        if len(nr) > 0:
            nr["ts"] = pd.to_datetime(nr["ts"], utc=True, errors="coerce")
            nr = nr.dropna(subset=["ts"])
            nr["_day"] = nr["ts"].dt.floor("D")
            nt = nr.loc[(nr["_day"] >= start_day) & (nr["_day"] <= end_day), ["ts", "title", "url", "text", "S"]].sort_values("ts")
            if nt.empty:
                nt = nr.sort_values("ts", ascending=False)[["ts", "title", "url", "text", "S"]].head(2000).sort_values("ts")

    s = pd.to_numeric(df["S"], errors="coerce").fillna(0.0).astype(float).tolist()

    # RECOMPUTE if the upstream daily S is too sparse or nearly all zeros
    if (_too_sparse(s) or _nonzero_fraction(s) < 0.25) and not nt.empty:
        s = _news_daily_sentiment(price_days, nt)

    s_ma7 = _roll_ma(s, n=7)

    # news headlines (newest first, STILL cap to 10 for UI)
    out_news: List[Dict[str, Any]] = []
    news_total = int(nt.shape[0])
    news_days = int(nt["ts"].dt.floor("D").nunique()) if not nt.empty else 0
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
        "S": [round(_safe_num(x), 4) for x in s],                # <- 4 decimals
        "S_ma7": [round(x, 4) if math.isfinite(x) else 0.0 for x in s_ma7],  # <- 4 decimals
        "news": out_news,
        "news_total": news_total,     # <- NEW
        "news_days": news_days,       # <- NEW
    }


def _write_json(path: str, obj: Dict) -> None:
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)

# ---------- public API ----------

def write_outputs(panel, news_rows, *rest):
    if len(rest) == 1:
        earn_rows = None; out_dir = rest[0]
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

    panel = panel.copy()
    if "date" not in panel.columns: raise KeyError("panel must contain 'date' column")
    if "ticker" not in panel.columns: raise KeyError("panel must contain 'ticker' column")
    panel["ticker"] = panel["ticker"].astype(str).str.upper()
    panel["date"] = pd.to_datetime(panel["date"], utc=True, errors="coerce")

    if news_rows is not None and len(news_rows) > 0:
        nr = news_rows.copy()
        for c in ("ticker", "ts", "title", "url", "text", "S"):
            if c not in nr.columns: nr[c] = pd.NA
        nr["ticker"] = nr["ticker"].astype(str).str.upper()
        news_rows = nr
    else:
        news_rows = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])

    tickers = sorted(panel["ticker"].dropna().unique().tolist())
    _write_json(os.path.join(out_dir, "_tickers.json"), tickers)

    pf_acc: Dict[pd.Timestamp, List[float]] = {}
    for t in tickers:
        obj = _build_one_ticker(t, panel, news_rows, headlines_max=10)
        if not obj or not obj.get("date", []) or (not obj.get("price", []) and not obj.get("S", [])):
            continue
        _write_json(os.path.join(tick_dir, f"{t}.json"), obj)

        dates = [pd.to_datetime(d, utc=True) for d in obj["date"]]
        svals = [float(x) for x in obj["S"]]
        for d, s in zip(dates, svals):
            pf_acc.setdefault(d, []).append(s)

    if pf_acc:
        days_sorted = sorted(pf_acc.keys())
        pf_dates = [d.strftime("%Y-%m-%d") for d in days_sorted]
        pf_S = [round(sum(vals) / max(1, len(vals)), 6) for vals in (pf_acc[d] for d in days_sorted)]
    else:
        pf_dates, pf_S = [], []
    _write_json(os.path.join(out_dir, "portfolio.json"), {"dates": pf_dates, "S": pf_S})

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
