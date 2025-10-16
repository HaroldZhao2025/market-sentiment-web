# src/market_sentiment/writers.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd


def _as_date_str(s: pd.Series) -> List[str]:
    """Convert a datetime-like series to 'YYYY-MM-DD' strings (tz-naive)."""
    d = pd.to_datetime(s, errors="coerce", utc=True)
    d = d.dt.tz_convert("UTC").dt.tz_localize(None).dt.normalize()
    return d.dt.strftime("%Y-%m-%d").tolist()


def _as_ts_iso(s: pd.Series) -> List[str]:
    """Convert a datetime-like series to ISO 8601 UTC timestamps."""
    d = pd.to_datetime(s, errors="coerce", utc=True)
    # keep time; ensure 'Z' style
    return d.dt.strftime("%Y-%m-%dT%H:%M:%SZ").tolist()


def _safe_float_list(s: pd.Series, fill: float = 0.0) -> List[float]:
    return pd.to_numeric(s, errors="coerce").fillna(fill).astype(float).tolist()


def _safe_int_list(s: pd.Series, fill: int = 0) -> List[int]:
    return pd.to_numeric(s, errors="coerce").fillna(fill).astype(int).tolist()


def _ma(arr: List[float], w: int = 7) -> List[float]:
    if not arr:
        return []
    a = np.asarray(arr, dtype=float)
    if len(a) < w:
        # simple padding behavior: moving average over available length
        out = pd.Series(a).rolling(w, min_periods=1).mean().to_numpy()
        return out.tolist()
    kernel = np.ones(w) / w
    out = np.convolve(a, kernel, mode="same")
    return out.tolist()


def build_ticker_json(
    ticker: str,
    prices: pd.DataFrame,   # expects columns incl. date, close
    daily: pd.DataFrame,    # expects columns incl. date, S, S_news, S_earn
    top_news: pd.DataFrame | None = None,  # columns: ts, title, url, S
) -> Dict:
    """
    Produce a compact JSON dict for one ticker with arrays the web app can render.
    """
    # normalize column names to lower case once
    p = prices.rename(columns=str.lower).copy()
    d = daily.rename(columns=str.lower).copy()

    # keep required columns and sort by date
    p = p[["date", "close"]].copy()
    d = d[["date", "s", "s_news", "s_earn"]].copy() if not d.empty else pd.DataFrame(
        columns=["date", "s", "s_news", "s_earn"]
    )

    # ensure datetime and merge
    p["date"] = pd.to_datetime(p["date"], errors="coerce", utc=True)
    d["date"] = pd.to_datetime(d["date"], errors="coerce", utc=True)

    ser = (
        p.merge(d, on="date", how="left")
         .sort_values("date")
         .fillna({"s": 0.0, "s_news": 0.0, "s_earn": 0.0})
    )

    # arrays
    dates = _as_date_str(ser["date"])
    close = _safe_float_list(ser["close"])
    S      = _safe_float_list(ser["s"])
    S_news = _safe_float_list(ser["s_news"])
    S_earn = _safe_float_list(ser["s_earn"])
    S_ma7  = _ma(S, 7)

    # top news items (optional)
    news_objs: List[Dict] = []
    if top_news is not None and not top_news.empty:
        tn = top_news.rename(columns=str.lower).copy()
        keep = ["ts", "title", "url", "s"]
        tn = tn[[c for c in keep if c in tn.columns]]
        tn = tn.sort_values("ts", ascending=False)
        # choose top by |S| then recency (cap to 20 to keep files light)
        if "s" in tn.columns:
            tn = tn.reindex(tn["s"].abs().sort_values(ascending=False).index)
        tn = tn.head(20)
        news_objs = [
            {
                "ts": _as_ts_iso(pd.Series([r["ts"]]))[0] if pd.notna(r.get("ts")) else None,
                "title": r.get("title") or "",
                "url": r.get("url") or "",
                "S": float(r.get("s") or 0.0),
            }
            for _, r in tn.iterrows()
        ]

    return {
        "ticker": ticker,
        "series": {
            "date": dates,
            "close": close,
            "S": S,
            "S_news": S_news,
            "S_earn": S_earn,
            "S_ma7": S_ma7,
        },
        "news": news_objs,
    }


def _write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"), indent=None)


def _write_earnings_json(base: Path, ticker: str, earn_rows: pd.DataFrame) -> None:
    """
    Write /earnings/<TICKER>.json with a simple array of {ts,title,url,S}.
    """
    if earn_rows is None or earn_rows.empty:
        items: List[Dict] = []
    else:
        e = earn_rows.rename(columns=str.lower).copy()
        keep = ["ts", "title", "url", "s"]
        e = e[[c for c in keep if c in e.columns]].copy()
        # sort by recency, keep up to 50
        e = e.sort_values("ts", ascending=False).head(50)
        items = [
            {
                "ts": _as_ts_iso(pd.Series([r["ts"]]))[0] if pd.notna(r.get("ts")) else None,
                "title": r.get("title") or "",
                "url": r.get("url") or "",
                "S": float(r.get("s") or 0.0),
            }
            for _, r in e.iterrows()
        ]
    _write_json(base / "earnings" / f"{ticker}.json", items)


def write_outputs(
    out_dir: str | Path,
    panel: pd.DataFrame,
    news_rows: pd.DataFrame,
    earn_rows: pd.DataFrame,
) -> None:
    """
    Write all web artifacts under <out_dir>:

      _tickers.json                     -> ["AAPL","MSFT",...]
      ticker/<TICKER>.json              -> per-ticker series + news
      earnings/<TICKER>.json            -> list of {ts,title,url,S}
      portfolio.json (if ret_cc_1d present)
    """
    base = Path(out_dir)
    base.mkdir(parents=True, exist_ok=True)

    # normalize column names once
    panel = panel.rename(columns=str.lower).copy()
    news_rows = news_rows.rename(columns=str.lower).copy() if news_rows is not None else pd.DataFrame(columns=["ticker"])
    earn_rows = earn_rows.rename(columns=str.lower).copy() if earn_rows is not None else pd.DataFrame(columns=["ticker"])

    # figure tickers
    if "ticker" not in panel.columns:
        raise ValueError("panel must have a 'ticker' column")
    tickers = panel["ticker"].dropna().astype(str).str.upper().unique().tolist()

    # write _tickers.json correctly (list of symbols)
    _write_json(base / "_tickers.json", tickers)

    # build a small daily frame per ticker to join to prices
    daily_cols = [c for c in ["date", "ticker", "s", "s_news", "s_earn"] if c in panel.columns]
    daily = panel[daily_cols].drop_duplicates() if daily_cols else pd.DataFrame(columns=["date","ticker","s","s_news","s_earn"])

    # write each ticker
    for t in tickers:
        pf = panel.loc[panel["ticker"].str.upper() == t, ["date", "ticker", "close"]].copy()
        df = daily.loc[daily["ticker"].str.upper() == t, ["date", "ticker", "s", "s_news", "s_earn"]].copy()

        # news slice for this ticker
        nf = news_rows.loc[news_rows.get("ticker","").str.upper() == t, ["ts","title","url","s"]].copy() if not news_rows.empty else pd.DataFrame(columns=["ts","title","url","s"])
        ef = earn_rows.loc[earn_rows.get("ticker","").str.upper() == t, ["ts","title","url","s"]].copy() if not earn_rows.empty else pd.DataFrame(columns=["ts","title","url","s"])

        obj = build_ticker_json(t, pf, df, top_news=nf)
        _write_json(base / "ticker" / f"{t}.json", obj)
        _write_earnings_json(base, t, ef)

    # optional: write a very simple portfolio curve if we have both S and returns
    if {"date","ticker","s","ret_cc_1d"}.issubset(panel.columns):
        # equal-weighted long top decile (S) minus bottom decile each day
        d = panel[["date","ticker","s","ret_cc_1d"]].copy()
        d["date"] = pd.to_datetime(d["date"], errors="coerce", utc=True).dt.tz_localize(None).dt.normalize()
        def _ls(g):
            g = g.dropna(subset=["s"])
            if g.empty:
                return 0.0
            q_hi = g["s"].quantile(0.9)
            q_lo = g["s"].quantile(0.1)
            long = g[g["s"] >= q_hi]["ret_cc_1d"].mean() if (g["s"] >= q_hi).any() else 0.0
            short = g[g["s"] <= q_lo]["ret_cc_1d"].mean() if (g["s"] <= q_lo).any() else 0.0
            return float((long - short))
        pnl = d.groupby("date", as_index=False).apply(lambda g: pd.Series({"ret": _ls(g)}))
        pnl = pnl.sort_values("date")
        out = {
            "date": _as_date_str(pnl["date"]),
            "ret": _safe_float_list(pnl["ret"]),
        }
        _write_json(base / "portfolio.json", out)
    else:
        # still produce an empty file to keep the page happy
        _write_json(base / "portfolio.json", {"date": [], "ret": []})
