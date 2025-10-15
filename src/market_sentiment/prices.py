# src/market_sentiment/prices.py
from __future__ import annotations

from typing import Iterable, List
import pandas as pd
import yfinance as yf


def _normalize_download(df: pd.DataFrame, tickers: List[str]) -> pd.DataFrame:
    """
    yfinance.download(...) returns:
      - MultiIndex columns when multiple tickers requested
      - Single-index columns when one ticker
    Normalize to long tidy frame with columns: date,ticker,open,high,low,close,adj_close,volume
    """
    if df.empty:
        return pd.DataFrame(columns=["date","ticker","open","high","low","close","adj_close","volume"])

    if isinstance(df.columns, pd.MultiIndex):
        frames = []
        top = set([lvl for lvl in df.columns.get_level_values(0)])
        for t in tickers:
            if t not in top:
                continue
            sub = df[t].reset_index()
            sub.columns = [c.lower().replace(" ", "_") for c in sub.columns]
            sub["ticker"] = t
            # Ensure adj_close present
            if "adj_close" not in sub.columns and "adj close" in sub.columns:
                sub = sub.rename(columns={"adj close": "adj_close"})
            frames.append(sub)
        out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    else:
        # single ticker path
        sub = df.reset_index()
        sub.columns = [c.lower().replace(" ", "_") for c in sub.columns]
        sub["ticker"] = tickers[0] if tickers else ""
        if "adj_close" not in sub.columns and "adj close" in sub.columns:
            sub = sub.rename(columns={"adj close": "adj_close"})
        out = sub

    if out.empty:
        return pd.DataFrame(columns=["date","ticker","open","high","low","close","adj_close","volume"])

    # Minimal set guaranteed
    for c in ["open","high","low","close","adj_close","volume"]:
        if c not in out.columns:
            out[c] = pd.NA

    out["date"] = pd.to_datetime(out["date"]).dt.tz_localize(None)
    out = out[["date","ticker","open","high","low","close","adj_close","volume"]].sort_values(["ticker","date"]).reset_index(drop=True)
    return out


def fetch_prices_yf(tickers: Iterable[str], start: str, end: str) -> pd.DataFrame:
    ts = [t for t in tickers if isinstance(t, str) and t]
    if not ts:
        return pd.DataFrame(columns=["date","ticker","open","high","low","close","adj_close","volume"])

    raw = yf.download(
        ts,
        start=start,
        end=end,
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        progress=False,
        threads=False,  # more predictable on CI
    )
    return _normalize_download(raw, ts)
