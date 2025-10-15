# src/market_sentiment/prices.py
from __future__ import annotations
import pandas as pd, numpy as np
import yfinance as yf

def fetch_prices_yf(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Returns columns: ['date','ticker','open','close','high','low','volume'] (date is tz-naive YYYY-MM-DD)
    """
    t = ticker.upper()
    df = yf.download(t, start=start, end=end, auto_adjust=False, progress=False, interval="1d")
    if df is None or df.empty:
        return pd.DataFrame(columns=["date","ticker","open","high","low","close","volume"])
    df = df.rename(columns=str.lower).reset_index()
    # yfinance gives tz-aware if index is DatetimeIndex with tz; normalize to date-naive
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.normalize()
    out = df[["date","open","high","low","close","volume"]].copy()
    out["ticker"] = t
    return out[["date","ticker","open","high","low","close","volume"]].sort_values(["ticker","date"])

def fetch_panel_yf(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """
    Concatenate all price frames. Robust to missing tickers.
    """
    rows = []
    for t in tickers:
        try:
            p = fetch_prices_yf(t, start, end)
            if not p.empty:
                rows.append(p)
        except Exception:
            continue
    if not rows:
        return pd.DataFrame(columns=["date","ticker","open","high","low","close","volume"])
    df = pd.concat(rows, ignore_index=True)
    df = df.drop_duplicates(["ticker","date"]).sort_values(["ticker","date"])
    return df
