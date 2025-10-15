# src/market_sentiment/prices.py
from __future__ import annotations
import pandas as pd, numpy as np
import yfinance as yf
from typing import List

def _normalize_price_frame(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["date","ticker","open","high","low","close","volume"])
    df = df.rename(columns=str.lower).reset_index()
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.normalize()
    out = df[["date","open","high","low","close","volume"]].copy()
    out["ticker"] = ticker
    return out[["date","ticker","open","high","low","close","volume"]].dropna(subset=["date","close"])

def fetch_prices_yf(ticker: str, start: str, end: str) -> pd.DataFrame:
    t = ticker.upper()
    try:
        df = yf.download(t, start=start, end=end, auto_adjust=False, progress=False, interval="1d", group_by="column")
    except Exception:
        df = pd.DataFrame()
    return _normalize_price_frame(df, t)

def fetch_panel_yf(tickers: List[str], start: str, end: str, chunk: int = 50) -> pd.DataFrame:
    """
    Batch downloads in chunks to reduce request overhead.
    Returns long format with columns: date, ticker, open, high, low, close, volume
    """
    tickers = [t.upper() for t in tickers]
    frames = []
    for i in range(0, len(tickers), chunk):
        bucket = tickers[i:i+chunk]
        try:
            data = yf.download(bucket, start=start, end=end, auto_adjust=False, progress=False, interval="1d", group_by="ticker", threads=True)
        except Exception:
            data = pd.DataFrame()

        if isinstance(data.columns, pd.MultiIndex):
            # shape: (dates) x (ticker, field)
            for t in bucket:
                if t not in data.columns.get_level_values(0):
                    continue
                sub = data[t].copy()
                sub = sub.rename(columns=str.lower).reset_index()
                sub["date"] = pd.to_datetime(sub["Date" if "Date" in sub.columns else "date"]).dt.tz_localize(None).dt.normalize()
                out = sub.rename(columns={"open":"open","high":"high","low":"low","close":"close","volume":"volume"})
                out["ticker"] = t
                frames.append(out[["date","ticker","open","high","low","close","volume"]].dropna(subset=["date","close"]))
        else:
            # single-frame fallback
            if not data.empty:
                frames.append(_normalize_price_frame(data, bucket[0]))

    if not frames:
        return pd.DataFrame(columns=["date","ticker","open","high","low","close","volume"])
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(["ticker","date"]).sort_values(["ticker","date"])
    return df

def add_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    g = df.sort_values(["ticker","date"]).copy()
    g["ret_cc_1d"] = g.groupby("ticker")["close"].pct_change().shift(-1)
    return g
