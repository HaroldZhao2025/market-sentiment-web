# src/market_sentiment/prices.py
from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List
import pandas as pd
import yfinance as yf

def _one_ticker_hist(ticker: str, start: str, end: str) -> pd.DataFrame:
    try:
        t = yf.Ticker(ticker)
        df = t.history(start=start, end=end, interval="1d", auto_adjust=False)
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.reset_index().rename(columns=str.lower)
        # yfinance returns 'date' tz-aware UTC or tz-naive; normalize to date (naive)
        df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce").dt.tz_convert("America/New_York").dt.normalize().dt.tz_localize(None)
        df["ticker"] = ticker
        # keep standard columns
        keep = ["date","ticker","open","high","low","close","volume"]
        return df[keep].dropna(subset=["date","close"])
    except Exception:
        return pd.DataFrame()

def fetch_prices_yf(tickers: List[str], start: str, end: str, max_workers: int = 8) -> pd.DataFrame:
    dfs = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_one_ticker_hist, t, start, end): t for t in tickers}
        for fut in as_completed(futs):
            df = fut.result()
            if df is not None and not df.empty:
                dfs.append(df)
    if not dfs:
        return pd.DataFrame(columns=["date","ticker","open","high","low","close","volume"])
    out = pd.concat(dfs, ignore_index=True)
    out = out.drop_duplicates(["ticker","date"]).sort_values(["ticker","date"])
    return out
