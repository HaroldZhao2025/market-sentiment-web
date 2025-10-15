# src/market_sentiment/prices.py
from __future__ import annotations
import yfinance as yf
import pandas as pd

def fetch_prices_yf(ticker: str, start: str, end: str) -> pd.DataFrame:
    df = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False)
    if df is None or df.empty:
        return pd.DataFrame(columns=["date","ticker","open","close","adj_close","volume"])
    df = df.reset_index().rename(columns={
        "Date":"date","Open":"open","Close":"close","Adj Close":"adj_close","Volume":"volume"
    })
    df["ticker"] = ticker.upper()
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    return df[["date","ticker","open","close","adj_close","volume"]]
