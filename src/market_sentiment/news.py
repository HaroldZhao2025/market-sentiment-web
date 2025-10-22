# src/market_sentiment/news.py
from __future__ import annotations

from typing import List

import pandas as pd

from .news_finnhub_daily import fetch_finnhub_daily
from .news_yfinance import fetch_yfinance_recent


def _dedup_and_sort(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    df["url"] = df["url"].fillna("")
    df = df.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    return df[["ticker", "ts", "title", "url", "text"]]


def fetch_news(
    ticker: str,
    start: str,
    end: str,
    *,
    yfinance_count: int = 240,
    finnhub_rps: int = 10,
    keep_source: bool = False,
) -> pd.DataFrame:
    """
    Merge Finnhub (daily loop) + Yahoo Finance (count=240) for full coverage of [start, end].
    """
    frames: List[pd.DataFrame] = []

    # Finnhub day-by-day
    try:
        fh = fetch_finnhub_daily(ticker, start, end, rps=int(finnhub_rps))
        if keep_source and not fh.empty:
            fh = fh.assign(_src="finnhub")
    except Exception:
        fh = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    if not fh.empty:
        frames.append(fh)

    # Yahoo Finance recent
    try:
        yf = fetch_yfinance_recent(ticker, start, end, count=int(yfinance_count), tab="all")
        if keep_source and not yf.empty:
            yf = yf.assign(_src="yfinance")
    except Exception:
        yf = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    if not yf.empty:
        frames.append(yf)

    if not frames:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    merged = pd.concat(frames, ignore_index=True)
    return _dedup_and_sort(merged)
