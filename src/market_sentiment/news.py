# src/market_sentiment/news.py
from __future__ import annotations

import pandas as pd

from .news_finnhub_daily import fetch_finnhub_daily
from .news_yfinance import fetch_yfinance_recent


def fetch_news(
    ticker: str,
    start: str,
    end: str,
    *,
    finnhub_rps: int = 10,
    finnhub_max_days: int | None = None,
    yf_count: int = 240,
) -> pd.DataFrame:
    """
    Merge Finnhub (day-by-day; freshest) + yfinance (recent ~200) and return
    a single standardized DataFrame with columns:
        ['ticker','ts','title','url','text']
    """
    frames = []

    try:
        df_fh = fetch_finnhub_daily(ticker, start, end, rps=finnhub_rps, max_days=finnhub_max_days)
    except Exception:
        df_fh = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    if not df_fh.empty:
        frames.append(df_fh)

    try:
        df_yf = fetch_yfinance_recent(ticker, start, end, count=yf_count, tab="all")
    except Exception:
        df_yf = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    if not df_yf.empty:
        frames.append(df_yf)

    if not frames:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    df = pd.concat(frames, ignore_index=True)

    # safe de-dup (URL can be empty)
    df["url"] = df["url"].fillna("")
    df = (
        df.drop_duplicates(["title", "url"])
          .sort_values("ts")
          .reset_index(drop=True)
    )
    return df
