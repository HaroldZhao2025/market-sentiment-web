# src/market_sentiment/news.py
from __future__ import annotations

from typing import List, Optional
import pandas as pd

from .news_finnhub_daily import fetch_finnhub_daily
from .news_yfinance import fetch_yfinance_recent


def fetch_news_all_sources(
    ticker: str,
    start: str,
    end: str,
    *,
    finnhub_rps: int = 5,
    finnhub_on429: str = "skip",     # "skip" or "wait"
    yfinance_count: int = 240,
    verbose: bool = False,
) -> pd.DataFrame:
    """
    Merge Finnhub (daily exact calls) + yfinance (recent ~200) within [start,end].

    Returns columns: [ticker, ts, title, url, text] sorted by ts.
    De-duplicates by [title,url].
    """
    frames: List[pd.DataFrame] = []

    # Finnhub daily (robust, rate-limited)
    try:
        df_fh = fetch_finnhub_daily(
            ticker, start, end,
            rps=finnhub_rps,
            on_429=finnhub_on429,
            verbose=verbose,
        )
        if df_fh is not None and not df_fh.empty:
            frames.append(df_fh)
            if verbose:
                days = df_fh["ts"].dt.date.nunique()
                print(f"[news] finnhub: rows={len(df_fh)} days={days}")
        elif verbose:
            print("[news] finnhub: no rows")
    except Exception as e:
        if verbose:
            print(f"[news] finnhub error: {e}")

    # yfinance recent (exact get_news(count=240, tab='all'))
    try:
        df_yf = fetch_yfinance_recent(
            ticker, start, end, count=yfinance_count, tab="all"
        )
        if df_yf is not None and not df_yf.empty:
            frames.append(df_yf)
            if verbose:
                days = df_yf["ts"].dt.date.nunique()
                print(f"[news] yfinance: rows={len(df_yf)} days={days}")
        elif verbose:
            print("[news] yfinance: no rows")
    except Exception as e:
        if verbose:
            print(f"[news] yfinance error: {e}")

    if not frames:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    df = pd.concat(frames, ignore_index=True)
    df["url"] = df["url"].fillna("")
    df = df.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    return df[["ticker", "ts", "title", "url", "text"]]
