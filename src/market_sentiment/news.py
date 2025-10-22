# src/market_sentiment/news.py
from __future__ import annotations

from typing import Callable, List, Optional

import pandas as pd

from .news_finnhub_daily import fetch_finnhub_daily
from .news_yfinance import fetch_yfinance_recent

# Public merged fetch ----------------------------------------------------------

def fetch_news(
    ticker: str,
    start: str,
    end: str,
    *,
    yfinance_count: int = 240,
    finnhub_rps: int = 10,
    keep_source: bool = False,  # for debugging
) -> pd.DataFrame:
    df_fh = fetch_finnhub_daily(ticker, start, end, rps=finnhub_rps)
    df_yf = fetch_yfinance_recent(ticker, start, end, count=yfinance_count)

    frames = [d for d in (df_fh, df_yf) if d is not None and not d.empty]
    if not frames:
        cols = ["ticker", "ts", "title", "url", "text"]
        if keep_source:
            cols += ["src"]
        return pd.DataFrame(columns=cols)

    out = pd.concat(frames, ignore_index=True)
    out["url"] = out["url"].fillna("")
    out = out.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    if keep_source:
        # annotate which source; best-effort by URL/domain presence
        def src_guess(row):
            u = (row.get("url") or "").lower()
            if "finnhub.io/api/news" in u:
                return "finnhub"
            if "finance.yahoo.com" in u or "yahoo.com" in u:
                return "yfinance"
            return "merged"
        out["src"] = out.apply(src_guess, axis=1)
    return out

# Back-compat provider names expected in older smoke tests ---------------------

def _prov_finnhub(ticker: str, start: str, end: str, *_, **__) -> pd.DataFrame:
    """Wrapper so old smoke tests keep working (ignores extra args)."""
    return fetch_finnhub_daily(ticker, start, end)

def _prov_yfinance(ticker: str, start: str, end: str, *_, **__) -> pd.DataFrame:
    """Wrapper for legacy test; always asks for 240 + tab=all."""
    return fetch_yfinance_recent(ticker, start, end, count=240)

# Leave the other legacy names present as no-ops to avoid import errors.
def _empty_provider(*args, **kwargs) -> pd.DataFrame:
    return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

_prov_google_rss = _empty_provider
_prov_yahoo_rss  = _empty_provider
_prov_nasdaq_rss = _empty_provider
_prov_gdelt      = _empty_provider
