# src/market_sentiment/news.py
from __future__ import annotations

from typing import List, Optional
import pandas as pd

# The two exact-source fetchers are kept in separate modules
from .news_finnhub_daily import fetch_finnhub_daily
from .news_yfinance import fetch_yfinance_recent


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])


# -----------------------------------------------------------------------------
# Provider wrappers (compat with older code / smoke tests)
# Signature: (ticker, start, end, company=None, limit=..., **kwargs)
# -----------------------------------------------------------------------------

def _prov_finnhub(
    ticker: str, start: str, end: str,
    company: Optional[str] = None, limit: int = 6000, **kwargs,
) -> pd.DataFrame:
    """
    EXACT Finnhub per-day calls under the hood.
    kwargs supported:
      - finnhub_rps: int (≤30, default 5)
      - finnhub_on429: "skip"|"wait" (default "skip")
      - verbose: bool
    """
    rps = int(kwargs.get("finnhub_rps", 5))
    on429 = str(kwargs.get("finnhub_on429", "skip"))
    verbose = bool(kwargs.get("verbose", False))
    return fetch_finnhub_daily(
        ticker, start, end, rps=rps, on_429=on429, verbose=verbose
    )


def _prov_yfinance(
    ticker: str, start: str, end: str,
    company: Optional[str] = None, limit: int = 240, **kwargs,
) -> pd.DataFrame:
    """
    EXACT yfinance usage: get_news(count=240, tab='all'), then filter to [start,end].
    kwargs supported:
      - yfinance_count: int (default 240)
    """
    count = int(kwargs.get("yfinance_count", limit or 240))
    return fetch_yfinance_recent(ticker, start, end, count=count, tab="all")


# Stubs kept only so old imports don’t crash (return empty frames)
def _prov_gdelt(*args, **kwargs) -> pd.DataFrame: return _empty()
def _prov_google_rss(*args, **kwargs) -> pd.DataFrame: return _empty()
def _prov_yahoo_rss(*args, **kwargs) -> pd.DataFrame: return _empty()
def _prov_nasdaq_rss(*args, **kwargs) -> pd.DataFrame: return _empty()


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------

def fetch_news_all_sources(
    ticker: str,
    start: str,
    end: str,
    *,
    finnhub_rps: int = 5,
    finnhub_on429: str = "skip",      # "skip" or "wait"
    yfinance_count: int = 240,
    verbose: bool = False,
) -> pd.DataFrame:
    """
    Merge Finnhub (day-by-day) + yfinance (recent up to 240 items).
    Returns columns: [ticker, ts, title, url, text], UTC ts, de-duplicated, time-sorted.
    """
    frames: List[pd.DataFrame] = []

    # Finnhub (deep, day-level; obeys rate limit and 429 policy)
    try:
        df_fh = _prov_finnhub(
            ticker, start, end,
            finnhub_rps=finnhub_rps, finnhub_on429=finnhub_on429, verbose=verbose,
        )
        if df_fh is not None and not df_fh.empty:
            frames.append(df_fh)
            if verbose:
                print(f"[news] finnhub rows={len(df_fh)} days={df_fh['ts'].dt.date.nunique()}")
        elif verbose:
            print("[news] finnhub rows=0")
    except Exception as e:
        if verbose:
            print(f"[news] finnhub error: {e}")

    # yfinance (freshest ~200)
    try:
        df_yf = _prov_yfinance(
            ticker, start, end, yfinance_count=yfinance_count
        )
        if df_yf is not None and not df_yf.empty:
            frames.append(df_yf)
            if verbose:
                print(f"[news] yfinance rows={len(df_yf)} days={df_yf['ts'].dt.date.nunique()}")
        elif verbose:
            print("[news] yfinance rows=0")
    except Exception as e:
        if verbose:
            print(f"[news] yfinance error: {e}")

    if not frames:
        return _empty()

    df = pd.concat(frames, ignore_index=True)
    df["url"] = df["url"].fillna("")
    df = df.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    return df[["ticker", "ts", "title", "url", "text"]]


def fetch_news(
    ticker: str,
    start: str,
    end: str,
    company: Optional[str] = None,
    max_per_provider: int = 6000,
    **kwargs,
) -> pd.DataFrame:
    """
    Backward-compatible export expected by your smoke tests.
    Ignores `company` and `max_per_provider` (kept for API compat).
    Extra kwargs: finnhub_rps, finnhub_on429, yfinance_count, verbose.
    """
    return fetch_news_all_sources(
        ticker, start, end,
        finnhub_rps=int(kwargs.get("finnhub_rps", 5)),
        finnhub_on429=str(kwargs.get("finnhub_on429", "skip")),
        yfinance_count=int(kwargs.get("yfinance_count", 240)),
        verbose=bool(kwargs.get("verbose", False)),
    )
