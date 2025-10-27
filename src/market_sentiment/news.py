from __future__ import annotations
from typing import List, Optional
import pandas as pd

from .news_finnhub_daily import fetch_finnhub_daily
from .news_yfinance import fetch_yfinance_recent


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])


# ------- Provider wrappers with the 5-arg signature your smoke tests use -------

def _prov_finnhub(
    ticker: str,
    start: str,
    end: str,
    company: Optional[str] = None,  # unused (kept for signature compatibility)
    limit: int = 0,                  # unused
    **kwargs,
) -> pd.DataFrame:
    # kwargs pass-through: finnhub_rps, max_wait_sec, cache_dir, verbose
    return fetch_finnhub_daily(
        ticker=ticker,
        start=start,
        end=end,
        rps=int(kwargs.get("finnhub_rps", 1)),
        max_wait_sec=int(kwargs.get("finnhub_max_wait_sec", 600)),
        cache_dir=kwargs.get("cache_dir", "data/news_cache"),
        verbose=bool(kwargs.get("verbose", False)),
    )


def _prov_yfinance(
    ticker: str,
    start: str,
    end: str,
    company: Optional[str] = None,   # unused
    limit: int = 240,
    **kwargs,
) -> pd.DataFrame:
    return fetch_yfinance_recent(
        ticker=ticker,
        start=start,
        end=end,
        count=int(kwargs.get("yfinance_count", limit or 240)),
        tab="all",
    )


# Legacy names kept as no-ops so old imports don’t break
def _prov_gdelt(*_args, **_kwargs) -> pd.DataFrame: return _empty()
def _prov_google_rss(*_args, **_kwargs) -> pd.DataFrame: return _empty()
def _prov_yahoo_rss(*_args, **_kwargs) -> pd.DataFrame: return _empty()
def _prov_nasdaq_rss(*_args, **_kwargs) -> pd.DataFrame: return _empty()


# ---------------- Public API ----------------

def fetch_news_all_sources(
    ticker: str,
    start: str,
    end: str,
    *,
    finnhub_rps: int = 1,
    finnhub_max_wait_sec: int = 600,
    yfinance_count: int = 240,
    cache_dir: str = "data/news_cache",
    verbose: bool = False,
) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []

    # Finnhub (historical, daily)
    try:
        df_fh = _prov_finnhub(
            ticker, start, end,
            finnhub_rps=finnhub_rps,
            finnhub_max_wait_sec=finnhub_max_wait_sec,
            cache_dir=cache_dir,
            verbose=verbose,
        )
        if df_fh is not None and not df_fh.empty:
            frames.append(df_fh)
            if verbose:
                print(f"[merge] finnhub rows={len(df_fh)} days={df_fh['ts'].dt.date.nunique()}")
    except Exception as e:
        if verbose:
            print(f"[merge] finnhub error: {e}")

    # yfinance (recent ~200)
    try:
        df_yf = _prov_yfinance(
            ticker, start, end,
            yfinance_count=yfinance_count,
        )
        if df_yf is not None and not df_yf.empty:
            frames.append(df_yf)
            if verbose:
                print(f"[merge] yfinance rows={len(df_yf)} days={df_yf['ts'].dt.date.nunique()}")
    except Exception as e:
        if verbose:
            print(f"[merge] yfinance error: {e}")

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
    company: Optional[str] = None,   # kept for compatibility
    max_per_provider: int = 0,       # ignored (Finnhub is per-day; yfinance uses count)
    **kwargs,
) -> pd.DataFrame:
    return fetch_news_all_sources(
        ticker=ticker,
        start=start,
        end=end,
        finnhub_rps=int(kwargs.get("finnhub_rps", 1)),
        finnhub_max_wait_sec=int(kwargs.get("finnhub_max_wait_sec", 600)),
        yfinance_count=int(kwargs.get("yfinance_count", 240)),
        cache_dir=kwargs.get("cache_dir", "data/news_cache"),
        verbose=bool(kwargs.get("verbose", False)),
    )
