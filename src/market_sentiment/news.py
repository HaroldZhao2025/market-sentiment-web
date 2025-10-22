# src/market_sentiment/news.py
from __future__ import annotations

import os
import time
from typing import Callable, List, Optional, Tuple

import pandas as pd

# yfinance (pip: yfinance)
import yfinance as yf

# finnhub (pip: finnhub-python ; import name: finnhub)
try:
    import finnhub
except Exception:
    finnhub = None


# --------------------------------------------------------------------
# Common helpers
# --------------------------------------------------------------------

def _clean_text(x) -> str:
    try:
        s = str(x)
    except Exception:
        return ""
    return " ".join(s.split())


def _mk_df(rows: List[Tuple[pd.Timestamp, str, str, str]], ticker: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    df = pd.DataFrame(rows, columns=["ts", "title", "url", "text"])
    df["ticker"] = ticker
    df = df.dropna(subset=["ts"])
    df["title"] = df["title"].map(_clean_text)
    df["text"] = df["text"].map(_clean_text)
    df["url"] = df["url"].fillna("")
    df = df.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    return df[["ticker", "ts", "title", "url", "text"]]


def _norm_ts_epoch_or_iso(x) -> pd.Timestamp:
    """
    Accepts epoch seconds or ISO8601 strings; returns tz-aware UTC or NaT.
    """
    if x is None:
        return pd.NaT
    # epoch?
    try:
        xi = int(x)
        if xi > 10_000_000_000:
            xi = xi / 1000.0
        return pd.Timestamp.utcfromtimestamp(xi).tz_localize("UTC")
    except Exception:
        pass
    # ISO
    try:
        return pd.to_datetime(x, utc=True, errors="coerce")
    except Exception:
        return pd.NaT


def _window_filter(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    return df[(df["ts"] >= s) & (df["ts"] <= e)].copy()


# --------------------------------------------------------------------
# Providers (the smoke test imports these by name)
#   Signature: (ticker, start, end, company, limit) -> DataFrame
# --------------------------------------------------------------------

def _prov_finnhub(
    ticker: str,
    start: str,
    end: str,
    company: Optional[str] = None,
    limit: int = 240,  # not used for Finnhub (per-day API), kept for signature compatibility
) -> pd.DataFrame:
    """
    EXACT logic:
        import finnhub
        c = finnhub.Client(api_key="...")
        c.company_news('AAPL', _from="YYYY-MM-DD", to="YYYY-MM-DD")

    We iterate day-by-day, with a safe rate limit (default 10 rps; hard cap 30 rps).
    """
    token = (
        os.getenv("FINNHUB_TOKEN")
        or os.getenv("FINNHUB_API_KEY")
        or os.getenv("FINNHUB_KEY")
    )
    if finnhub is None or not token:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    try:
        client = finnhub.Client(api_key=token)
    except Exception:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    # throttle: rps <= 30 (we choose 10 rps by default)
    rps_env = os.getenv("FINNHUB_RPS")
    try:
        rps = max(1, min(30, int(rps_env))) if rps_env else 10
    except Exception:
        rps = 10
    min_gap = 1.0 / float(rps)

    s = pd.Timestamp(start, tz="UTC").normalize()
    e = pd.Timestamp(end, tz="UTC").normalize()
    days = list(pd.date_range(s, e, freq="D", tz="UTC"))

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    last = 0.0

    for d in days:
        day_str = d.date().isoformat()

        # simple rate limiter
        now = time.time()
        wait = max(0.0, (last + min_gap) - now)
        if wait > 0:
            time.sleep(wait)
        last = time.time()

        try:
            arr = client.company_news(ticker, _from=day_str, to=day_str) or []
        except Exception:
            continue

        for it in arr:
            ts = _norm_ts_epoch_or_iso(it.get("datetime"))
            if pd.isna(ts):
                continue
            title = _clean_text(it.get("headline") or "")
            if not title:
                continue
            url = it.get("url") or ""
            text = _clean_text(it.get("summary") or title)
            rows.append((ts, title, url, text))

    return _mk_df(rows, ticker)


def _prov_yfinance(
    ticker: str,
    start: str,
    end: str,
    company: Optional[str] = None,
    limit: int = 240,
) -> pd.DataFrame:
    """
    EXACT logic:
        t = yf.Ticker("MSFT")
        items = t.get_news(count=240, tab="all")   # fallback to `.news` if needed
    Then window-filter to [start, end].
    """
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    count = max(1, min(int(limit or 240), 240))

    try:
        t = yf.Ticker(ticker)
        if hasattr(t, "get_news"):
            items = t.get_news(count=count, tab="all") or []
        else:
            items = getattr(t, "news", None) or []
    except Exception:
        items = []

    for it in items:
        content = it.get("content") if isinstance(it, dict) else None
        ts = _norm_ts_epoch_or_iso(
            (it.get("providerPublishTime") if isinstance(it, dict) else None)
            or (it.get("provider_publish_time") if isinstance(it, dict) else None)
            or (it.get("published_at") if isinstance(it, dict) else None)
            or (content or {}).get("pubDate")
            or (content or {}).get("displayTime")
            or (content or {}).get("published")
        )
        if pd.isna(ts):
            continue

        title = _clean_text(
            (content or {}).get("title")
            or it.get("title")
            or ""
        )
        if not title:
            continue

        link = (
            ((content or {}).get("canonicalUrl") or {}).get("url")
            or ((content or {}).get("clickThroughUrl") or {}).get("url")
            or it.get("link")
            or it.get("url")
            or ""
        )
        text = _clean_text(
            (content or {}).get("summary")
            or (content or {}).get("description")
            or it.get("summary")
            or title
        )
        rows.append((ts, title, link, text))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


# The following provider stubs keep your existing smoke test intact.
# They return empty frames (you can swap in your previous implementations later).

def _prov_google_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 300) -> pd.DataFrame:
    return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

def _prov_yahoo_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 300) -> pd.DataFrame:
    return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

def _prov_nasdaq_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 200) -> pd.DataFrame:
    return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

def _prov_gdelt(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 6000) -> pd.DataFrame:
    return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])


# --------------------------------------------------------------------
# Public API that your build script uses
# --------------------------------------------------------------------

Provider = Callable[[str, str, str, Optional[str], int], pd.DataFrame]

_PROVIDERS: List[Provider] = [
    _prov_finnhub,     # freshest, day-by-day
    _prov_yfinance,    # ~200 recent items
    _prov_google_rss,  # stubs retained for compatibility / smoke test
    _prov_yahoo_rss,
    _prov_nasdaq_rss,
    _prov_gdelt,
]


def fetch_news(
    ticker: str,
    start: str,
    end: str,
    company: str | None = None,
    max_per_provider: int = 240,
) -> pd.DataFrame:
    """
    Merge Finnhub + yfinance (plus stubs), de-dup by (title, url), keep within window.
    """
    frames: List[pd.DataFrame] = []
    for prov in _PROVIDERS:
        try:
            df = prov(ticker, start, end, company, max_per_provider)
        except Exception:
            df = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    out = pd.concat(frames, ignore_index=True)
    out["url"] = out["url"].fillna("")
    out = (
        _window_filter(out, start, end)
        .drop_duplicates(["title", "url"])
        .sort_values("ts")
        .reset_index(drop=True)
    )
    return out
