# src/market_sentiment/news.py
from __future__ import annotations

import re
import time
import calendar
from typing import List, Tuple, Callable, Optional

import pandas as pd
import feedparser
import yfinance as yf  # free

# ------------------------
# Utils
# ------------------------

def _clean_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s

def _norm_ts_utc(x) -> pd.Timestamp:
    """
    Normalize to tz-aware UTC Timestamp.
    Accepts epoch, struct_time, or date-like strings.
    Returns pd.NaT on failure.
    """
    if x is None:
        return pd.NaT

    # feedparser: struct_time
    if hasattr(x, "tm_year"):
        try:
            # struct_time assumed in UTC (most feeds); use timegm
            sec = calendar.timegm(x)
            return pd.Timestamp.utcfromtimestamp(sec).tz_localize("UTC")
        except Exception:
            pass

    # epoch (int/str)
    try:
        xi = int(x)  # raises if non-int
        # Heuristic: treat small values as epoch seconds, not ms
        if xi > 10_000_000_000:  # likely ms
            xi = xi / 1000.0
        return pd.Timestamp.utcfromtimestamp(xi).tz_localize("UTC")
    except Exception:
        pass

    # generic parse
    ts = pd.to_datetime(x, utc=True, errors="coerce")
    return ts

def _window_filter(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if df.empty:
        return df
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end,   utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    return df[(df["ts"] >= s) & (df["ts"] <= e)]

def _mk_df(rows: List[Tuple[pd.Timestamp, str, str, str]], ticker: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["ticker","ts","title","url","text"])
    df = pd.DataFrame(rows, columns=["ts","title","url","text"])
    df["ticker"] = ticker
    # final clean
    df["title"] = df["title"].map(_clean_text)
    df["text"]  = df["text"].map(_clean_text)
    df = df.dropna(subset=["ts"])
    df = df.drop_duplicates(["title","url"])
    df = df.sort_values("ts").reset_index(drop=True)
    return df[["ticker","ts","title","url","text"]]

# ------------------------
# Providers (free)
# ------------------------

def _prov_yfinance(ticker: str, start: str, end: str, company: str | None = None, limit: int = 80) -> pd.DataFrame:
    """
    yfinance .news (no key)
    """
    try:
        t = yf.Ticker(ticker)
        raw = getattr(t, "news", None)
    except Exception:
        raw = None

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    if isinstance(raw, list):
        for item in raw[:limit]:
            ts = _norm_ts_utc(item.get("providerPublishTime"))
            if ts is pd.NaT:
                continue
            title = item.get("title") or ""
            url   = item.get("link") or item.get("url") or ""
            summary = item.get("summary") or ""
            rows.append((ts, title, url, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)

def _prov_google_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 80) -> pd.DataFrame:
    """
    Google News RSS (no key)
    """
    q = f'"{ticker}"'
    if company:
        q += f' OR "{company}"'
    url = f"https://news.google.com/rss/search?q={q}+when:30d&hl=en-US&gl=US&ceid=US:en"
    feed = feedparser.parse(url)

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for i, entry in enumerate(getattr(feed, "entries", [])):
        if i >= limit: break
        ts = _norm_ts_utc(getattr(entry, "published_parsed", None) or
                          getattr(entry, "updated_parsed",   None) or
                          getattr(entry, "published",         None) or
                          getattr(entry, "updated",           None))
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link  = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)

def _prov_yahoo_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 80) -> pd.DataFrame:
    """
    Yahoo Finance RSS (no key)
    """
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&lang=en-US&region=US&count={limit}"
    feed = feedparser.parse(url)

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for entry in getattr(feed, "entries", []):
        ts = _norm_ts_utc(getattr(entry, "published_parsed", None) or
                          getattr(entry, "updated_parsed",   None) or
                          getattr(entry, "published",         None) or
                          getattr(entry, "updated",           None))
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link  = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)

def _prov_nasdaq_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 80) -> pd.DataFrame:
    """
    Nasdaq RSS (no key)
    """
    url = f"https://www.nasdaq.com/feed/rssoutbound?symbol={ticker}"
    feed = feedparser.parse(url)

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for i, entry in enumerate(getattr(feed, "entries", [])):
        if i >= limit: break
        ts = _norm_ts_utc(getattr(entry, "published_parsed", None) or
                          getattr(entry, "updated_parsed",   None) or
                          getattr(entry, "published",         None) or
                          getattr(entry, "updated",           None))
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link  = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)

# You can add more free RSS sources here as additional _prov_* functions.

# ------------------------
# Public API (kept stable)
# ------------------------

Provider = Callable[[str, str, str, Optional[str], int], pd.DataFrame]

_PROVIDERS: List[Provider] = [
    _prov_yfinance,
    _prov_google_rss,
    _prov_yahoo_rss,
    _prov_nasdaq_rss,
]

def fetch_news(ticker: str, start: str, end: str, company: str | None = None, max_per_provider: int = 80) -> pd.DataFrame:
    """
    Combine multiple free providers, normalize & dedupe.
    Output columns: ticker, ts (UTC), title, url, text
    """
    frames: List[pd.DataFrame] = []
    for prov in _PROVIDERS:
        try:
            df = prov(ticker, start, end, company, max_per_provider)
        except Exception:
            df = pd.DataFrame(columns=["ticker","ts","title","url","text"])
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["ticker","ts","title","url","text"])

    df = pd.concat(frames, ignore_index=True)
    # Final cleanse/dedupe
    df["title"] = df["title"].map(_clean_text)
    df["text"]  = df["text"].map(_clean_text)
    df = df.drop_duplicates(["title","url"])
    df = df.sort_values("ts").reset_index(drop=True)
    return df
