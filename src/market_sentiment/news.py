# src/market_sentiment/news.py
from __future__ import annotations

import calendar
import re
import time
from typing import Callable, List, Optional, Tuple
from urllib.parse import quote_plus

import feedparser
import pandas as pd
import yfinance as yf


# ------------------------
# Helpers
# ------------------------

def _clean_text(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s


def _norm_ts_utc(x) -> pd.Timestamp:
    """
    Normalize to tz-aware UTC pd.Timestamp.
    Accepts epoch seconds/ms, struct_time, ISO strings. pd.NaT on failure.
    """
    if x is None:
        return pd.NaT

    # feedparser struct_time
    if hasattr(x, "tm_year"):
        try:
            sec = calendar.timegm(x)  # struct_time assumed UTC
            return pd.Timestamp.utcfromtimestamp(sec).tz_localize("UTC")
        except Exception:
            return pd.NaT

    # epoch (int-like / str digits)
    try:
        xi = int(x)
        if xi > 10_000_000_000:  # likely ms
            xi = xi / 1000.0
        return pd.Timestamp.utcfromtimestamp(xi).tz_localize("UTC")
    except Exception:
        pass

    # ISO/datelike
    try:
        return pd.to_datetime(x, utc=True, errors="coerce")
    except Exception:
        return pd.NaT


def _window_filter(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    out = df[(df["ts"] >= s) & (df["ts"] <= e)]
    if len(out) == 0:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    return out


def _mk_df(
    rows: List[Tuple[pd.Timestamp, str, str, str]], ticker: str
) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    df = pd.DataFrame(rows, columns=["ts", "title", "url", "text"])
    df["ticker"] = str(ticker)
    # clean + dedupe
    df["title"] = df["title"].map(_clean_text)
    df["text"] = df["text"].map(_clean_text)
    df = df.dropna(subset=["ts"])
    if len(df) == 0:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    df = df.drop_duplicates(subset=["title", "url"], keep="first")
    df = df.sort_values("ts").reset_index(drop=True)
    return df[["ticker", "ts", "title", "url", "text"]]


# ------------------------
# Providers (keyless)
# ------------------------

def _prov_yfinance(
    ticker: str,
    start: str,
    end: str,
    company: Optional[str] = None,
    limit: int = 120,
) -> pd.DataFrame:
    """
    yfinance .news has changed formats over time.
    We read multiple possible fields safely and filter to [start, end].
    """
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    try:
        raw = yf.Ticker(ticker).news or []
    except Exception:
        raw = []

    for item in (raw[:limit] if isinstance(raw, list) else []):
        # known possibilities for timestamp
        ts = (
            _norm_ts_utc(item.get("providerPublishTime"))
            or _norm_ts_utc(item.get("published"))
            or _norm_ts_utc(item.get("pubDate"))
            or pd.NaT
        )
        if ts is pd.NaT:
            continue
        title = item.get("title") or item.get("headline") or ""
        url = item.get("link") or item.get("url") or ""
        text = item.get("summary") or item.get("content") or ""
        rows.append((ts, title, url, text))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_google_rss(
    ticker: str,
    start: str,
    end: str,
    company: Optional[str] = None,
    limit: int = 120,
) -> pd.DataFrame:
    """
    Google News RSS. We URL-encode the query to avoid “control character” errors.
    Note: Google News can't fetch deep historical slices precisely; it's “best effort”.
    """
    q = f'("{ticker}")'
    if company:
        q += f' OR ("{company}")'
    # favor finance-y results & last 365d
    q = f"{q} when:365d"
    url = f"https://news.google.com/rss/search?q={quote_plus(q)}&hl=en-US&gl=US&ceid=US:en"

    try:
        feed = feedparser.parse(url)
    except Exception:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for i, entry in enumerate(getattr(feed, "entries", []) or []):
        if i >= int(limit):
            break
        ts = _norm_ts_utc(
            getattr(entry, "published_parsed", None)
            or getattr(entry, "updated_parsed", None)
            or getattr(entry, "published", None)
            or getattr(entry, "updated", None)
        )
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(
            getattr(entry, "summary", "") or getattr(entry, "description", "")
        )
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_yahoo_rss(
    ticker: str,
    start: str,
    end: str,
    company: Optional[str] = None,
    limit: int = 120,
) -> pd.DataFrame:
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={quote_plus(ticker)}&lang=en-US&region=US&count={int(limit)}"
    try:
        feed = feedparser.parse(url)
    except Exception:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for entry in getattr(feed, "entries", []) or []:
        ts = _norm_ts_utc(
            getattr(entry, "published_parsed", None)
            or getattr(entry, "updated_parsed", None)
            or getattr(entry, "published", None)
            or getattr(entry, "updated", None)
        )
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(
            getattr(entry, "summary", "") or getattr(entry, "description", "")
        )
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_nasdaq_rss(
    ticker: str,
    start: str,
    end: str,
    company: Optional[str] = None,
    limit: int = 120,
) -> pd.DataFrame:
    url = f"https://www.nasdaq.com/feed/rssoutbound?symbol={quote_plus(ticker)}"
    try:
        feed = feedparser.parse(url)
    except Exception:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for i, entry in enumerate(getattr(feed, "entries", []) or []):
        if i >= int(limit):
            break
        ts = _norm_ts_utc(
            getattr(entry, "published_parsed", None)
            or getattr(entry, "updated_parsed", None)
            or getattr(entry, "published", None)
            or getattr(entry, "updated", None)
        )
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(
            getattr(entry, "summary", "") or getattr(entry, "description", "")
        )
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


Provider = Callable[[str, str, str, Optional[str], int], pd.DataFrame]

_PROVIDERS: List[Provider] = [
    _prov_yfinance,
    _prov_google_rss,
    _prov_yahoo_rss,
    _prov_nasdaq_rss,
]


# ------------------------
# Public API
# ------------------------

def fetch_news(
    ticker: str,
    start: str,
    end: str,
    company: Optional[str] = None,
    max_per_provider: int = 120,
) -> pd.DataFrame:
    """
    Aggregate all providers, normalize & dedupe.
    Output columns: ticker, ts (UTC), title, url, text
    """
    frames: List[pd.DataFrame] = []
    for prov in _PROVIDERS:
        try:
            df = prov(ticker, start, end, company, max_per_provider)
            if df is not None and len(df) > 0:
                frames.append(df)
        except Exception:
            # provider may fail intermittently; ignore
            continue

    if not frames:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    df = pd.concat(frames, ignore_index=True)
    # final clean/dedupe/sort
    for col in ("title", "text"):
        df[col] = df[col].map(_clean_text)
    df = df.drop_duplicates(subset=["title", "url"], keep="first")
    df = df.sort_values("ts").reset_index(drop=True)
    return df[["ticker", "ts", "title", "url", "text"]]
