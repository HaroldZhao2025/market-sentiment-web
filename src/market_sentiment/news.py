# src/market_sentiment/news.py
from __future__ import annotations

import re
import calendar
from typing import List, Tuple, Optional, Any, Callable
from urllib.parse import quote_plus

import pandas as pd
import feedparser
import yfinance as yf  # free


# ------------------------
# Utils
# ------------------------

def _clean_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _norm_ts_utc(x: Any) -> pd.Timestamp:
    """
    Normalize to tz-aware UTC Timestamp.
    Accepts epoch seconds/ms, struct_time, or date-like strings.
    Returns pd.NaT on failure.
    """
    if x is None:
        return pd.NaT

    # feedparser: struct_time
    if hasattr(x, "tm_year"):
        try:
            sec = calendar.timegm(x)  # struct_time -> epoch seconds (UTC)
            return pd.Timestamp.utcfromtimestamp(sec).tz_localize("UTC")
        except Exception:
            pass

    # epoch-like (int/str)
    try:
        xi = int(x)
        if xi > 10_000_000_000:  # treat as ms
            xi = xi / 1000.0
        return pd.Timestamp.utcfromtimestamp(xi).tz_localize("UTC")
    except Exception:
        pass

    # generic parse (may already be tz-aware; keep it)
    ts = pd.to_datetime(x, utc=True, errors="coerce")
    return ts


def _window_filter(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if df.empty:
        return df
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    return df[(df["ts"] >= s) & (df["ts"] <= e)]


def _mk_df(rows: List[Tuple[pd.Timestamp, str, str, str]], ticker: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    df = pd.DataFrame(rows, columns=["ts", "title", "url", "text"])
    df["ticker"] = ticker
    df["title"] = df["title"].map(_clean_text)
    df["text"] = df["text"].map(_clean_text)
    df = df.dropna(subset=["ts"])
    df = df.drop_duplicates(["title", "url"])
    df = df.sort_values("ts").reset_index(drop=True)
    return df[["ticker", "ts", "title", "url", "text"]]


def _deep_find_first(d: Any, keys: List[str]) -> Optional[Any]:
    """
    Find the first value for any of `keys` in nested dict/list structures.
    """
    try:
        from collections import deque
        q = deque([d])
        while q:
            cur = q.popleft()
            if isinstance(cur, dict):
                for k in keys:
                    if k in cur and cur[k] is not None:
                        return cur[k]
                q.extend(cur.values())
            elif isinstance(cur, (list, tuple)):
                q.extend(cur)
    except Exception:
        pass
    return None


# ------------------------
# Providers (free)
# ------------------------

def _prov_yfinance(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 60) -> pd.DataFrame:
    """
    yfinance .news (no key). YF payloads vary; we search multiple fields for time/title/url.
    """
    try:
        t = yf.Ticker(ticker)
        raw = getattr(t, "news", None)
    except Exception:
        raw = None

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    if isinstance(raw, list):
        for item in raw[:limit]:
            # Robust timestamp extraction
            ts_val = _deep_find_first(
                item,
                keys=[
                    "providerPublishTime", "timePublished", "published", "pubDate",
                    "date", "created", "updated", "update_time",
                ],
            )
            ts = _norm_ts_utc(ts_val)

            # Title / URL / Summary
            title = _deep_find_first(item, ["title"]) or ""
            url = _deep_find_first(item, ["link", "url"]) or ""
            summary = _deep_find_first(item, ["summary", "description", "content_text"]) or ""

            # If still no timestamp, fall back to "now" (keeps sample builds alive)
            if pd.isna(ts):
                ts = pd.Timestamp.utcnow().tz_localize("UTC")

            rows.append((ts, str(title), str(url), str(summary)))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_google_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 60) -> pd.DataFrame:
    """
    Google News RSS (no key). Note: must URL-encode the query.
    """
    q = f'"{ticker}"'
    if company:
        q += f' OR "{company}"'
    enc = quote_plus(q)
    url = f"https://news.google.com/rss/search?q={enc}+when:30d&hl=en-US&gl=US&ceid=US:en"
    feed = feedparser.parse(url)

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for i, entry in enumerate(getattr(feed, "entries", [])):
        if i >= limit:
            break
        ts = _norm_ts_utc(
            getattr(entry, "published_parsed", None)
            or getattr(entry, "updated_parsed", None)
            or getattr(entry, "published", None)
            or getattr(entry, "updated", None)
        )
        if pd.isna(ts):
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_yahoo_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 60) -> pd.DataFrame:
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={quote_plus(ticker)}&lang=en-US&region=US&count={limit}"
    feed = feedparser.parse(url)

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for entry in getattr(feed, "entries", []):
        ts = _norm_ts_utc(
            getattr(entry, "published_parsed", None)
            or getattr(entry, "updated_parsed", None)
            or getattr(entry, "published", None)
            or getattr(entry, "updated", None)
        )
        if pd.isna(ts):
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_nasdaq_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 60) -> pd.DataFrame:
    url = f"https://www.nasdaq.com/feed/rssoutbound?symbol={quote_plus(ticker)}"
    feed = feedparser.parse(url)

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for i, entry in enumerate(getattr(feed, "entries", [])):
        if i >= limit:
            break
        ts = _norm_ts_utc(
            getattr(entry, "published_parsed", None)
            or getattr(entry, "updated_parsed", None)
            or getattr(entry, "published", None)
            or getattr(entry, "updated", None)
        )
        if pd.isna(ts):
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_bizinsider_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 60) -> pd.DataFrame:
    """
    Business Insider markets RSS (best-effort).
    """
    # Their ticker feeds are inconsistent; use general markets feed, filter by title.
    url = "https://www.businessinsider.com/sai/rss"
    feed = feedparser.parse(url)

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for i, entry in enumerate(getattr(feed, "entries", [])):
        if i >= limit:
            break
        title_raw = getattr(entry, "title", "") or ""
        if ticker.upper() not in title_raw.upper():
            continue
        ts = _norm_ts_utc(
            getattr(entry, "published_parsed", None)
            or getattr(entry, "updated_parsed", None)
            or getattr(entry, "published", None)
            or getattr(entry, "updated", None)
        )
        if pd.isna(ts):
            continue
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, _clean_text(title_raw), link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


# Register providers (order matters; fastest first)
Provider = Callable[[str, str, str, Optional[str], int], pd.DataFrame]

_PROVIDERS: List[Provider] = [
    _prov_yfinance,
    _prov_google_rss,
    _prov_yahoo_rss,
    _prov_nasdaq_rss,
    _prov_bizinsider_rss,
]


# ------------------------
# Public API
# ------------------------

def fetch_news(ticker: str, start: str, end: str, company: Optional[str] = None, max_per_provider: int = 60) -> pd.DataFrame:
    """
    Combine multiple free providers, normalize & dedupe.
    Output columns: ticker, ts (UTC), title, url, text
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

    df = pd.concat(frames, ignore_index=True)
    df["title"] = df["title"].map(_clean_text)
    df["text"] = df["text"].map(_clean_text)
    df = df.drop_duplicates(["title", "url"])
    df = df.sort_values("ts").reset_index(drop=True)
    return df
