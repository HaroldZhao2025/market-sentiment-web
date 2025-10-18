# src/market_sentiment/news.py
from __future__ import annotations

import calendar
import re
import time
from typing import Callable, List, Optional, Tuple
from urllib.parse import quote_plus

import feedparser
import pandas as pd
import yfinance as yf  # free

# ------------------------
# Utils
# ------------------------

def _clean_text(s: Optional[str]) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _norm_ts_utc(x) -> pd.Timestamp:
    """
    Normalize anything-ish into a tz-aware UTC pandas Timestamp.
    Accepts: struct_time, epoch seconds/ms, strings, pd.Timestamp.
    Returns pd.NaT on failure.
    """
    if x is None:
        return pd.NaT

    # Already a Timestamp?
    if isinstance(x, pd.Timestamp):
        try:
            return x.tz_convert("UTC") if x.tzinfo else x.tz_localize("UTC")
        except Exception:
            try:
                return pd.to_datetime(x, utc=True, errors="coerce")
            except Exception:
                return pd.NaT

    # feedparser: struct_time
    if hasattr(x, "tm_year"):
        try:
            sec = calendar.timegm(x)
            return pd.Timestamp.utcfromtimestamp(sec).tz_localize("UTC")
        except Exception:
            pass

    # epoch int/str
    try:
        xi = int(x)
        # Heuristic for ms vs s
        if xi > 10_000_000_000:
            xi = xi / 1000.0
        return pd.Timestamp.utcfromtimestamp(xi).tz_localize("UTC")
    except Exception:
        pass

    # generic parse
    try:
        return pd.to_datetime(x, utc=True, errors="coerce")
    except Exception:
        return pd.NaT


def _window_filter(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if df.empty:
        return df
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end,   utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    return df[(df["ts"] >= s) & (df["ts"] <= e)]


def _mk_df(rows: List[Tuple[pd.Timestamp, str, str, str]], ticker: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    df = pd.DataFrame(rows, columns=["ts", "title", "url", "text"])
    df["ticker"] = ticker
    df["title"] = df["title"].map(_clean_text)
    df["text"]  = df["text"].map(_clean_text)
    df = df.dropna(subset=["ts"])
    df = df.drop_duplicates(["title", "url"])
    df = df.sort_values("ts").reset_index(drop=True)
    return df[["ticker", "ts", "title", "url", "text"]]


def _date_slices(start: str, end: str, chunk_days: int = 28) -> List[Tuple[str, str]]:
    """
    Split [start, end] into ~monthly slices to avoid provider-side 30d truncation.
    """
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end,   utc=True)
    out: List[Tuple[str, str]] = []
    cur = s
    while cur <= e:
        nxt = min(cur + pd.Timedelta(days=chunk_days - 1), e)
        out.append((cur.strftime("%Y-%m-%d"), nxt.strftime("%Y-%m-%d")))
        cur = nxt + pd.Timedelta(days=1)
    return out


# ------------------------
# Providers (free)
# ------------------------

def _prov_yfinance(ticker: str, start: str, end: str, company: str | None = None, limit: int = 200) -> pd.DataFrame:
    try:
        t = yf.Ticker(ticker)
        raw = getattr(t, "news", None)
    except Exception:
        raw = None

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    if isinstance(raw, list):
        for item in raw[: max(0, limit)]:
            ts = _norm_ts_utc(item.get("providerPublishTime"))
            if pd.isna(ts):
                # Some entries hide timestamp under "providerPublishTime" as str
                ts = _norm_ts_utc(item.get("providerPublishTime") or item.get("pubDate"))
            if pd.isna(ts):
                continue
            title = item.get("title") or ""
            url   = item.get("link") or item.get("url") or ""
            summary = item.get("summary") or item.get("content") or ""
            rows.append((ts, title, url, _clean_text(summary)))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_google_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 200) -> pd.DataFrame:
    """
    Google News RSS. We chunk the year into 28-day windows and query each window.
    """
    name = company or ticker
    # Build a tight query: by ticker OR company, quoted
    q = f'"{ticker}" OR "{name}"'

    rows: List[pd.DataFrame] = []
    for s, e in _date_slices(start, end, 28):
        # Use 'when:30d' to bias freshness, then post-filter by our [s,e]
        q_slice = quote_plus(f"{q} when:30d")
        url = f"https://news.google.com/rss/search?q={q_slice}&hl=en-US&gl=US&ceid=US:en"
        try:
            feed = feedparser.parse(url)
        except Exception:
            continue

        chunk: List[Tuple[pd.Timestamp, str, str, str]] = []
        for i, entry in enumerate(getattr(feed, "entries", [])[: max(0, limit)]):
            ts = _norm_ts_utc(
                getattr(entry, "published_parsed", None)
                or getattr(entry, "updated_parsed", None)
                or getattr(entry, "published", None)
                or getattr(entry, "updated", None)
            )
            if pd.isna(ts):
                continue
            title = _clean_text(getattr(entry, "title", ""))
            link  = getattr(entry, "link", "") or ""
            summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
            chunk.append((ts, title, link, summary))

        if chunk:
            df = _mk_df(chunk, ticker)
            df = _window_filter(df, s, e)
            rows.append(df)

    if not rows:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    df = pd.concat(rows, ignore_index=True)
    return _window_filter(df, start, end)


def _prov_yahoo_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 400) -> pd.DataFrame:
    """
    Yahoo Finance RSS — allows a 'count' param. We still post-filter.
    """
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={quote_plus(ticker)}&lang=en-US&region=US&count={int(limit)}"
    try:
        feed = feedparser.parse(url)
    except Exception:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

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


def _prov_nasdaq_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 200) -> pd.DataFrame:
    """
    Nasdaq outbound RSS; sometimes flaky — we guard exceptions.
    """
    url = f"https://www.nasdaq.com/feed/rssoutbound?symbol={quote_plus(ticker)}"
    try:
        feed = feedparser.parse(url)
    except Exception:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for i, entry in enumerate(getattr(feed, "entries", [])[: max(0, limit)]):
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


def _prov_apple_newsroom(ticker: str, start: str, end: str, company: str | None = None, limit: int = 200) -> pd.DataFrame:
    """
    Apple official newsroom RSS. Not huge volume but high-signal.
    """
    url = "https://www.apple.com/newsroom/rss-feed.rss"
    try:
        feed = feedparser.parse(url)
    except Exception:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for i, entry in enumerate(getattr(feed, "entries", [])[: max(0, limit)]):
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
    # All these items are about Apple; still filter by date.
    return _window_filter(df, start, end)


def _prov_bizinsider_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 120) -> pd.DataFrame:
    """
    Business Insider does not expose a stable per-ticker RSS. We keep a stub that returns empty
    so imports in smoke tests never fail.
    """
    return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])


# Public API type
Provider = Callable[[str, str, str, Optional[str], int], pd.DataFrame]

# Provider order (diverse + resilient). You can re-order if you prefer.
_PROVIDERS: List[Provider] = [
    _prov_yahoo_rss,
    _prov_google_rss,
    _prov_yfinance,
    _prov_nasdaq_rss,
    _prov_apple_newsroom,
]


# ------------------------
# Public API
# ------------------------

def fetch_news(
    ticker: str,
    start: str,
    end: str,
    company: str | None = None,
    max_per_provider: int = 200,
) -> pd.DataFrame:
    """
    Combine multiple free providers, normalize & dedupe across slices.
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
    # final cleanse/dedupe
    df["title"] = df["title"].map(_clean_text)
    df["text"]  = df["text"].map(_clean_text)
    df["ts"]    = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])
    df = df.drop_duplicates(["title", "url"])
    df = df.sort_values("ts").reset_index(drop=True)
    return _window_filter(df, start, end)
