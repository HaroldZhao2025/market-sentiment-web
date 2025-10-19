# src/market_sentiment/news.py
from __future__ import annotations

import calendar
import json
import re
from typing import Callable, List, Optional, Tuple
from urllib.parse import quote_plus

import feedparser
import pandas as pd
import yfinance as yf


# ============ Utilities ============

def _clean_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _norm_ts_utc(x) -> pd.Timestamp:
    """
    Normalize to tz-aware UTC Timestamp. Accepts epoch/str/struct_time.
    Returns NaT on failure.
    """
    if x is None:
        return pd.NaT

    # struct_time (e.g., feedparser)
    if hasattr(x, "tm_year"):
        try:
            sec = calendar.timegm(x)
            return pd.Timestamp.utcfromtimestamp(sec).tz_localize("UTC")
        except Exception:
            pass

    # epoch-ish
    try:
        xi = int(x)
        if xi > 10_000_000_000:  # ms -> s
            xi = xi / 1000.0
        return pd.Timestamp.utcfromtimestamp(xi).tz_localize("UTC")
    except Exception:
        pass

    # generic
    ts = pd.to_datetime(x, utc=True, errors="coerce")
    return ts


def _first_ts(*candidates) -> pd.Timestamp:
    """
    Return the first candidate that converts to a non-NaT UTC Timestamp.
    """
    for c in candidates:
        ts = _norm_ts_utc(c)
        if not pd.isna(ts):
            return ts
    return pd.NaT


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


def _window_iter(start_utc: pd.Timestamp, end_utc: pd.Timestamp, days_per_window: int = 30):
    """Yield (ws, we) inclusive UTC windows that cover [start, end]."""
    ws = start_utc
    while ws <= end_utc:
        we = min(ws + pd.Timedelta(days=days_per_window - 1), end_utc)
        yield ws, we
        ws = we + pd.Timedelta(days=1)


# ============ Providers (free) ============

def _prov_yfinance(ticker: str, start: str, end: str, company: str | None = None, limit: int = 200) -> pd.DataFrame:
    """
    yfinance .news (no key)
    Structure varies; try multiple fields, including when the item is nested in 'content'.
    """
    try:
        raw = (yf.Ticker(ticker).news) or []
    except Exception:
        raw = []

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for item in raw[:limit]:
        # Some environments return {id, content=JSON-string} instead of the usual fields
        title = item.get("title") or ""
        url = item.get("link") or item.get("url") or ""
        summary = item.get("summary") or item.get("content", "")

        ts = _first_ts(
            item.get("providerPublishTime"),
            item.get("published_at"),
            item.get("pubDate"),
            item.get("date"),
        )

        # If still NaT, content might be a JSON string with nested date
        if pd.isna(ts):
            content = item.get("content")
            if isinstance(content, dict):
                ts = _first_ts(
                    content.get("providerPublishTime"),
                    content.get("pubDate"),
                    content.get("date"),
                    content.get("published_at"),
                )
                if not title:
                    title = content.get("title") or title
                if not url:
                    url = content.get("link") or content.get("url") or url
                if not summary:
                    summary = content.get("summary") or summary
            elif isinstance(content, str):
                try:
                    c = json.loads(content)
                    ts = _first_ts(
                        c.get("providerPublishTime"),
                        c.get("pubDate"),
                        c.get("date"),
                        c.get("published_at"),
                    )
                    if not title:
                        title = c.get("title") or title
                    if not url:
                        url = c.get("link") or c.get("url") or url
                    if not summary:
                        summary = c.get("summary") or summary
                except Exception:
                    pass

        if pd.isna(ts):
            continue

        rows.append((ts, title, url, summary))

    return _mk_df(rows, ticker)


def _google_rss_once(ticker: str, company: str | None, window_days: int) -> pd.DataFrame:
    """
    One Google News RSS fetch with 'when:Xd' cap (X in days).
    """
    query = f'"{ticker}"'
    if company:
        query += f' OR "{company}"'
    q = quote_plus(query)
    d = max(1, min(int(window_days), 365))
    url = f"https://news.google.com/rss/search?q={q}+when:{d}d&hl=en-US&gl=US&ceid=US:en"
    feed = feedparser.parse(url)

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for entry in getattr(feed, "entries", []):
        ts = _first_ts(
            getattr(entry, "published_parsed", None),
            getattr(entry, "updated_parsed", None),
            getattr(entry, "published", None),
            getattr(entry, "updated", None),
        )
        if pd.isna(ts):
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))

    return _mk_df(rows, ticker)


def _prov_google_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 9999) -> pd.DataFrame:
    """
    Google News RSS across the full [start, end] by windowing, then dedupe.
    """
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end, utc=True)

    parts: List[pd.DataFrame] = []
    for ws, we in _window_iter(s, e, days_per_window=30):
        dfw = _google_rss_once(ticker, company, (we - ws).days + 1)
        if not dfw.empty:
            parts.append(dfw)

    if not parts:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    df = (
        pd.concat(parts, ignore_index=True)
        .drop_duplicates(["title", "url"])
        .sort_values("ts")
        .reset_index(drop=True)
    )
    return df[(df["ts"] >= s) & (df["ts"] <= e)].reset_index(drop=True)


def _yahoo_rss_once(ticker: str, count: int = 400) -> pd.DataFrame:
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={quote_plus(ticker)}&lang=en-US&region=US&count={count}"
    feed = feedparser.parse(url)
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for entry in getattr(feed, "entries", []):
        ts = _first_ts(
            getattr(entry, "published_parsed", None),
            getattr(entry, "updated_parsed", None),
            getattr(entry, "published", None),
            getattr(entry, "updated", None),
        )
        if pd.isna(ts):
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))
    return _mk_df(rows, ticker)


def _prov_yahoo_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 9999) -> pd.DataFrame:
    df = _yahoo_rss_once(ticker, count=400)
    if df.empty:
        return df
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end, utc=True)
    df = df.drop_duplicates(["title", "url"])
    return df[(df["ts"] >= s) & (df["ts"] <= e)].reset_index(drop=True)


def _prov_nasdaq_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 9999) -> pd.DataFrame:
    url = f"https://www.nasdaq.com/feed/rssoutbound?symbol={quote_plus(ticker)}"
    feed = feedparser.parse(url)
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for entry in getattr(feed, "entries", []):
        ts = _first_ts(
            getattr(entry, "published_parsed", None),
            getattr(entry, "updated_parsed", None),
            getattr(entry, "published", None),
            getattr(entry, "updated", None),
        )
        if pd.isna(ts):
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))
    df = _mk_df(rows, ticker)
    if df.empty:
        return df
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end, utc=True)
    return df[(df["ts"] >= s) & (df["ts"] <= e)].reset_index(drop=True)


# ============ Public API ============

Provider = Callable[[str, str, str, Optional[str], int], pd.DataFrame]

_PROVIDERS: List[Provider] = [
    _prov_google_rss,
    _prov_yahoo_rss,
    _prov_nasdaq_rss,
    _prov_yfinance,  # keep as fallback
]


def fetch_news(
    ticker: str,
    start: str,
    end: str,
    company: str | None = None,
    max_per_provider: int = 9999,
) -> pd.DataFrame:
    """
    Combine multiple free providers, normalize & dedupe across the entire period.
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

    out = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(["title", "url"])
        .sort_values("ts")
        .reset_index(drop=True)
    )

    # Final window filter (in case any provider returned extra items)
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end, utc=True)
    out = out[(out["ts"] >= s) & (out["ts"] <= e)].reset_index(drop=True)
    return out
