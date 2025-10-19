# src/market_sentiment/news.py
from __future__ import annotations

import calendar
import re
from typing import List, Tuple, Callable, Optional
from urllib.parse import quote_plus
from datetime import datetime, timedelta, timezone

import pandas as pd
import feedparser
import yfinance as yf

# ------------------------
# Small helpers
# ------------------------

def _clean_text(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = str(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _first_str(*candidates) -> str:
    """
    Return the first candidate that can be cleanly turned into a non-empty string.
    Avoids `a or b` on numpy/FeedParser objects (which triggers ambiguous truth errors).
    """
    for v in candidates:
        if v is None:
            continue
        # unpack lists/tuples and try first stringy element
        if isinstance(v, (list, tuple)):
            for w in v:
                w = _clean_text(w)
                if w:
                    return w
            continue
        s = _clean_text(v)
        if s:
            return s
    return ""

def _norm_ts_utc(x) -> pd.Timestamp:
    """
    Normalize to tz-aware UTC Timestamp.
    Accepts epoch seconds/ms, struct_time, or date-like strings.
    Returns pd.NaT on failure.
    """
    if x is None:
        return pd.NaT

    # feedparser struct_time
    if hasattr(x, "tm_year"):
        try:
            sec = calendar.timegm(x)
            return pd.Timestamp.utcfromtimestamp(sec).tz_localize("UTC")
        except Exception:
            return pd.NaT

    # epoch seconds/ms
    try:
        xi = int(x)  # raises if not coercible
        if xi > 10_000_000_000:  # likely ms
            xi = xi / 1000.0
        return pd.Timestamp.utcfromtimestamp(xi).tz_localize("UTC")
    except Exception:
        pass

    # generic parse
    try:
        ts = pd.to_datetime(x, utc=True, errors="coerce")
        return ts
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
    df = df.dropna(subset=["ts"])
    df["title"] = df["title"].map(_clean_text)
    df["text"]  = df["text"].map(_clean_text)
    df = df.drop_duplicates(["title", "url"])
    df = df.sort_values("ts").reset_index(drop=True)
    return df[["ticker", "ts", "title", "url", "text"]]

def _month_windows(start: str, end: str) -> List[tuple[str, str]]:
    """Generate (~monthly) [start,end] windows covering the given period."""
    s = pd.to_datetime(start, utc=True).to_pydatetime().replace(tzinfo=timezone.utc)
    e = pd.to_datetime(end, utc=True).to_pydatetime().replace(tzinfo=timezone.utc)
    if s > e:
        s, e = e, s
    out = []
    cur = s
    while cur <= e:
        nxt = (cur + timedelta(days=32)).replace(day=1) - timedelta(days=1)  # end of month
        if nxt > e:
            nxt = e
        out.append((cur.strftime("%Y-%m-%d"), nxt.strftime("%Y-%m-%d")))
        cur = (nxt + timedelta(days=1)).replace(tzinfo=timezone.utc)
    return out

# ------------------------
# Providers (free / keyless)
# ------------------------

def _prov_yfinance(ticker: str, start: str, end: str, company: str | None = None, limit: int = 300) -> pd.DataFrame:
    """
    yfinance .news (no key). Structure varies; be defensive.
    """
    try:
        t = yf.Ticker(ticker)
        raw = getattr(t, "news", None)
    except Exception:
        raw = None

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []

    if isinstance(raw, list):
        for item in raw[:limit]:
            # timestamp: try several keys; content dicts; fall back to NaT
            ts = _norm_ts_utc(
                item.get("providerPublishTime")
                or item.get("provider_publish_time")
                or item.get("published_at")
                or item.get("pubDate")
                or (item.get("content") or {}).get("published")
            )
            if pd.isna(ts):
                # yfinance sometimes omits ts; ignore those
                continue

            title = _first_str(item.get("title"), (item.get("content") or {}).get("title"))
            link  = _first_str(item.get("link"), item.get("url"), (item.get("content") or {}).get("link"))
            text  = _first_str(item.get("summary"), item.get("content"), (item.get("content") or {}).get("summary"), title)

            rows.append((ts, title, link, text))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)

def _prov_google_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 300) -> pd.DataFrame:
    """
    Google News RSS (no key). We iterate month windows to improve coverage and then dedupe.
    """
    def _one_window(win_start: str, win_end: str) -> pd.DataFrame:
        # encode query; Google sometimes ignores long windows, but when=365d helps
        q_parts = [f'"{ticker}"']
        if company:
            q_parts.append(f'"{company}"')
        q = " OR ".join(q_parts)
        # ask for 365d but we still loop months to coax variety
        url = f"https://news.google.com/rss/search?q={quote_plus(q)}+when:365d&hl=en-US&gl=US&ceid=US:en"
        feed = feedparser.parse(url, request_headers={"User-Agent": "Mozilla/5.0"})
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
            title = _first_str(getattr(entry, "title", None))
            link  = _first_str(getattr(entry, "link", None))
            text  = _first_str(getattr(entry, "summary", None), getattr(entry, "description", None), title)
            rows.append((ts, title, link, text))
        df = _mk_df(rows, ticker)
        return _window_filter(df, win_start, win_end)

    frames = []
    for win_start, win_end in _month_windows(start, end):
        try:
            w = _one_window(win_start, win_end)
        except Exception:
            w = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
        if not w.empty:
            frames.append(w)

    if not frames:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    return df

def _prov_yahoo_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 300) -> pd.DataFrame:
    """
    Yahoo Finance RSS (no key).
    """
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={quote_plus(ticker)}&lang=en-US&region=US&count={limit}"
    feed = feedparser.parse(url, request_headers={"User-Agent": "Mozilla/5.0"})

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
        title = _first_str(getattr(entry, "title", None))
        link  = _first_str(getattr(entry, "link", None))
        text  = _first_str(getattr(entry, "summary", None), getattr(entry, "description", None), title)
        rows.append((ts, title, link, text))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)

def _prov_nasdaq_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 200) -> pd.DataFrame:
    """
    Nasdaq RSS (no key). This endpoint is flaky; errors are swallowed by fetch_news().
    """
    url = f"https://www.nasdaq.com/feed/rssoutbound?symbol={quote_plus(ticker)}"
    feed = feedparser.parse(url, request_headers={"User-Agent": "Mozilla/5.0"})

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
        title = _first_str(getattr(entry, "title", None))
        link  = _first_str(getattr(entry, "link", None))
        text  = _first_str(getattr(entry, "summary", None), getattr(entry, "description", None), title)
        rows.append((ts, title, link, text))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)

# ------------------------
# Public API
# ------------------------

Provider = Callable[[str, str, str, Optional[str], int], pd.DataFrame]

_PROVIDERS: List[Provider] = [
    _prov_yfinance,
    _prov_google_rss,
    _prov_yahoo_rss,
    _prov_nasdaq_rss,
]

def fetch_news(
    ticker: str,
    start: str,
    end: str,
    company: str | None = None,
    max_per_provider: int = 300
) -> pd.DataFrame:
    """
    Combine multiple free providers, normalize & dedupe.
    Output columns: ticker, ts(UTC), title, url, text
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
    df = df.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    return df
