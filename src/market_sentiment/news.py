# src/market_sentiment/news.py
from __future__ import annotations

import re
import time
import calendar
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Callable, Optional
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
            sec = calendar.timegm(x)  # interpret as UTC
            return pd.Timestamp(sec, unit="s", tz="UTC")
        except Exception:
            pass

    # epoch (int/str) or ms
    try:
        xi = int(x)
        if xi > 10_000_000_000:  # ms -> s
            xi = xi / 1000.0
        return pd.Timestamp(xi, unit="s", tz="UTC")
    except Exception:
        pass

    # generic parse
    ts = pd.to_datetime(x, utc=True, errors="coerce")
    if isinstance(ts, pd.Timestamp):
        return ts
    return pd.NaT

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

def _month_edges(start: str, end: str) -> List[tuple[str, str]]:
    """Return a list of (month_start, month_end) ISO strings covering [start, end]."""
    s = pd.to_datetime(start).date()
    e = pd.to_datetime(end).date()
    out = []
    cur = datetime(s.year, s.month, 1).date()
    while cur <= e:
        nxt_month = (datetime(cur.year, cur.month, 1) + timedelta(days=32)).replace(day=1).date()
        m_start = max(cur, s)
        m_end = min(nxt_month - timedelta(days=1), e)
        out.append((m_start.isoformat(), m_end.isoformat()))
        cur = nxt_month
    return out

# ------------------------
# Providers (free)
# ------------------------

def _prov_yfinance(ticker: str, start: str, end: str, company: str | None = None, limit: int = 80) -> pd.DataFrame:
    """yfinance .news (free, but shallow in history)"""
    try:
        t = yf.Ticker(ticker)
        raw = getattr(t, "news", None)
    except Exception:
        raw = None

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    if isinstance(raw, list):
        for item in raw[:limit]:
            ts = _norm_ts_utc(item.get("providerPublishTime"))
            if pd.isna(ts):
                continue
            title = item.get("title") or ""
            url   = item.get("link") or item.get("url") or ""
            summary = item.get("summary") or ""
            rows.append((ts, title, url, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)

def _google_rss_url(q: str) -> str:
    # Keep it simple; q should be already URL-encoded.
    return f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"

def _prov_google_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 120) -> pd.DataFrame:
    """
    Google News RSS with **year-long coverage** by slicing the period monthly using after:/before: operators.
    Falls back to when:365d if per-month queries return nothing.
    """
    base_q = f'"{ticker}"'
    if company:
        base_q += f' OR "{company}"'

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    total_cap = max(limit, 120) * 12  # generous cap across months

    # 1) Try monthly slices (after:YYYY-MM-DD before:YYYY-MM-DD)
    months = _month_edges(start, end)
    for a, b in months:
        q = quote_plus(f'{base_q} after:{a} before:{b}')
        feed = feedparser.parse(_google_rss_url(q))
        for entry in getattr(feed, "entries", []):
            ts = _norm_ts_utc(getattr(entry, "published_parsed", None) or
                              getattr(entry, "updated_parsed",   None) or
                              getattr(entry, "published",         None) or
                              getattr(entry, "updated",           None))
            if pd.isna(ts):
                continue
            title = _clean_text(getattr(entry, "title", ""))
            link  = getattr(entry, "link", "") or ""
            summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
            rows.append((ts, title, link, summary))
        # Be polite
        time.sleep(0.2)
        if len(rows) >= total_cap:
            break

    # 2) If monthly pulled nothing, try one-shot when:365d
    if not rows:
        q = quote_plus(f"{base_q} when:365d")
        feed = feedparser.parse(_google_rss_url(q))
        for entry in getattr(feed, "entries", []):
            ts = _norm_ts_utc(getattr(entry, "published_parsed", None) or
                              getattr(entry, "updated_parsed",   None) or
                              getattr(entry, "published",         None) or
                              getattr(entry, "updated",           None))
            if pd.isna(ts):
                continue
            title = _clean_text(getattr(entry, "title", ""))
            link  = getattr(entry, "link", "") or ""
            summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
            rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)

def _prov_yahoo_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 200) -> pd.DataFrame:
    """
    Yahoo Finance RSS (free). Limited history, but we bump 'count' to try getting more.
    """
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&lang=en-US&region=US&count={limit}"
    feed = feedparser.parse(url)

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for entry in getattr(feed, "entries", []):
        ts = _norm_ts_utc(getattr(entry, "published_parsed", None) or
                          getattr(entry, "updated_parsed",   None) or
                          getattr(entry, "published",         None) or
                          getattr(entry, "updated",           None))
        if pd.isna(ts):
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link  = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)

def _prov_nasdaq_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 120) -> pd.DataFrame:
    """
    Nasdaq RSS (free). Sometimes flaky; we keep it best-effort.
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
        if pd.isna(ts):
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link  = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)

# Optional extra provider (Business Insider search via Google News handled already)

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

def fetch_news(ticker: str, start: str, end: str, company: str | None = None, max_per_provider: int = 120) -> pd.DataFrame:
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
