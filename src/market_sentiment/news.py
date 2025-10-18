# src/market_sentiment/news.py
from __future__ import annotations

import calendar
import time
import re
from typing import List, Tuple, Callable, Optional
from urllib.parse import quote_plus

import pandas as pd
import feedparser
import yfinance as yf


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
    Normalize to tz-aware UTC Timestamp. Accepts epoch, struct_time, or date-like strings.
    Returns pd.NaT on failure.
    """
    if x is None:
        return pd.NaT

    # feedparser: struct_time
    if hasattr(x, "tm_year"):
        try:
            sec = calendar.timegm(x)
            return pd.Timestamp.utcfromtimestamp(sec).tz_localize("UTC")
        except Exception:
            return pd.NaT

    # epoch (int/str) seconds or ms
    try:
        xi = int(x)
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
    e = pd.to_datetime(end, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    m = (df["ts"] >= s) & (df["ts"] <= e)
    return df[m]


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


def _weekly_spans(start: str, end: str) -> List[Tuple[str, str]]:
    s = pd.to_datetime(start, utc=True).normalize()
    e = pd.to_datetime(end, utc=True).normalize()
    spans = []
    cur = s
    while cur <= e:
        nxt = min(cur + pd.Timedelta(days=6), e)
        spans.append((cur.strftime("%Y-%m-%d"), nxt.strftime("%Y-%m-%d")))
        cur = nxt + pd.Timedelta(days=1)
    return spans


# ------------------------
# Providers (free)
# ------------------------

def _prov_yfinance(ticker: str, start: str, end: str, company: str | None = None, limit: int = 100) -> pd.DataFrame:
    """yfinance .news (usually ~10 items; keep as a supplement)."""
    try:
        raw = getattr(yf.Ticker(ticker), "news", None)
    except Exception:
        raw = None

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    if isinstance(raw, list):
        for item in raw[:limit]:
            ts = _norm_ts_utc(item.get("providerPublishTime"))
            if pd.isna(ts):
                continue
            title = item.get("title") or ""
            url = item.get("link") or item.get("url") or ""
            summary = item.get("summary") or ""
            rows.append((ts, title, url, summary))
    return _window_filter(_mk_df(rows, ticker), start, end)


def _google_rss_bucket(ticker: str, company: str | None, s: str, e: str, limit: int) -> pd.DataFrame:
    """
    One weekly bucket for Google News RSS using date operators.
    We use: after:YYYY-MM-DD before:YYYY-MM-DD
    """
    quoted = f'"{ticker}"'
    if company:
        quoted += f' OR "{company}"'
    q = f'{quoted} after:{s} before:{e}'
    url = f"https://news.google.com/rss/search?q={quote_plus(q)}&hl=en-US&gl=US&ceid=US:en"
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
    return _window_filter(df, s, e)


def _prov_google_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 100) -> pd.DataFrame:
    """
    Google News RSS over the *entire* [start, end] by bucketing weekly to avoid
    the ~100 item cap per feed call.
    """
    spans = _weekly_spans(start, end)
    frames = []
    for s, e in spans:
        try:
            df = _google_rss_bucket(ticker, company, s, e, limit)
            if not df.empty:
                frames.append(df)
        except Exception:
            continue
    if not frames:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    return out


def _prov_yahoo_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 200) -> pd.DataFrame:
    """
    Yahoo Finance RSS. No date params; just ask for a larger 'count'.
    """
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
    return _window_filter(_mk_df(rows, ticker), start, end)


def _prov_nasdaq_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 120) -> pd.DataFrame:
    """
    Nasdaq RSS (can be flaky; keep errors silent).
    """
    url = f"https://www.nasdaq.com/feed/rssoutbound?symbol={quote_plus(ticker)}"
    try:
        feed = feedparser.parse(url)
    except Exception:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

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
    return _window_filter(_mk_df(rows, ticker), start, end)


# ------------------------
# Public API
# ------------------------

Provider = Callable[[str, str, str, Optional[str], int], pd.DataFrame]

_PROVIDERS: List[Provider] = [
    _prov_google_rss,   # now weekly-bucketed full-range
    _prov_yahoo_rss,
    _prov_nasdaq_rss,
    _prov_yfinance,
]


def fetch_news(
    ticker: str,
    start: str,
    end: str,
    company: str | None = None,
    max_per_provider: int = 100,
) -> pd.DataFrame:
    """
    Combine free providers, normalize, dedupe.
    Output: columns [ticker, ts(UTC), title, url, text]
    """
    frames: List[pd.DataFrame] = []
    for prov in _PROVIDERS:
        try:
            df = prov(ticker, start, end, company, max_per_provider)
            if not df.empty:
                frames.append(df)
        except Exception:
            continue

    if not frames:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    out = pd.concat(frames, ignore_index=True)
    out["title"] = out["title"].map(_clean_text)
    out["text"] = out["text"].map(_clean_text)
    out = out.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    return _window_filter(out, start, end)
