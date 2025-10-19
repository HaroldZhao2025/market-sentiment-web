# src/market_sentiment/news.py
from __future__ import annotations

import re
import calendar
from typing import List, Tuple, Optional
from urllib.parse import quote_plus

import pandas as pd
import feedparser
import yfinance as yf
from dateutil import parser as dateparser
from datetime import timezone

# ---------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------

def _clean_text(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s

def _to_ts(x) -> pd.Timestamp:
    """
    Convert many possible inputs to a single UTC pd.Timestamp.
    Returns pd.NaT on failure.
    """
    if x is None:
        return pd.NaT

    # If a list/tuple/array, take the first valid scalar
    if isinstance(x, (list, tuple)):
        for y in x:
            ts = _to_ts(y)
            if not pd.isna(ts):
                return ts
        return pd.NaT

    # feedparser struct_time
    if hasattr(x, "tm_year"):
        try:
            sec = calendar.timegm(x)
            return pd.Timestamp(sec, unit="s", tz="UTC")
        except Exception:
            return pd.NaT

    # epoch numbers / strings that look numeric
    try:
        xs = str(x).strip()
        if xs.isdigit():
            val = int(xs)
            if val > 10_000_000_000:  # milliseconds -> seconds
                val = val / 1000.0
            return pd.Timestamp(val, unit="s", tz="UTC")
    except Exception:
        pass

    # generic date string
    try:
        dt = dateparser.parse(str(x))
        if dt is None:
            return pd.NaT
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return pd.Timestamp(dt).tz_convert("UTC")
    except Exception:
        return pd.NaT


def _mk_df(rows: List[Tuple[pd.Timestamp, str, str, str]], ticker: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    df = pd.DataFrame(rows, columns=["ts", "title", "url", "text"])
    df["ticker"] = ticker
    df = df.dropna(subset=["ts"])
    df["title"] = df["title"].map(_clean_text)
    df["text"] = df["text"].map(_clean_text)
    df = df.drop_duplicates(["title", "url"])
    df = df.sort_values("ts").reset_index(drop=True)
    return df[["ticker", "ts", "title", "url", "text"]]


def _window_filter(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if df.empty:
        return df
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    out = df[(df["ts"] >= s) & (df["ts"] <= e)]
    return out


def _month_windows(start: str, end: str):
    s = pd.Timestamp(start, tz="UTC").normalize()
    e = pd.Timestamp(end, tz="UTC").normalize()
    cur = s
    while cur <= e:
        w_end = min((cur + pd.offsets.MonthEnd(0)), e)  # up to end of current month
        yield cur, w_end
        cur = (w_end + pd.Timedelta(days=1)).normalize()


# ---------------------------------------------------------------------
# providers
# ---------------------------------------------------------------------

def _prov_yfinance(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 120) -> pd.DataFrame:
    """yfinance .news (10 recent only in practice)."""
    try:
        raw = getattr(yf.Ticker(ticker), "news", None) or []
    except Exception:
        raw = []

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for item in list(raw)[:limit]:
        ts = _to_ts(item.get("providerPublishTime"))
        if pd.isna(ts):
            continue
        title = item.get("title") or ""
        url = item.get("link") or item.get("url") or ""
        summary = item.get("summary") or ""
        rows.append((ts, title, url, summary))

    return _window_filter(_mk_df(rows, ticker), start, end)


def _fetch_rss(url: str, limit: int) -> List[dict]:
    feed = feedparser.parse(url)
    entries = getattr(feed, "entries", []) or []
    return list(entries)[:limit]


def _prov_google_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 120) -> pd.DataFrame:
    """
    Google News RSS. We split by calendar month to improve coverage,
    URL-encode the query, and parse timestamps robustly.
    """
    query_terms = [ticker]
    if company:
        query_terms.append(company)
    quoted = " OR ".join(f'"{q}"' for q in query_terms)

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for ws, we in _month_windows(start, end):
        qp = quote_plus(f"{quoted} after:{ws.date()} before:{(we + pd.Timedelta(days=1)).date()}")
        url = f"https://news.google.com/rss/search?q={qp}&hl=en-US&gl=US&ceid=US:en"
        try:
            for entry in _fetch_rss(url, limit):
                ts = _to_ts(
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
        except Exception:
            # swallow provider errors — other providers will still contribute
            continue

    return _window_filter(_mk_df(rows, ticker), start, end)


def _prov_yahoo_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 200) -> pd.DataFrame:
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={quote_plus(ticker)}&lang=en-US&region=US&count={limit}"
    try:
        entries = _fetch_rss(url, limit)
    except Exception:
        entries = []

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for entry in entries:
        ts = _to_ts(
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


def _prov_seekingalpha_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 200) -> pd.DataFrame:
    """SeekingAlpha combined feed (often generous)."""
    url = f"https://seekingalpha.com/api/sa/combined/{quote_plus(ticker)}.xml"
    try:
        entries = _fetch_rss(url, limit)
    except Exception:
        entries = []

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for entry in entries:
        ts = _to_ts(
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
        # only keep those that mention ticker/company in title to reduce noise
        if ticker.lower() in title.lower() or (company and company.lower() in title.lower()):
            rows.append((ts, title, link, summary))

    return _window_filter(_mk_df(rows, ticker), start, end)


# public API
Provider = callable

_PROVIDERS: List[Provider] = [
    _prov_google_rss,
    _prov_yahoo_rss,
    _prov_seekingalpha_rss,
    _prov_yfinance,  # small but free
]

def fetch_news(ticker: str, start: str, end: str, company: Optional[str] = None, max_per_provider: int = 200) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for prov in _PROVIDERS:
        try:
            df = prov(ticker, start, end, company, max_per_provider)
            if not df.empty:
                frames.append(df)
        except Exception:
            # one provider failing shouldn't kill the whole pipeline
            continue

    if not frames:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    out = pd.concat(frames, ignore_index=True)
    out["title"] = out["title"].map(_clean_text)
    out["text"] = out["text"].map(_clean_text)
    out = out.drop_duplicates(["title", "url"])
    out = _window_filter(out, start, end)
    out = out.sort_values("ts").reset_index(drop=True)
    return out
