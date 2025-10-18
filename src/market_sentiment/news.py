# src/market_sentiment/news.py
from __future__ import annotations

import calendar
import re
from typing import List, Tuple, Optional, Callable
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
    return re.sub(r"\s+", " ", str(s)).strip()


def _norm_ts_utc(x) -> pd.Timestamp:
    """
    Robust timestamp normalization -> UTC-aware pandas Timestamp.
    Handles epoch seconds/millis, struct_time, strings.
    On failure returns NaT.
    """
    if x is None:
        return pd.NaT

    # feedparser struct_time
    if hasattr(x, "tm_year"):
        try:
            sec = calendar.timegm(x)
            return pd.Timestamp(sec, unit="s", tz="UTC")
        except Exception:
            return pd.NaT

    # epoch seconds / millis in str or int
    try:
        xi = int(x)
        if xi > 10_000_000_000:  # likely ms
            return pd.Timestamp(xi / 1000.0, unit="s", tz="UTC")
        return pd.Timestamp(xi, unit="s", tz="UTC")
    except Exception:
        pass

    # generic parse
    return pd.to_datetime(x, utc=True, errors="coerce")


def _window(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if df.empty:
        return df
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    return df[(df["ts"] >= s) & (df["ts"] <= e)]


def _mk_df(rows: List[Tuple[pd.Timestamp, str, str, str]], ticker: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    df = pd.DataFrame(rows, columns=["ts", "title", "url", "text"]).dropna(subset=["ts"])
    df["ticker"] = ticker
    df["title"] = df["title"].map(_clean_text)
    df["text"] = df["text"].map(_clean_text)
    df = df.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    return df[["ticker", "ts", "title", "url", "text"]]


# ------------------------
# Providers (free)
# ------------------------

def _prov_yfinance(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 80) -> pd.DataFrame:
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    try:
        raw = getattr(yf.Ticker(ticker), "news", None)
        if isinstance(raw, list):
            for item in raw[:limit]:
                ts = _norm_ts_utc(
                    item.get("providerPublishTime")
                    or item.get("published_at")
                    or item.get("providerPublishDate")
                )
                if pd.isna(ts):
                    continue
                title = item.get("title") or ""
                url = item.get("link") or item.get("url") or ""
                summary = item.get("summary") or ""
                rows.append((ts, title, url, summary))
    except Exception:
        pass
    return _window(_mk_df(rows, ticker), start, end)


def _prov_google_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 80) -> pd.DataFrame:
    q = f'"{ticker}"' + (f' OR "{company}"' if company else "")
    url = f"https://news.google.com/rss/search?q={quote_plus(q)}+when:30d&hl=en-US&gl=US&ceid=US:en"
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    try:
        feed = feedparser.parse(url)
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
    except Exception:
        pass
    return _window(_mk_df(rows, ticker), start, end)


def _prov_yahoo_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 80) -> pd.DataFrame:
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={quote_plus(ticker)}&lang=en-US&region=US&count={limit}"
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    try:
        feed = feedparser.parse(url)
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
    except Exception:
        pass
    return _window(_mk_df(rows, ticker), start, end)


def _prov_nasdaq_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 80) -> pd.DataFrame:
    url = f"https://www.nasdaq.com/feed/rssoutbound?symbol={quote_plus(ticker)}"
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    try:
        feed = feedparser.parse(url)
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
    except Exception:
        pass
    return _window(_mk_df(rows, ticker), start, end)


def _prov_bi_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 60) -> pd.DataFrame:
    # Business Insider (best effort)
    url = f"https://www.businessinsider.com/s?q={quote_plus(ticker)}&op=1&type=article&sort=time&max={limit}&format=rss"
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    try:
        feed = feedparser.parse(url)
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
    except Exception:
        pass
    return _window(_mk_df(rows, ticker), start, end)


def _prov_sa_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 60) -> pd.DataFrame:
    # Seeking Alpha (best effort)
    url = f"https://seekingalpha.com/api/sa/combined/{quote_plus(ticker)}.xml"
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    try:
        feed = feedparser.parse(url)
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
    except Exception:
        pass
    return _window(_mk_df(rows, ticker), start, end)


# Back-compat alias so your smoke test continues to work
_prov_bizinsider_rss = _prov_bi_rss  # alias name expected by your test


Provider = Callable[[str, str, str, Optional[str], int], pd.DataFrame]

_PROVIDERS: List[Provider] = [
    _prov_yfinance,
    _prov_google_rss,
    _prov_yahoo_rss,
    _prov_nasdaq_rss,
    _prov_bi_rss,
    _prov_sa_rss,
]


def fetch_news(ticker: str, start: str, end: str, company: Optional[str] = None, max_per_provider: int = 80) -> pd.DataFrame:
    """
    Combine multiple free providers -> clean, deduped news:
    columns = [ticker, ts(UTC), title, url, text]
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

    df = pd.concat(frames, ignore_index=True)
    df["title"] = df["title"].map(_clean_text)
    df["text"] = df["text"].map(_clean_text)
    df = df.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    return df


__all__ = [
    "fetch_news",
    "_prov_yfinance",
    "_prov_google_rss",
    "_prov_yahoo_rss",
    "_prov_nasdaq_rss",
    "_prov_bi_rss",
    "_prov_bizinsider_rss",  # explicitly exported alias
    "_prov_sa_rss",
]
