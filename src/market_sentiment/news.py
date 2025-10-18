# src/market_sentiment/news.py
from __future__ import annotations

import os
import re
import calendar
from typing import List, Tuple, Optional, Callable

import pandas as pd
import requests
import feedparser
import yfinance as yf  # free

# --- Tunables (via env) ---
_DEFAULT_CONNECT = float(os.getenv("NEWS_CONNECT_TIMEOUT", "3.5"))  # seconds
_DEFAULT_READ    = float(os.getenv("NEWS_READ_TIMEOUT", "6.0"))     # seconds
_DEFAULT_LIMIT   = int(os.getenv("NEWS_ITEMS_PER_SOURCE", "60"))    # per source per ticker
_UA = os.getenv(
    "NEWS_USER_AGENT",
    "Mozilla/5.0 (compatible; market-sentiment-web/1.0; +https://github.com/HaroldZhao2025/market-sentiment-web)"
)
_HEADERS = {"User-Agent": _UA, "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8"}

def _get(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, headers=_HEADERS, timeout=(_DEFAULT_CONNECT, _DEFAULT_READ))
        if r.ok and r.content:
            return r.content
    except Exception:
        pass
    return None

# ------------------------
# Utils
# ------------------------

def _clean_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s

def _norm_ts_utc(x) -> pd.Timestamp:
    """Normalize a date-like to tz-aware UTC Timestamp; returns NaT on failure."""
    if x is None:
        return pd.NaT

    # feedparser struct_time
    if hasattr(x, "tm_year"):
        try:
            sec = calendar.timegm(x)
            return pd.Timestamp.utcfromtimestamp(sec).tz_localize("UTC")
        except Exception:
            pass

    # epoch int/str (sec or ms)
    try:
        xi = int(x)
        if xi > 10_000_000_000:  # likely ms
            xi /= 1000.0
        return pd.Timestamp.utcfromtimestamp(xi).tz_localize("UTC")
    except Exception:
        pass

    # generic parse
    return pd.to_datetime(x, utc=True, errors="coerce")

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
    df = df.dropna(subset=["ts"]).drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    return df[["ticker", "ts", "title", "url", "text"]]

# ------------------------
# Providers (free)
# ------------------------

def _prov_yfinance(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = _DEFAULT_LIMIT) -> pd.DataFrame:
    try:
        raw = getattr(yf.Ticker(ticker), "news", None)
    except Exception:
        raw = None
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    if isinstance(raw, list):
        for item in raw[:limit]:
            ts = _norm_ts_utc(item.get("providerPublishTime"))
            if ts is pd.NaT:
                continue
            title = item.get("title") or ""
            url = item.get("link") or item.get("url") or ""
            summary = item.get("summary") or ""
            rows.append((ts, title, url, summary))
    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)

def _parse_feed(url: str) -> feedparser.FeedParserDict:
    # Fetch with requests (timeouts), then parse bytes with feedparser
    blob = _get(url)
    return feedparser.parse(blob) if blob else feedparser.parse(b"")

def _prov_google_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = _DEFAULT_LIMIT) -> pd.DataFrame:
    q = f'"{ticker}"'
    if company:
        q += f' OR "{company}"'
    url = f"https://news.google.com/rss/search?q={q}+when:30d&hl=en-US&gl=US&ceid=US:en"
    feed = _parse_feed(url)
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for i, entry in enumerate(getattr(feed, "entries", [])):
        if i >= limit: break
        ts = _norm_ts_utc(getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
                          or getattr(entry, "published", None) or getattr(entry, "updated", None))
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))
    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)

def _prov_yahoo_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = _DEFAULT_LIMIT) -> pd.DataFrame:
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&lang=en-US&region=US&count={limit}"
    feed = _parse_feed(url)
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for entry in getattr(feed, "entries", []):
        ts = _norm_ts_utc(getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
                          or getattr(entry, "published", None) or getattr(entry, "updated", None))
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))
    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)

def _prov_nasdaq_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = _DEFAULT_LIMIT) -> pd.DataFrame:
    url = f"https://www.nasdaq.com/feed/rssoutbound?symbol={ticker}"
    feed = _parse_feed(url)
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for i, entry in enumerate(getattr(feed, "entries", [])):
        if i >= limit: break
        ts = _norm_ts_utc(getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
                          or getattr(entry, "published", None) or getattr(entry, "updated", None))
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
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

# Public API
def fetch_news(ticker: str, start: str, end: str, company: Optional[str] = None, max_per_provider: int = _DEFAULT_LIMIT) -> pd.DataFrame:
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
    out = pd.concat(frames, ignore_index=True)
    out["title"] = out["title"].map(_clean_text)
    out["text"]  = out["text"].map(_clean_text)
    out = out.drop_duplicates(["title","url"]).sort_values("ts").reset_index(drop=True)
    return out
