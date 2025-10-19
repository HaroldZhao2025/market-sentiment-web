# src/market_sentiment/news.py
from __future__ import annotations

import calendar
import time
from typing import List, Tuple, Callable, Optional
from urllib.parse import quote_plus

import pandas as pd
import feedparser
import yfinance as yf

# ------------------------
# Helpers
# ------------------------

def _clean_text(x) -> str:
    try:
        s = str(x)
    except Exception:
        return ""
    s = " ".join(s.split())
    return s

def _first_str(*candidates) -> str:
    """
    Return the first candidate that can be turned into a non-empty string.
    Never rely on Python truthiness of arrays/objects.
    """
    for v in candidates:
        if v is None:
            continue
        if isinstance(v, (list, tuple)):
            for w in v:
                w = _clean_text(w)
                if w:
                    return w
            continue
        w = _clean_text(v)
        if w:
            return w
    return ""

def _norm_ts_utc(x) -> pd.Timestamp:
    """
    Normalize to tz-aware UTC Timestamp.
    Accepts epoch seconds/ms, struct_time, or date-like strings.
    Returns pd.NaT if unparseable.
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

    # epoch seconds / ms
    try:
        xi = int(x)
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

def _retry(fn, tries=2, delay=0.8):
    for i in range(tries):
        try:
            return fn()
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(delay)

# ------------------------
# Providers
# ------------------------

def _prov_yfinance(
    ticker: str, start: str, end: str, company: str | None = None, limit: int = 300
) -> pd.DataFrame:
    """
    yfinance .news (no key).
    Structure is inconsistent across versions – be extremely defensive.
    """
    def _get():
        try:
            t = yf.Ticker(ticker)
            return getattr(t, "news", None)
        except Exception:
            return None

    raw = _retry(_get, tries=2, delay=0.6)

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    if isinstance(raw, list):
        for item in raw[:limit]:
            content = item.get("content") if isinstance(item, dict) else None
            # Many possible fields for publish time
            ts = _norm_ts_utc(
                (item.get("providerPublishTime") if isinstance(item, dict) else None)
                or (item.get("provider_publish_time") if isinstance(item, dict) else None)
                or (item.get("published_at") if isinstance(item, dict) else None)
                or (item.get("pubDate") if isinstance(item, dict) else None)
                or ((content or {}).get("published") if isinstance(content, dict) else None)
                or ((content or {}).get("pubDate") if isinstance(content, dict) else None)
            )
            # If still NaT, assign a recent timestamp to avoid dropping otherwise useful items
            if pd.isna(ts):
                ts = pd.Timestamp.utcnow().tz_localize("UTC")

            title = _first_str(
                (item.get("title") if isinstance(item, dict) else None),
                ((content or {}).get("title") if isinstance(content, dict) else None),
            )
            link = _first_str(
                (item.get("link") if isinstance(item, dict) else None),
                (item.get("url") if isinstance(item, dict) else None),
                ((content or {}).get("link") if isinstance(content, dict) else None),
                ((content or {}).get("url") if isinstance(content, dict) else None),
            )
            text = _first_str(
                (item.get("summary") if isinstance(item, dict) else None),
                ((content or {}).get("summary") if isinstance(content, dict) else None),
                ((content or {}).get("content") if isinstance(content, dict) else None),
                title,
            )
            if not title and not text:
                continue
            rows.append((ts, title or text, link, text or title))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)

def _prov_google_rss(
    ticker: str, start: str, end: str, company: str | None = None, limit: int = 300
) -> pd.DataFrame:
    # we ask for 365d; Google may still return a slice; dedupe later
    q = f'"{ticker}"' + (f' OR "{company}"' if company else "")
    url = f"https://news.google.com/rss/search?q={quote_plus(q)}+when:365d&hl=en-US&gl=US&ceid=US:en"

    def _get():
        return feedparser.parse(url, request_headers={"User-Agent": "Mozilla/5.0"})

    try:
        feed = _retry(_get, tries=2, delay=0.6)
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
        title = _first_str(getattr(entry, "title", None))
        link  = _first_str(getattr(entry, "link", None))
        text  = _first_str(getattr(entry, "summary", None), getattr(entry, "description", None), title)
        if not title and not text:
            continue
        rows.append((ts, title or text, link, text or title))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)

def _prov_yahoo_rss(
    ticker: str, start: str, end: str, company: str | None = None, limit: int = 300
) -> pd.DataFrame:
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={quote_plus(ticker)}&lang=en-US&region=US&count={limit}"

    def _get():
        return feedparser.parse(url, request_headers={"User-Agent": "Mozilla/5.0"})

    try:
        feed = _retry(_get, tries=2, delay=0.6)
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
        title = _first_str(getattr(entry, "title", None))
        link  = _first_str(getattr(entry, "link", None))
        text  = _first_str(getattr(entry, "summary", None), getattr(entry, "description", None), title)
        if not title and not text:
            continue
        rows.append((ts, title or text, link, text or title))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)

def _prov_nasdaq_rss(
    ticker: str, start: str, end: str, company: str | None = None, limit: int = 200
) -> pd.DataFrame:
    url = f"https://www.nasdaq.com/feed/rssoutbound?symbol={quote_plus(ticker)}"

    def _get():
        return feedparser.parse(url, request_headers={"User-Agent": "Mozilla/5.0"})

    try:
        feed = _retry(_get, tries=2, delay=0.8)
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
        title = _first_str(getattr(entry, "title", None))
        link  = _first_str(getattr(entry, "link", None))
        text  = _first_str(getattr(entry, "summary", None), getattr(entry, "description", None), title)
        if not title and not text:
            continue
        rows.append((ts, title or text, link, text or title))

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
    max_per_provider: int = 300,
) -> pd.DataFrame:
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
