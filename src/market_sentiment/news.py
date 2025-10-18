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
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _first_present(*vals: Any) -> Any:
    """
    Return the first value that is not None. Do not evaluate truthiness
    (avoids numpy 'truth value is ambiguous' errors).
    """
    for v in vals:
        if v is not None:
            return v
    return None


def _as_str(x: Any) -> str:
    return "" if x is None else str(x)


def _norm_ts_utc(x: Any) -> pd.Timestamp:
    """
    Normalize to tz-aware UTC Timestamp.
    Accepts epoch seconds/ms, struct_time, or parseable strings.
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

    # generic parse (force UTC)
    return pd.to_datetime(x, utc=True, errors="coerce")


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
    yfinance .news (no key). Payloads vary; search multiple fields for time/title/url.
    """
    try:
        t = yf.Ticker(ticker)
        raw = getattr(t, "news", None)
    except Exception:
        raw = None

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    if isinstance(raw, list):
        for item in raw[:limit]:
            ts_val = _deep_find_first(
                item,
                keys=[
                    "providerPublishTime", "timePublished", "published", "pubDate",
                    "date", "created", "updated", "update_time",
                ],
            )
            ts = _norm_ts_utc(ts_val)
            if pd.isna(ts):
                # keep sample builds alive; don't drop the item silently
                ts = pd.Timestamp.utcnow().tz_localize("UTC")

            title = _as_str(_deep_find_first(item, ["title"]))
            url   = _as_str(_deep_find_first(item, ["link", "url"]))
            summ  = _as_str(_deep_find_first(item, ["summary", "description", "content_text"]))
            rows.append((ts, title, url, summ))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_google_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 60) -> pd.DataFrame:
    """
    Google News RSS (no key). Must URL-encode query; avoid `or` chains on possibly array-like fields.
    """
    q = f'"{ticker}"'
    if company:
        q += f' OR "{company}"'
    enc = quote_plus(q)
    url = f"https://news.google.com/rss/search?q={enc}+when:30d&hl=en-US&gl=US&ceid=US:en"
    feed = feedparser.parse(url)

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    entries = list(getattr(feed, "entries", []) or [])
    for i, entry in enumerate(entries):
        if i >= limit:
            break
        ts_src = _first_present(
            getattr(entry, "published_parsed", None),
            getattr(entry, "updated_parsed", None),
            getattr(entry, "published", None),
            getattr(entry, "updated", None),
        )
        ts = _norm_ts_utc(ts_src)
        if pd.isna(ts):
            continue

        title = _clean_text(_as_str(getattr(entry, "title", "")))
        link  = _as_str(getattr(entry, "link", "")) or ""
        summ  = _clean_text(_as_str(getattr(entry, "summary", "")) or _as_str(getattr(entry, "description", "")))
        rows.append((ts, title, link, summ))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_yahoo_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 60) -> pd.DataFrame:
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={quote_plus(ticker)}&lang=en-US&region=US&count={limit}"
    feed = feedparser.parse(url)

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    entries = list(getattr(feed, "entries", []) or [])
    for entry in entries:
        ts_src = _first_present(
            getattr(entry, "published_parsed", None),
            getattr(entry, "updated_parsed", None),
            getattr(entry, "published", None),
            getattr(entry, "updated", None),
        )
        ts = _norm_ts_utc(ts_src)
        if pd.isna(ts):
            continue

        title = _clean_text(_as_str(getattr(entry, "title", "")))
        link  = _as_str(getattr(entry, "link", "")) or ""
        summ  = _clean_text(_as_str(getattr(entry, "summary", "")) or _as_str(getattr(entry, "description", "")))
        rows.append((ts, title, link, summ))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_nasdaq_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 60) -> pd.DataFrame:
    url = f"https://www.nasdaq.com/feed/rssoutbound?symbol={quote_plus(ticker)}"
    feed = feedparser.parse(url)

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    entries = list(getattr(feed, "entries", []) or [])
    for i, entry in enumerate(entries):
        if i >= limit:
            break
        ts_src = _first_present(
            getattr(entry, "published_parsed", None),
            getattr(entry, "updated_parsed", None),
            getattr(entry, "published", None),
            getattr(entry, "updated", None),
        )
        ts = _norm_ts_utc(ts_src)
        if pd.isna(ts):
            continue

        title = _clean_text(_as_str(getattr(entry, "title", "")))
        link  = _as_str(getattr(entry, "link", "")) or ""
        summ  = _clean_text(_as_str(getattr(entry, "summary", "")) or _as_str(getattr(entry, "description", "")))
        rows.append((ts, title, link, summ))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_bizinsider_rss(ticker: str, start: str, end: str, company: Optional[str] = None, limit: int = 60) -> pd.DataFrame:
    """
    Business Insider markets RSS (best-effort).
    """
    url = "https://www.businessinsider.com/sai/rss"
    feed = feedparser.parse(url)

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    entries = list(getattr(feed, "entries", []) or [])
    for i, entry in enumerate(entries):
        if i >= limit:
            break
        title_raw = _as_str(getattr(entry, "title", "")) or ""
        if ticker.upper() not in title_raw.upper():
            continue
        ts_src = _first_present(
            getattr(entry, "published_parsed", None),
            getattr(entry, "updated_parsed", None),
            getattr(entry, "published", None),
            getattr(entry, "updated", None),
        )
        ts = _norm_ts_utc(ts_src)
        if pd.isna(ts):
            continue
        link = _as_str(getattr(entry, "link", "")) or ""
        summ = _clean_text(_as_str(getattr(entry, "summary", "")) or _as_str(getattr(entry, "description", "")))
        rows.append((ts, _clean_text(title_raw), link, summ))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


# Register providers (order matters; fastest/most stable first)
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
