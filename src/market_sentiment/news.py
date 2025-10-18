# src/market_sentiment/news.py
from __future__ import annotations

import calendar
import os
import re
from typing import List, Tuple, Callable, Optional
from urllib.parse import quote_plus

import pandas as pd
import feedparser
import yfinance as yf  # free


# ============================================================
# Helpers
# ============================================================

def _clean_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s


def _norm_ts_utc(x) -> pd.Timestamp:
    """
    Normalize to tz-aware UTC Timestamp.
    Accepts: epoch seconds/ms, time.struct_time, or date-like strings.
    Returns pd.NaT on failure.
    """
    if x is None:
        return pd.NaT

    # feedparser often gives time.struct_time
    if hasattr(x, "tm_year"):
        try:
            sec = calendar.timegm(x)  # treat as UTC
            return pd.Timestamp.utcfromtimestamp(sec).tz_localize("UTC")
        except Exception:
            pass

    # epoch seconds/ms as int-ish string
    try:
        xi = int(str(x).strip())
        if xi > 10_000_000_000:  # likely ms
            xi = xi / 1000.0
        return pd.Timestamp.utcfromtimestamp(xi).tz_localize("UTC")
    except Exception:
        pass

    # fall back to pandas parser
    ts = pd.to_datetime(x, utc=True, errors="coerce")
    return ts


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
    df = df.dropna(subset=["ts"])
    df = df.drop_duplicates(["title", "url"])
    df = df.sort_values("ts").reset_index(drop=True)
    return df[["ticker", "ts", "title", "url", "text"]]


def _parse_feed(url: str) -> feedparser.FeedParserDict:
    """
    feedparser supports an 'agent' kwarg which sets User-Agent and helps
    avoid some 403s on RSS endpoints.
    """
    try:
        return feedparser.parse(url, agent="Mozilla/5.0 (compatible; MarketSentimentBot/1.0)")
    except Exception:
        # Return an empty structure if anything goes wrong
        return feedparser.FeedParserDict(entries=[])


# ============================================================
# Providers (all free / no key)
# ============================================================

def _prov_yfinance(ticker: str, start: str, end: str, company: str | None = None, limit: int = 80) -> pd.DataFrame:
    """
    yfinance .news
    """
    try:
        t = yf.Ticker(ticker)
        raw = getattr(t, "news", None)
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


def _prov_google_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 80) -> pd.DataFrame:
    """
    Google News RSS search
    """
    q = f'"{ticker}"'
    if company:
        q += f' OR "{company}"'
    # Use a 30d window (RSS itself has no strict date filter; we filter afterward)
    url = f"https://news.google.com/rss/search?q={quote_plus(q)}+when:30d&hl=en-US&gl=US&ceid=US:en"
    feed = _parse_feed(url)

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
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_bing_news_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 80) -> pd.DataFrame:
    """
    Bing News RSS search (free)
    """
    q = f'"{ticker}"'
    if company:
        q += f' OR "{company}"'
    # Request newest first (qft=sortbydate) and RSS format
    url = f"https://www.bing.com/news/search?q={quote_plus(q)}&qft=sortbydate&format=rss"
    feed = _parse_feed(url)

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
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_yahoo_news_search(ticker: str, start: str, end: str, company: str | None = None, limit: int = 80) -> pd.DataFrame:
    """
    Yahoo News search RSS (free)
    """
    q = f'"{ticker}"'
    if company:
        q += f' OR "{company}"'
    url = f"https://news.search.yahoo.com/rss?p={quote_plus(q)}"
    feed = _parse_feed(url)

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
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_yahoo_finance_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 80) -> pd.DataFrame:
    """
    Yahoo Finance symbol RSS
    """
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={quote_plus(ticker)}&lang=en-US&region=US&count={limit}"
    feed = _parse_feed(url)

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for entry in getattr(feed, "entries", []):
        ts = _norm_ts_utc(
            getattr(entry, "published_parsed", None)
            or getattr(entry, "updated_parsed", None)
            or getattr(entry, "published", None)
            or getattr(entry, "updated", None)
        )
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_nasdaq_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 80) -> pd.DataFrame:
    """
    Nasdaq symbol RSS
    """
    url = f"https://www.nasdaq.com/feed/rssoutbound?symbol={quote_plus(ticker)}"
    feed = _parse_feed(url)

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
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_seekingalpha_combined(ticker: str, start: str, end: str, company: str | None = None, limit: int = 80) -> pd.DataFrame:
    """
    Seeking Alpha 'combined' feed (articles + PR) by symbol (no key).
    Example: https://seekingalpha.com/api/sa/combined/AAPL.xml
    """
    url = f"https://seekingalpha.com/api/sa/combined/{quote_plus(ticker)}.xml"
    feed = _parse_feed(url)

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
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_globenewswire_search(ticker: str, start: str, end: str, company: str | None = None, limit: int = 80) -> pd.DataFrame:
    """
    GlobeNewswire RSS search (no key)
    """
    q = f'"{ticker}"'
    if company:
        q += f' OR "{company}"'
    url = f"https://www.globenewswire.com/RssSearch/Index?search={quote_plus(q)}"
    feed = _parse_feed(url)

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
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_businesswire_search(ticker: str, start: str, end: str, company: str | None = None, limit: int = 80) -> pd.DataFrame:
    """
    Business Wire search RSS (best-effort).
    If BW changes params, this provider will just yield zero rows.
    """
    q = f'"{ticker}"'
    if company:
        q += f' OR "{company}"'
    # documented-ish search param: rss=1
    url = f"https://www.businesswire.com/portal/site/home/news/?searchType=news&searchTerm={quote_plus(q)}&rss=1"
    feed = _parse_feed(url)

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
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


# ============================================================
# Public API
# ============================================================

Provider = Callable[[str, str, str, Optional[str], int], pd.DataFrame]

# Order roughly from fastest/broadest → more niche PR sources
_PROVIDERS: List[Provider] = [
    _prov_yfinance,
    _prov_google_rss,
    _prov_bing_news_rss,
    _prov_yahoo_news_search,
    _prov_yahoo_finance_rss,
    _prov_nasdaq_rss,
    _prov_seekingalpha_combined,
    _prov_globenewswire_search,
    _prov_businesswire_search,
]


def fetch_news(ticker: str, start: str, end: str, company: str | None = None, max_per_provider: int = 80) -> pd.DataFrame:
    """
    Combine multiple free providers, normalize & dedupe.
    Output columns: ticker, ts (tz-aware UTC), title, url, text
    Environment knobs (optional):
      - NEWS_ITEMS_PER_SOURCE     (default: max_per_provider)
      - NEWS_SOURCES              (comma list of provider keys; default: all)
        Keys: yfinance, google, bing, yahoosearch, yahoofin, nasdaq, sa, globe, bw
    """
    # Allow operator to reduce/increase items without code edit
    try:
        max_per_provider = int(os.getenv("NEWS_ITEMS_PER_SOURCE", max_per_provider))
    except Exception:
        pass

    # Allow restricting providers via env (for quick experiments)
    keymap = {
        "yfinance": _prov_yfinance,
        "google": _prov_google_rss,
        "bing": _prov_bing_news_rss,
        "yahoosearch": _prov_yahoo_news_search,
        "yahoofin": _prov_yahoo_finance_rss,
        "nasdaq": _prov_nasdaq_rss,
        "sa": _prov_seekingalpha_combined,
        "globe": _prov_globenewswire_search,
        "bw": _prov_businesswire_search,
    }
    sources_env = os.getenv("NEWS_SOURCES", "").strip()
    providers: List[Provider]
    if sources_env:
        picks = [keymap.get(k.strip().lower()) for k in sources_env.split(",")]
        providers = [p for p in picks if p is not None]
        if not providers:
            providers = _PROVIDERS  # fallback
    else:
        providers = _PROVIDERS

    frames: List[pd.DataFrame] = []
    for prov in providers:
        try:
            df = prov(ticker, start, end, company, max_per_provider)
        except Exception:
            df = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    df = pd.concat(frames, ignore_index=True)
    # Final clean & dedupe across providers
    df["title"] = df["title"].map(_clean_text)
    df["text"] = df["text"].map(_clean_text)
    df = df.drop_duplicates(["title", "url"])
    df = df.sort_values("ts").reset_index(drop=True)
    return df
