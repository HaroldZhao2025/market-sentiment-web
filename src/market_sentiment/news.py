# src/market_sentiment/news.py
from __future__ import annotations

import calendar
import time
from typing import List, Tuple, Callable, Optional
from urllib.parse import quote_plus

import pandas as pd
import feedparser
import requests
import yfinance as yf

# ------------------------
# Helpers
# ------------------------

def _clean_text(x) -> str:
    try:
        s = str(x)
    except Exception:
        return ""
    return " ".join(s.split())

def _first_str(*candidates) -> str:
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
        if xi > 10_000_000_000:  # ms
            xi = xi / 1000.0
        return pd.Timestamp.utcfromtimestamp(xi).tz_localize("UTC")
    except Exception:
        pass

    # generic parse
    try:
        return pd.to_datetime(x, utc=True, errors="coerce")
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

def _month_windows(start: str, end: str):
    s = pd.Timestamp(start, tz="UTC").normalize()
    e = pd.Timestamp(end, tz="UTC").normalize()
    cur = s
    while cur <= e:
        w_end = min((cur + pd.offsets.MonthEnd(0)), e)
        yield cur, w_end
        cur = (w_end + pd.Timedelta(days=1)).normalize()

# ------------------------
# Providers (non-Finnhub; each capped to <= 240)
# ------------------------

def _prov_yfinance(
    ticker: str, start: str, end: str, company: str | None = None, limit: int = 240
) -> pd.DataFrame:
    """
    EXACT call you asked for:
        t = yf.Ticker(ticker)
        items = t.get_news(count=240, tab="all")
    """
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    try:
        t = yf.Ticker(ticker)
        items = t.get_news(count=int(limit), tab="all") or []
    except Exception:
        items = []

    for it in items[: int(limit)]:
        ts = _norm_ts_utc(
            it.get("providerPublishTime")
            or it.get("provider_publish_time")
            or it.get("published_at")
            or it.get("pubDate")
        )
        if pd.isna(ts):
            continue
        title = _first_str(it.get("title"))
        link  = _first_str(it.get("link"), it.get("url"))
        text  = _first_str(
            it.get("summary"),
            (it.get("content") or {}).get("summary") if isinstance(it.get("content"), dict) else "",
            title
        )
        if not title and not text:
            continue
        rows.append((ts, title or text, link, text or title))

    return _window_filter(_mk_df(rows, ticker), start, end)

def _prov_google_rss(
    ticker: str, start: str, end: str, company: str | None = None, limit: int = 240
) -> pd.DataFrame:
    q = f'"{ticker}"' + (f' OR "{company}"' if company else "")
    url = f"https://news.google.com/rss/search?q={quote_plus(q)}+when:365d&hl=en-US&gl=US&ceid=US:en"
    def _get(url_):
        return feedparser.parse(url_, request_headers={"User-Agent": "Mozilla/5.0"})
    try:
        feed = _retry(lambda: _get(url), tries=2, delay=0.6)
    except Exception:
        feed = None

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for i, entry in enumerate(getattr(feed, "entries", []) if feed else []):
        if i >= int(limit): break
        ts = _norm_ts_utc(
            getattr(entry, "published_parsed", None)
            or getattr(entry, "updated_parsed", None)
            or getattr(entry, "published", None)
            or getattr(entry, "updated", None)
        )
        if pd.isna(ts): continue
        title = _first_str(getattr(entry, "title", None))
        link  = _first_str(getattr(entry, "link", None))
        text  = _first_str(getattr(entry, "summary", None), getattr(entry, "description", None), title)
        if not title and not text: continue
        rows.append((ts, title or text, link, text or title))
    return _window_filter(_mk_df(rows, ticker), start, end)

def _prov_yahoo_rss(
    ticker: str, start: str, end: str, company: str | None = None, limit: int = 240
) -> pd.DataFrame:
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={quote_plus(ticker)}&lang=en-US&region=US&count={int(limit)}"
    def _get():
        return feedparser.parse(url, request_headers={"User-Agent": "Mozilla/5.0"})
    try:
        feed = _retry(_get, tries=2, delay=0.6)
    except Exception:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for i, entry in enumerate(getattr(feed, "entries", [])):
        if i >= int(limit): break
        ts = _norm_ts_utc(
            getattr(entry, "published_parsed", None)
            or getattr(entry, "updated_parsed", None)
            or getattr(entry, "published", None)
            or getattr(entry, "updated", None)
        )
        if pd.isna(ts): continue
        title = _first_str(getattr(entry, "title", None))
        link  = _first_str(getattr(entry, "link", None))
        text  = _first_str(getattr(entry, "summary", None), getattr(entry, "description", None), title)
        if not title and not text: continue
        rows.append((ts, title or text, link, text or title))
    return _window_filter(_mk_df(rows, ticker), start, end)

def _prov_nasdaq_rss(
    ticker: str, start: str, end: str, company: str | None = None, limit: int = 240
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
        if i >= int(limit): break
        ts = _norm_ts_utc(
            getattr(entry, "published_parsed", None)
            or getattr(entry, "updated_parsed", None)
            or getattr(entry, "published", None)
            or getattr(entry, "updated", None)
        )
        if pd.isna(ts): continue
        title = _first_str(getattr(entry, "title", None))
        link  = _first_str(getattr(entry, "link", None))
        text  = _first_str(getattr(entry, "summary", None), getattr(entry, "description", None), title)
        if not title and not text: continue
        rows.append((ts, title or text, link, text or title))
    return _window_filter(_mk_df(rows, ticker), start, end)

# ---- Smoke-test compatibility shims ----

def _prov_gdelt(ticker: str, start: str, end: str, company: str | None = None, limit: int = 240) -> pd.DataFrame:
    q = quote_plus((company or ticker).replace("&", " and "))
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    fetched = 0
    for ws, we in _month_windows(start, end):
        if fetched >= limit:
            break
        per = min(1000, limit - fetched)
        startdt = ws.strftime("%Y%m%d%H%M%S")
        enddt   = (we + pd.Timedelta(hours=23, minutes=59, seconds=59)).strftime("%Y%m%d%H%M%S")
        url = (
            "https://api.gdeltproject.org/api/v2/doc/doc"
            f"?query={q}&mode=artlist&format=json&maxrecords={per}"
            f"&startdatetime={startdt}&enddatetime={enddt}"
        )
        try:
            r = requests.get(url, timeout=30)
            if not r.ok:
                continue
            js = r.json() or {}
            arts = js.get("articles") or []
            for a in arts:
                ts = _norm_ts_utc(a.get("seendate") or a.get("date"))
                title = _first_str(a.get("title"))
                link  = _first_str(a.get("url"))
                if pd.isna(ts) or not title:
                    continue
                rows.append((ts, title, link, title))
            fetched += len(arts)
        except Exception:
            continue
    return _window_filter(_mk_df(rows, ticker), start, end)

def _prov_finnhub(ticker: str, start: str, end: str, company: str | None = None, limit: int = 240) -> pd.DataFrame:
    # Delegates to daily per-day exact call implemented in news_finnhub_daily.py
    try:
        from .news_finnhub_daily import fetch_finnhub_daily_news
    except Exception:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    return fetch_finnhub_daily_news(ticker, start, end, rps=20.0)

# ------------------------
# Public API (non-Finnhub aggregation)
# ------------------------

Provider = Callable[[str, str, str, Optional[str], int], pd.DataFrame]

_PROVIDERS: List[Provider] = [
    _prov_yfinance,    # uses count=240, tab="all"
    _prov_google_rss,
    _prov_yahoo_rss,
    _prov_nasdaq_rss,
]

def fetch_news(
    ticker: str,
    start: str,
    end: str,
    company: str | None = None,
    max_per_provider: int = 240,
) -> pd.DataFrame:
    cap = min(240, int(max_per_provider or 240))
    frames: List[pd.DataFrame] = []
    for prov in _PROVIDERS:
        try:
            df = prov(ticker, start, end, company, limit=cap)
        except Exception:
            df = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    return out
