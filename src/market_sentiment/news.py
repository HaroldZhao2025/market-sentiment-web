# src/market_sentiment/news.py
from __future__ import annotations

import calendar
import os
import time
from typing import List, Tuple, Callable, Optional
from urllib.parse import quote_plus

import pandas as pd
import feedparser
import requests
import yfinance as yf

# pip install finnhub-python  (import name is "finnhub")
try:
    import finnhub
except Exception:
    finnhub = None

# ------------------------ helpers ------------------------

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
        if xi > 10_000_000_000:
            xi = xi / 1000.0
        return pd.Timestamp.utcfromtimestamp(xi).tz_localize("UTC")
    except Exception:
        pass
    # parse string
    try:
        return pd.to_datetime(x, utc=True, errors="coerce")
    except Exception:
        return pd.NaT

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
    df = df.dropna(subset=["ts"])
    df["title"] = df["title"].map(_clean_text)
    df["text"] = df["text"].map(_clean_text)
    df = df.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    return df[["ticker", "ts", "title", "url", "text"]]

def _retry(fn, tries=2, delay=0.6):
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

# ------------------------ providers ------------------------

def _prov_finnhub(
    ticker: str, start: str, end: str, company: str | None = None, limit_days: int = 366
) -> pd.DataFrame:
    """
    EXACT Finnhub usage (per-day), throttled to FINNHUB_RPS (<= 30 req/s):

        import finnhub
        client = finnhub.Client(api_key="…")
        client.company_news('AAPL', _from='YYYY-MM-DD', to='YYYY-MM-DD')

    Env keys checked (first found wins): FINNHUB_TOKEN, FINNHUB_API_KEY, FINNHUB_KEY
    """
    token = os.getenv("FINNHUB_TOKEN") or os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_KEY")
    if not token or finnhub is None:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    try:
        client = finnhub.Client(api_key=token)
    except Exception:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    rps = max(1, int(os.getenv("FINNHUB_RPS", "10")))  # default 10; must be <= 30
    gap = 1.0 / float(rps)

    s = pd.Timestamp(start, tz="UTC").normalize()
    e = pd.Timestamp(end, tz="UTC").normalize()
    days = list(pd.date_range(s, e, freq="D", tz="UTC"))
    if len(days) > int(limit_days):
        days = days[: int(limit_days)]

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    last = 0.0
    for d in days:
        day = d.date().isoformat()
        # throttle
        now = time.time()
        sleep_for = max(0.0, (last + gap) - now)
        if sleep_for > 0:
            time.sleep(sleep_for)
        last = time.time()

        try:
            arr = client.company_news(ticker, _from=day, to=day) or []
        except Exception:
            continue

        for it in arr:
            ts = _norm_ts_utc(it.get("datetime"))
            if pd.isna(ts):
                continue
            title = _first_str(it.get("headline"))
            link = _first_str(it.get("url"))
            text = _first_str(it.get("summary"), title)
            if not title and not text:
                continue
            rows.append((ts, title or text, link, text or title))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_yfinance(
    ticker: str, start: str, end: str, company: str | None = None, count: int = 240
) -> pd.DataFrame:
    """
    Use the deep endpoint explicitly:
        t.get_news(count=240, tab="all")
    (fallback to .news if get_news unavailable)
    """
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    try:
        t = yf.Ticker(ticker)
        # prefer get_news(count=…, tab="all")
        items = None
        try:
            items = t.get_news(count=int(count), tab="all")
        except Exception:
            pass
        if items is None:
            items = t.news or []
    except Exception:
        items = []

    for item in items or []:
        content = item.get("content") if isinstance(item, dict) else None
        ts = _norm_ts_utc(
            (item.get("providerPublishTime") if isinstance(item, dict) else None)
            or (item.get("provider_publish_time") if isinstance(item, dict) else None)
            or (item.get("published_at") if isinstance(item, dict) else None)
            or (item.get("pubDate") if isinstance(item, dict) else None)
            or ((content or {}).get("published") if isinstance(content, dict) else None)
            or ((content or {}).get("pubDate") if isinstance(content, dict) else None)
        )
        if pd.isna(ts):
            continue
        title = _first_str(item.get("title"), (content or {}).get("title"))
        link = _first_str(item.get("link"), item.get("url"), (content or {}).get("link"), (content or {}).get("url"))
        text = _first_str(item.get("summary"), (content or {}).get("summary"), (content or {}).get("content"), title)
        if not title and not text:
            continue
        rows.append((ts, title or text, link, text or title))

    return _window_filter(_mk_df(rows, ticker), start, end)


def _prov_google_rss(
    ticker: str, start: str, end: str, company: str | None = None, limit: int = 400
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
        link = _first_str(getattr(entry, "link", None))
        text = _first_str(getattr(entry, "summary", None), getattr(entry, "description", None), title)
        if not title and not text:
            continue
        rows.append((ts, title or text, link, text or title))

    return _window_filter(_mk_df(rows, ticker), start, end)


def _prov_yahoo_rss(
    ticker: str, start: str, end: str, company: str | None = None, limit: int = 400
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
        link = _first_str(getattr(entry, "link", None))
        text = _first_str(getattr(entry, "summary", None), getattr(entry, "description", None), title)
        if not title and not text:
            continue
        rows.append((ts, title or text, link, text or title))

    return _window_filter(_mk_df(rows, ticker), start, end)


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
        link = _first_str(getattr(entry, "link", None))
        text = _first_str(getattr(entry, "summary", None), getattr(entry, "description", None), title)
        if not title and not text:
            continue
        rows.append((ts, title or text, link, text or title))

    return _window_filter(_mk_df(rows, ticker), start, end)

# ------------------------ public API ------------------------

Provider = Callable[[str, str, str, Optional[str], int], pd.DataFrame]

_PROVIDERS: List[Provider] = [
    _prov_finnhub,    # freshest & complete day-by-day
    _prov_yfinance,   # deep batch (~200)
    _prov_yahoo_rss,  # additional coverage
    _prov_google_rss,
    _prov_nasdaq_rss,
]

def fetch_news(
    ticker: str,
    start: str,
    end: str,
    company: str | None = None,
    max_per_provider: int = 600,  # yfinance count; finnhub uses as day-cap
) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for prov in _PROVIDERS:
        try:
            df = prov(ticker, start, end, company, max_per_provider)
            if df is not None and not df.empty:
                frames.append(df)
        except Exception:
            # continue rather than failing the whole fetch
            pass

    if not frames:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    return out
