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
            sec = calendar.timegm(x)  # type: ignore
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
    return df[(df["ts"] >= s) & (df["ts"] <= e)].copy()


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


def _retry(fn, tries: int = 3, base_delay: float = 0.8):
    for i in range(tries):
        try:
            return fn()
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(base_delay * (2 ** i))


def _month_windows(start: str, end: str):
    """
    Yield (month_start_UTC, month_end_UTC) pairs covering [start, end] inclusive.
    """
    s = pd.Timestamp(start, tz="UTC").normalize()
    e = pd.Timestamp(end, tz="UTC").normalize()
    cur = s
    while cur <= e:
        w_end = min((cur + pd.offsets.MonthEnd(0)), e)
        yield cur, w_end
        cur = (w_end + pd.Timedelta(days=1)).normalize()


# ------------------------
# Providers
# ------------------------

def _prov_finnhub(
    ticker: str, start: str, end: str, company: str | None = None, limit: int = 6000
) -> pd.DataFrame:
    """
    Finnhub company-news (https://finnhub.io/docs/api/company-news)
    - Reads API key from FINNHUB_TOKEN / FINNHUB_API_KEY / FINNHUB_KEY
    - Pulls month-by-month to avoid server caps and ensure full-year coverage
    """
    token = os.getenv("FINNHUB_TOKEN") or os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_KEY")
    if not token:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    fetched = 0

    for ws, we in _month_windows(start, end):
        if fetched >= limit:
            break

        params = {
            "symbol": ticker,
            "from": str(ws.date()),
            "to": str(we.date()),
            "token": token,
        }

        def _fetch():
            r = requests.get("https://finnhub.io/api/v1/company-news", params=params, timeout=20)
            if r.status_code in (429, 502, 503, 504):
                raise RuntimeError(f"Finnhub HTTP {r.status_code}")
            r.raise_for_status()
            return r.json()

        try:
            data = _retry(_fetch, tries=3, base_delay=1.0)
        except Exception:
            continue

        if not isinstance(data, list):
            continue

        for item in data:
            if fetched >= limit:
                break
            if not isinstance(item, dict):
                continue
            ts = _norm_ts_utc(item.get("datetime"))
            if pd.isna(ts):
                continue
            title = _first_str(item.get("headline"))
            url   = _first_str(item.get("url"))
            text  = _first_str(item.get("summary"), title)
            if not title and not text:
                continue
            rows.append((ts, title or text, url, text or title))
            fetched += 1

        # be gentle on rate limit
        time.sleep(0.25)

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_gdelt(
    ticker: str, start: str, end: str, company: str | None = None, limit: int = 6000
) -> pd.DataFrame:
    """
    GDELT Doc API – no API key; good coverage & date filters.
    We query month-by-month to harvest many items (up to `limit` total).
    """
    q = quote_plus((company or ticker).replace("&", " and "))
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []

    fetched = 0
    for ws, we in _month_windows(start, end):
        if fetched >= limit:
            break
        startdt = ws.strftime("%Y%m%d%H%M%S")
        enddt   = (we + pd.Timedelta(hours=23, minutes=59, seconds=59)).strftime("%Y%m%d%H%M%S")
        # per-month max (GDELT caps anyway); keep requests light
        per = min(1000, limit - fetched)
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

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_yfinance(
    ticker: str, start: str, end: str, company: str | None = None, limit: int = 300
) -> pd.DataFrame:
    """
    yfinance .news (no key). Structure varies by version, so be defensive.
    """
    def _get():
        try:
            t = yf.Ticker(ticker)
            return getattr(t, "news", None)
        except Exception:
            return None

    raw = _retry(_get, tries=2, base_delay=0.6)

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    if isinstance(raw, list):
        for item in raw[:limit]:
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
    # First attempt: a single 365d window (fast)
    q = f'"{ticker}"' + (f' OR "{company}"' if company else "")
    url = f"https://news.google.com/rss/search?q={quote_plus(q)}+when:365d&hl=en-US&gl=US&ceid=US:en"

    def _get(url_):
        return feedparser.parse(url_, request_headers={"User-Agent": "Mozilla/5.0"})

    try:
        feed = _retry(lambda: _get(url), tries=2, base_delay=0.6)
    except Exception:
        feed = None

    def _consume(feed_) -> pd.DataFrame:
        rows: List[Tuple[pd.Timestamp, str, str, str]] = []
        for i, entry in enumerate(getattr(feed_, "entries", []) if feed_ else []):
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
        return _mk_df(rows, ticker)

    df = _consume(feed)

    # Fallback: if coverage is thin (< 40 items), fetch month-by-month for the whole range
    if len(df) < 40:
        frames = []
        for ws, we in _month_windows(start, end):
            qp = f'"{ticker}"' + (f' OR "{company}"' if company else "")
            qp = quote_plus(f"{qp} after:{ws.date()} before:{(we + pd.Timedelta(days=1)).date()}")
            url_m = f"https://news.google.com/rss/search?q={qp}&hl=en-US&gl=US&ceid=US:en"
            try:
                feed_m = _retry(lambda: _get(url_m), tries=2, base_delay=0.6)
                frames.append(_consume(feed_m))
            except Exception:
                continue
        if frames:
            df = pd.concat([df] + frames, ignore_index=True).drop_duplicates(["title", "url"])

    return _window_filter(df.sort_values("ts").reset_index(drop=True), start, end)


def _prov_yahoo_rss(
    ticker: str, start: str, end: str, company: str | None = None, limit: int = 300
) -> pd.DataFrame:
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={quote_plus(ticker)}&lang=en-US&region=US&count={limit}"

    def _get():
        return feedparser.parse(url, request_headers={"User-Agent": "Mozilla/5.0"})

    try:
        feed = _retry(_get, tries=2, base_delay=0.6)
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
        feed = _retry(_get, tries=2, base_delay=0.8)
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

# Base providers (good coverage even without keys)
_BASE_PROVIDERS: List[Provider] = [
    _prov_gdelt,        # depth and date control
    _prov_yfinance,     # quick
    _prov_google_rss,   # general news
    _prov_yahoo_rss,    # finance feed
    _prov_nasdaq_rss,   # finance feed
]

# Conditionally prepend Finnhub when token present
_PROVIDERS: List[Provider] = []
if os.getenv("FINNHUB_TOKEN") or os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_KEY"):
    _PROVIDERS.append(_prov_finnhub)
_PROVIDERS.extend(_BASE_PROVIDERS)


def fetch_news(
    ticker: str,
    start: str,
    end: str,
    company: str | None = None,
    max_per_provider: int = 6000,
) -> pd.DataFrame:
    """
    Fan out across multiple providers (Finnhub if available, GDELT, RSS, yfinance),
    dedupe by (title, url), return sorted dataframe with:
      ['ticker', 'ts', 'title', 'url', 'text']
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
    df = df.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    return df
