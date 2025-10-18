# src/market_sentiment/news.py
from __future__ import annotations

import re
import time
import calendar
from typing import List, Tuple, Callable, Optional
from urllib.parse import quote_plus

import pandas as pd
import requests
import feedparser
import yfinance as yf

# -------------------- HTTP --------------------

_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/118.0 Safari/537.36"
)

def _http_get(url: str, timeout=(8, 20)) -> Optional[str]:
    try:
        r = requests.get(url, headers={"User-Agent": _UA}, timeout=timeout)
        if r.status_code == 200 and r.text:
            return r.text
    except Exception:
        pass
    return None

def _parse_rss_from(url: str, limit: int = 120) -> List[Tuple[pd.Timestamp, str, str, str]]:
    html = _http_get(url)
    if not html:
        return []
    feed = feedparser.parse(html)
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
        # tolerate missing timestamp (fallback to "now" so we don't drop items)
        if ts is pd.NaT:
            ts = pd.Timestamp.utcnow().tz_localize("UTC")
        title = _clean_text(getattr(entry, "title", ""))
        link  = getattr(entry, "link", "") or ""
        summary = _clean_text(
            getattr(entry, "summary", "")
            or getattr(entry, "description", "")
            or ""
        )
        rows.append((ts, title, link, summary))
    return rows

# -------------------- Utils --------------------

def _clean_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()

def _norm_ts_utc(x) -> pd.Timestamp:
    """
    Normalize to tz-aware UTC Timestamp.
    Accepts epoch, struct_time, or date-like strings. Returns NaT on failure.
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

    # string-like
    return pd.to_datetime(x, utc=True, errors="coerce")

def _window(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if df.empty:
        return df
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end,   utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    return df[(df["ts"] >= s) & (df["ts"] <= e)]

def _mk_df(rows: List[Tuple[pd.Timestamp, str, str, str]], ticker: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["ticker","ts","title","url","text"])
    df = pd.DataFrame(rows, columns=["ts","title","url","text"])
    df["ticker"] = ticker
    df["title"] = df["title"].map(_clean_text)
    df["text"]  = df["text"].map(_clean_text)
    df = df.dropna(subset=["ts"])
    df = df.drop_duplicates(["title","url"])
    df = df.sort_values("ts").reset_index(drop=True)
    return df[["ticker","ts","title","url","text"]]

# -------------------- Providers (free) --------------------

def _prov_google_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 120) -> pd.DataFrame:
    # proper URL-encoding avoids “control characters” errors in CI
    q = f'"{ticker}"'
    if company:
        q += f' OR "{company}"'
    q_enc = quote_plus(q)
    # broaden window to improve hit-rate in CI
    url = f"https://news.google.com/rss/search?q={q_enc}+when:365d&hl=en-US&gl=US&ceid=US:en"
    rows = _parse_rss_from(url, limit=limit)
    return _window(_mk_df(rows, ticker), start, end)

def _prov_yahoo_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 120) -> pd.DataFrame:
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={quote_plus(ticker)}&lang=en-US&region=US&count={limit}"
    rows = _parse_rss_from(url, limit=limit)
    return _window(_mk_df(rows, ticker), start, end)

def _prov_nasdaq_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 120) -> pd.DataFrame:
    url = f"https://www.nasdaq.com/feed/rssoutbound?symbol={quote_plus(ticker)}"
    rows = _parse_rss_from(url, limit=limit)
    return _window(_mk_df(rows, ticker), start, end)

def _prov_bizinsider_rss(ticker: str, start: str, end: str, company: str | None = None, limit: int = 120) -> pd.DataFrame:
    # broad tech feed → filter locally by ticker/company
    url = "https://www.businessinsider.com/sai/rss"
    rows = _parse_rss_from(url, limit=limit)
    if not rows:
        return pd.DataFrame(columns=["ticker","ts","title","url","text"])
    pat  = re.compile(rf"\b{re.escape(ticker)}\b", re.I)
    pat2 = re.compile(re.escape(company or ""), re.I) if company else None
    flt = []
    for ts, title, link, text in rows:
        s = f"{title} {text}"
        if pat.search(s) or (pat2 and pat2.search(s)):
            flt.append((ts, title, link, text))
    return _window(_mk_df(flt, ticker), start, end)

def _prov_yfinance(ticker: str, start: str, end: str, company: str | None = None, limit: int = 120) -> pd.DataFrame:
    # yfinance .news often lacks timestamps in CI — tolerate by using "now" fallback
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    try:
        raw = getattr(yf.Ticker(ticker), "news", None)
        if isinstance(raw, list):
            for item in raw[:limit]:
                ts = _norm_ts_utc(item.get("providerPublishTime"))
                if ts is pd.NaT:
                    ts = pd.Timestamp.utcnow().tz_localize("UTC")
                title = item.get("title") or ""
                url   = item.get("link") or item.get("url") or ""
                summary = item.get("summary") or ""
                # extremely short items add noise; keep title-only as text when summary missing
                rows.append((ts, title, url, summary or title))
    except Exception:
        pass
    return _window(_mk_df(rows, ticker), start, end)

# Order: RSS first, yfinance last
Provider = Callable[[str, str, str, Optional[str], int], pd.DataFrame]
_PROVIDERS: List[Provider] = [
    _prov_google_rss,
    _prov_yahoo_rss,
    _prov_nasdaq_rss,
    _prov_bizinsider_rss,
    _prov_yfinance,
]

# -------------------- Public API --------------------

def fetch_news(
    ticker: str,
    start: str,
    end: str,
    company: str | None = None,
    max_per_provider: int = 120,
) -> pd.DataFrame:
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

    df = pd.concat(frames, ignore_index=True)
    df["title"] = df["title"].map(_clean_text)
    df["text"]  = df["text"].map(_clean_text)
    df = df.drop_duplicates(["title","url"])
    df = df.sort_values("ts").reset_index(drop=True)
    return df
