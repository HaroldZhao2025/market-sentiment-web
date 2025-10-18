# src/market_sentiment/news.py
from __future__ import annotations

import re
import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Optional, Callable

import pandas as pd
import feedparser
import requests
import yfinance as yf

def _clean_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s

def _norm_ts_utc(x) -> pd.Timestamp:
    if x is None:
        return pd.NaT
    if hasattr(x, "tm_year"):
        try:
            sec = calendar.timegm(x)
            return pd.Timestamp.utcfromtimestamp(sec).tz_localize("UTC")
        except Exception:
            return pd.NaT
    try:
        xi = int(x)
        if xi > 10_000_000_000:
            xi = xi / 1000.0
        return pd.Timestamp.utcfromtimestamp(xi).tz_localize("UTC")
    except Exception:
        pass
    return pd.to_datetime(x, utc=True, errors="coerce")

def _mk_df(rows: List[Tuple[pd.Timestamp, str, str, str]], ticker: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["ticker","ts","title","url","text"])
    df = pd.DataFrame(rows, columns=["ts","title","url","text"])
    df["ticker"] = ticker
    df = df.dropna(subset=["ts"])
    df["title"] = df["title"].map(_clean_text)
    df["text"]  = df["text"].map(_clean_text)
    df = df.drop_duplicates(["title","url"]).sort_values("ts").reset_index(drop=True)
    return df[["ticker","ts","title","url","text"]]

def _window_filter(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if df.empty: return df
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end,   utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    return df[(df["ts"] >= s) & (df["ts"] <= e)]

def _rss(url: str, timeout: int = 6) -> feedparser.FeedParserDict:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent":"Mozilla/5.0"})
        r.raise_for_status()
        return feedparser.parse(r.content)
    except Exception:
        try:
            return feedparser.parse(url)
        except Exception:
            return feedparser.parse(b"")

def _rows_from_feed(feed: feedparser.FeedParserDict, limit: int = 40) -> List[Tuple[pd.Timestamp, str, str, str]]:
    out: List[Tuple[pd.Timestamp, str, str, str]] = []
    for i, entry in enumerate(getattr(feed, "entries", [])):
        if i >= limit: break
        ts = _norm_ts_utc(
            getattr(entry, "published_parsed", None)
            or getattr(entry, "updated_parsed", None)
            or getattr(entry, "published", None)
            or getattr(entry, "updated", None)
        )
        if ts is pd.NaT: continue
        title = _clean_text(getattr(entry, "title", ""))
        link  = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        out.append((ts, title, link, summary))
    return out

# Providers
def _prov_yfinance(ticker: str, start: str, end: str, company: Optional[str], limit: int) -> pd.DataFrame:
    try:
        raw = getattr(yf.Ticker(ticker), "news", None)
    except Exception:
        raw = None
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    if isinstance(raw, list):
        for item in raw[:limit]:
            ts = _norm_ts_utc(item.get("providerPublishTime"))
            if ts is pd.NaT: continue
            title = item.get("title") or ""
            url   = item.get("link") or item.get("url") or ""
            summary = item.get("summary") or ""
            rows.append((ts, title, url, summary))
    return _window_filter(_mk_df(rows, ticker), start, end)

def _prov_google_rss(ticker: str, start: str, end: str, company: Optional[str], limit: int) -> pd.DataFrame:
    q = f'"{ticker}"' + (f' OR "{company}"' if company else "")
    url = f"https://news.google.com/rss/search?q={q}+when:30d&hl=en-US&gl=US&ceid=US:en"
    return _window_filter(_mk_df(_rows_from_feed(_rss(url), limit), ticker), start, end)

def _prov_yahoo_rss(ticker: str, start: str, end: str, company: Optional[str], limit: int) -> pd.DataFrame:
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&lang=en-US&region=US&count={limit}"
    return _window_filter(_mk_df(_rows_from_feed(_rss(url), limit), ticker), start, end)

def _prov_nasdaq_rss(ticker: str, start: str, end: str, company: Optional[str], limit: int) -> pd.DataFrame:
    return _window_filter(_mk_df(_rows_from_feed(_rss(f"https://www.nasdaq.com/feed/rssoutbound?symbol={ticker}"), limit), ticker), start, end)

def _prov_marketwatch_rss(ticker: str, start: str, end: str, company: Optional[str], limit: int) -> pd.DataFrame:
    q = requests.utils.quote(_clean_text(company or ticker))
    return _window_filter(_mk_df(_rows_from_feed(_rss(f"https://www.marketwatch.com/rss/search?q={q}"), limit), ticker), start, end)

def _prov_investopedia_rss(ticker: str, start: str, end: str, company: Optional[str], limit: int) -> pd.DataFrame:
    q = requests.utils.quote(_clean_text(company or ticker))
    return _window_filter(_mk_df(_rows_from_feed(_rss(f"https://www.investopedia.com/search/rss?q={q}"), limit), ticker), start, end)

def _prov_prnewswire_rss(ticker: str, start: str, end: str, company: Optional[str], limit: int) -> pd.DataFrame:
    q = requests.utils.quote(_clean_text(company or ticker))
    return _window_filter(_mk_df(_rows_from_feed(_rss(f"https://www.prnewswire.com/ap/rss/NewsSearch/{q}.rss"), limit), ticker), start, end)

def _prov_businesswire_rss(ticker: str, start: str, end: str, company: Optional[str], limit: int) -> pd.DataFrame:
    q = requests.utils.quote(_clean_text(company or ticker))
    url = ("https://www.businesswire.com/portal/site/home/template.RSS/"
           "?javax.portlet.tpst=3475e9c8b0a4f8d4c86f0f10a4f8a62a_ws_MX"
           "&javax.portlet.prp_3475e9c8b0a4f8d4c86f0f10a4f8a62a_viewID=MY_PORTAL_VIEW"
           "&javax.portlet.prp_3475e9c8b0a4f8d4c86f0f10a4f8a62a_ndm=1"
           "&javax.portlet.begCacheTok=com.vignette.cachetoken"
           "&javax.portlet.endCacheTok=com.vignette.cachetoken"
           f"&javax.portlet.prp_3475e9c8b0a4f8d4c86f0f10a4f8a62a_keywords={q}")
    return _window_filter(_mk_df(_rows_from_feed(_rss(url), limit), ticker), start, end)

Provider = Callable[[str, str, str, Optional[str], int], pd.DataFrame]
_PROVIDERS: List[Provider] = [
    _prov_yfinance,
    _prov_google_rss,
    _prov_yahoo_rss,
    _prov_nasdaq_rss,
    _prov_marketwatch_rss,
    _prov_investopedia_rss,
    _prov_prnewswire_rss,
    _prov_businesswire_rss,
]

def fetch_news(ticker: str, start: str, end: str, company: Optional[str] = None, max_per_provider: int = 40, max_workers: int = 6) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    def call(p: Provider) -> pd.DataFrame:
        try:
            return p(ticker, start, end, company, max_per_provider)
        except Exception:
            return pd.DataFrame(columns=["ticker","ts","title","url","text"])
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(call, p) for p in _PROVIDERS]
        for f in as_completed(futs):
            df = f.result()
            if not df.empty:
                frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["ticker","ts","title","url","text"])
    df = pd.concat(frames, ignore_index=True)
    df["title"] = df["title"].map(_clean_text)
    df["text"]  = df["text"].map(_clean_text)
    df = df.drop_duplicates(["title","url"]).sort_values("ts").reset_index(drop=True)
    return df
