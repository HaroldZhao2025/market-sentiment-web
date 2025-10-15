# src/market_sentiment/news.py
from __future__ import annotations
import re
import time
import requests
from typing import List, Optional
import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup

_YF_UA = "Mozilla/5.0 (compatible; MarketSentimentBot/1.0; +https://github.com/)"
_YF_RSS = "https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"

def _norm_utc(ts) -> Optional[pd.Timestamp]:
    """
    Return a UTC tz-aware pandas Timestamp or None.
    Accepts epoch seconds or datelike strings.
    """
    if ts is None:
        return None
    try:
        # epoch seconds (int)
        return pd.to_datetime(int(ts), unit="s", utc=True)
    except Exception:
        pass
    try:
        # string / datetime
        return pd.to_datetime(ts, utc=True, errors="coerce")
    except Exception:
        return None

def _within(ts: pd.Timestamp, start: pd.Timestamp, end: pd.Timestamp) -> bool:
    if ts is None or pd.isna(ts):
        return False
    return (ts >= start) and (ts <= end)

def _clean_text(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _fetch_yf_news(ticker: str) -> List[dict]:
    """
    Use yfinance's Ticker.news (free) when available.
    Returns list of dicts with keys similar to:
      title, link, publisher, providerPublishTime, summary
    """
    try:
        t = yf.Ticker(ticker)
        items = t.news or []
        return items
    except Exception:
        return []

def _fetch_yf_rss(ticker: str) -> List[dict]:
    """
    Fallback: Yahoo Finance RSS feed (free, no key).
    Returns a list of dicts {title, link, pubDate}.
    """
    url = _YF_RSS.format(ticker=ticker)
    try:
        r = requests.get(url, headers={"User-Agent": _YF_UA}, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, features="xml")
        out = []
        for it in soup.find_all("item"):
            title = _clean_text((it.find("title") or {}).get_text() if it.find("title") else "")
            link = _clean_text((it.find("link") or {}).get_text() if it.find("link") else "")
            pub = _clean_text((it.find("pubDate") or {}).get_text() if it.find("pubDate") else "")
            out.append({"title": title, "link": link, "pubDate": pub})
        return out
    except Exception:
        return []

def fetch_news_yf(ticker: str, start: str, end: str, max_items: int = 400) -> pd.DataFrame:
    """
    Primary free news fetcher.
    Returns DataFrame with columns: ['ts','title','url','text']
    - ts is UTC tz-aware
    - text is (title + summary) if summary is present, else title
    """
    s_utc = pd.to_datetime(start, utc=True)
    # add almost one day to include end-of-day content in comparisons
    e_utc = pd.to_datetime(end, utc=True) + pd.Timedelta(hours=23, minutes=59)

    rows = []

    # 1) yfinance built-in news
    items = _fetch_yf_news(ticker)
    for it in items:
        title = _clean_text(it.get("title") or "")
        url = _clean_text(it.get("link") or "")
        if not title or not url:
            continue
        ts = _norm_utc(it.get("providerPublishTime"))
        if ts is None:
            # try 'published' key if present
            ts = _norm_utc(it.get("published"))
        if ts is None or not _within(ts, s_utc, e_utc):
            continue
        summary = _clean_text(it.get("summary") or "")
        text = _clean_text(f"{title}. {summary}".strip() or title)
        rows.append((ts, title, url, text))

    # If too few, try RSS fallback
    if len(rows) < 5:
        rss = _fetch_yf_rss(ticker)
        for it in rss:
            title = _clean_text(it.get("title") or "")
            url = _clean_text(it.get("link") or "")
            if not title or not url:
                continue
            ts = _norm_utc(it.get("pubDate"))
            if ts is None or not _within(ts, s_utc, e_utc):
                continue
            text = title  # RSS rarely has a quality summary; title-only is OK
            rows.append((ts, title, url, text))

    if not rows:
        return pd.DataFrame(columns=["ts","title","url","text"])

    df = pd.DataFrame(rows, columns=["ts","title","url","text"])
    # Deduplicate
    df = (
        df.sort_values("ts", ascending=False)
          .drop_duplicates(subset=["title","url"])
          .head(max_items)
          .reset_index(drop=True)
    )
    # Ensure utc tz-aware
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df
