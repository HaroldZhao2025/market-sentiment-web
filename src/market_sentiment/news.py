from __future__ import annotations
import re
from typing import List, Tuple
import pandas as pd
import yfinance as yf
import feedparser

def _norm_ts_utc(x) -> pd.Timestamp:
    if pd.isna(x):
        return pd.NaT
    try:
        xi = int(x)
        return pd.Timestamp.utcfromtimestamp(xi).tz_localize("UTC")
    except Exception:
        pass
    return pd.to_datetime(x, utc=True, errors="coerce")

def _clean_text(s: str) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()

def fetch_news_yf(ticker: str, start: str, end: str) -> pd.DataFrame:
    try:
        t = yf.Ticker(ticker)
        raw = getattr(t, "news", None)
    except Exception:
        raw = None

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    if isinstance(raw, list):
        for item in raw:
            ts = _norm_ts_utc(item.get("providerPublishTime"))
            if not isinstance(ts, pd.Timestamp) or ts is pd.NaT:
                continue
            title = item.get("title") or ""
            url = item.get("link") or item.get("url") or ""
            summary = item.get("summary") or ""
            rows.append((ts, title, url, summary))

    if not rows:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    df = pd.DataFrame(rows, columns=["ts", "title", "url", "text"])
    df["ticker"] = ticker
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    df = df[(df["ts"] >= s) & (df["ts"] <= e)]
    df["title"] = df["title"].map(_clean_text)
    df["text"] = df["text"].map(_clean_text)
    df = df.drop_duplicates(["title", "url"])
    return df[["ticker", "ts", "title", "url", "text"]].reset_index(drop=True)

def fetch_news_google(ticker: str, start: str, end: str, company: str | None = None) -> pd.DataFrame:
    q = f'"{ticker}"'
    if company:
        q += f' OR "{company}"'
    url = f"https://news.google.com/rss/search?q={q}+when:14d&hl=en-US&gl=US&ceid=US:en"
    feed = feedparser.parse(url)

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for entry in getattr(feed, "entries", []):
        published = getattr(entry, "published", None) or getattr(entry, "updated", None)
        ts = _norm_ts_utc(published)
        if not isinstance(ts, pd.Timestamp) or ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(getattr(entry, "summary", "") or getattr(entry, "description", ""))
        rows.append((ts, title, link, summary))

    if not rows:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    df = pd.DataFrame(rows, columns=["ts", "title", "url", "text"])
    df["ticker"] = ticker
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    df = df[(df["ts"] >= s) & (df["ts"] <= e)]
    df = df.drop_duplicates(["title", "url"])
    return df[["ticker", "ts", "title", "url", "text"]].reset_index(drop=True)

def fetch_news(ticker: str, start: str, end: str, company: str | None = None) -> pd.DataFrame:
    """
    Combine YF + Google RSS with fallback, normalized schema.
    Returns columns: ticker, ts, title, url, text (ts is tz-aware UTC)
    """
    a = fetch_news_yf(ticker, start, end)
    b = fetch_news_google(ticker, start, end, company=company)

    parts = [x for x in (a, b) if x is not None and not x.empty]
    if not parts:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    df = pd.concat(parts, ignore_index=True)

    # Final clean & de-dupe
    df["title"] = df["title"].map(_clean_text)
    df["text"]  = df["text"].map(_clean_text)
    df = df.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    return df
