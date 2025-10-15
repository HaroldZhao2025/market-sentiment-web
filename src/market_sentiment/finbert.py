# src/market_sentiment/news.py
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import List, Tuple
import re

import pandas as pd

# yfinance is allowed (free)
import yfinance as yf

# Free RSS fallback (no key)
import feedparser


def _norm_ts_utc(x) -> pd.Timestamp:
    """
    Normalize to tz-aware UTC Timestamp.
    Accepts epoch seconds or date-like strings.
    """
    if pd.isna(x):
        return pd.NaT
    # epoch
    try:
        xi = int(x)
        return pd.Timestamp.utcfromtimestamp(xi).tz_localize("UTC")
    except Exception:
        pass
    # anything else we let pandas parse
    ts = pd.to_datetime(x, utc=True, errors="coerce")
    return ts


def _clean_text(s: str) -> str:
    if s is None:
        return ""
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s


def fetch_news_yf(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Yahoo Finance news via yfinance (free).
    Returns columns: ticker, ts (UTC), title, url, text
    """
    try:
        t = yf.Ticker(ticker)
        raw = getattr(t, "news", None)
    except Exception:
        raw = None

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    if isinstance(raw, list):
        for item in raw:
            ts = _norm_ts_utc(item.get("providerPublishTime"))
            if ts is pd.NaT:
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
    df = df[["ticker", "ts", "title", "url", "text"]].reset_index(drop=True)
    return df


def fetch_news_google(ticker: str, start: str, end: str, company: str | None = None) -> pd.DataFrame:
    """
    Free Google News RSS fallback (no keys).
    Query includes ticker and optional company name.
    Returns columns: ticker, ts (UTC), title, url, text
    """
    q = f'"{ticker}"'
    if company:
        q += f' OR "{company}"'
    # 2-week window helps relevancy; we still filter by [start, end]
    url = f"https://news.google.com/rss/search?q={q}+when:14d&hl=en-US&gl=US&ceid=US:en"
    feed = feedparser.parse(url)

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for entry in getattr(feed, "entries", []):
        published = getattr(entry, "published", None) or getattr(entry, "updated", None)
        ts = _norm_ts_utc(published)
        if ts is pd.NaT:
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
    df = df[["ticker", "ts", "title", "url", "text"]].reset_index(drop=True)
    return df


def fetch_news(ticker: str, start: str, end: str, company: str | None = None) -> pd.DataFrame:
    """
    Combine YF + Google RSS with fallback, normalized schema.
    """
    a = fetch_news_yf(ticker, start, end)
    b = fetch_news_google(ticker, start, end, company=company)
    if a.empty and b.empty:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    df = pd.concat([a, b], ignore_index=True)
    # Final clean & de-dupe
    df["title"] = df["title"].map(_clean_text)
    df["text"] = df["text"].map(_clean_text)
    df = df.drop_duplicates(["title", "url"])
    df = df.sort_values("ts").reset_index(drop=True)
    return df
