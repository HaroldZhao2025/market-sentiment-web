# src/market_sentiment/news_yfinance.py
from __future__ import annotations

from typing import List, Tuple

import pandas as pd
import yfinance as yf


def _clean_text(x) -> str:
    try:
        s = str(x)
    except Exception:
        return ""
    return " ".join(s.split())


def _norm_ts_any(x) -> pd.Timestamp:
    """
    yfinance timestamps vary: providerPublishTime (epoch), content.pubDate (ISO), etc.
    """
    if x is None:
        return pd.NaT
    # epoch seconds or ms
    try:
        xi = int(x)
        if xi > 10_000_000_000:
            xi = xi / 1000.0
        return pd.Timestamp.utcfromtimestamp(float(xi)).tz_localize("UTC")
    except Exception:
        pass
    # ISO 8601
    try:
        return pd.to_datetime(x, utc=True, errors="coerce")
    except Exception:
        return pd.NaT


def _mk_df(rows: List[Tuple[pd.Timestamp, str, str, str]], ticker: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    df = pd.DataFrame(rows, columns=["ts", "title", "url", "text"])
    df["ticker"] = ticker
    df = df.dropna(subset=["ts"])
    df["title"] = df["title"].map(_clean_text)
    df["text"] = df["text"].map(_clean_text)
    df["url"] = df["url"].fillna("")
    df = df.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    return df[["ticker", "ts", "title", "url", "text"]]


def fetch_yfinance_recent(
    ticker: str,
    start: str,
    end: str,
    *,
    count: int = 240,          # EXACT requirement
    tab: str = "all",          # "news" | "press releases" | "all"
) -> pd.DataFrame:
    """
    EXACT yfinance usage:

        t = yf.Ticker("MSFT")
        items = t.get_news(count=240, tab="all")

    We still filter to [start, end] (UTC).
    """
    rows: List[Tuple[pd.Timestamp, str, str, str]] = []

    try:
        t = yf.Ticker(ticker)
        if hasattr(t, "get_news"):
            items = t.get_news(count=int(count), tab=tab) or []
        else:
            # fallback; older yfinance only exposes ~10 via .news
            items = getattr(t, "news", None) or []
    except Exception:
        items = []

    for it in items:
        content = it.get("content") if isinstance(it, dict) else None

        ts = _norm_ts_any(
            (it.get("providerPublishTime") if isinstance(it, dict) else None)
            or (it.get("provider_publish_time") if isinstance(it, dict) else None)
            or (it.get("published_at") if isinstance(it, dict) else None)
            or (content or {}).get("pubDate")
            or (content or {}).get("displayTime")
            or (content or {}).get("published")
        )
        if pd.isna(ts):
            continue

        title = _clean_text(
            (content or {}).get("title")
            or it.get("title")
            or ""
        )
        if not title:
            continue

        link = (
            ((content or {}).get("canonicalUrl") or {}).get("url")
            or ((content or {}).get("clickThroughUrl") or {}).get("url")
            or it.get("link")
            or it.get("url")
            or ""
        )
        text = _clean_text(
            (content or {}).get("summary")
            or (content or {}).get("description")
            or it.get("summary")
            or title
        )
        rows.append((ts, title, link, text))

    df = _mk_df(rows, ticker)

    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    return df[(df["ts"] >= s) & (df["ts"] <= e)]
