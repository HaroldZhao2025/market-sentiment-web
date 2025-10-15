from __future__ import annotations
import os
import requests
import pandas as pd
import yfinance as yf


def _to_eastern_from_epoch_seconds_or_str(v) -> pd.Timestamp:
    """
    Robustly parse a timestamp that may be epoch-seconds or an ISO string.
    Fallback to 'now' if missing/invalid. Always return tz-aware America/New_York.
    """
    # Try epoch-seconds first (most common in yfinance)
    ts = pd.to_datetime(v, unit="s", errors="coerce", utc=True)
    if pd.isna(ts):
        # Try generic parse (ISO8601 string)
        ts = pd.to_datetime(v, errors="coerce", utc=True)
    if pd.isna(ts):
        # Final fallback: now (UTC) then convert
        ts = pd.Timestamp.now(tz="UTC")
    return ts.tz_convert("America/New_York")


def news_yfinance(ticker: str) -> pd.DataFrame:
    """
    Fetch news via Yahoo Finance. Fields vary; timestamps can be missing.
    We robustly coerce timestamp and fill with 'now' if needed.
    """
    try:
        items = yf.Ticker(ticker).news or []
    except Exception:
        items = []

    rows = []
    for it in items:
        ts_raw = it.get("providerPublishTime")  # epoch seconds
        ts = _to_eastern_from_epoch_seconds_or_str(ts_raw)

        src = (it.get("publisher") or "unknown").strip()
        title = (it.get("title") or "").strip()
        url = (it.get("link") or "").strip()

        rows.append((ticker, ts, src, title, url))

    return pd.DataFrame(rows, columns=["ticker", "ts", "source", "title", "url"])


def news_newsapi(ticker: str, api_key: str, start: str, end: str) -> pd.DataFrame:
    """
    Fetch news via NewsAPI (if NEWS_API_KEY is set). Timestamps are ISO strings.
    We coerce and fallback similarly to yfinance.
    """
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": ticker,
        "from": start,
        "to": end,
        "language": "en",
        "pageSize": 100,
        "sortBy": "publishedAt",
        "apiKey": api_key,
    }

    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        arts = r.json().get("articles", [])
    except Exception:
        arts = []

    rows = []
    for a in arts:
        ts = _to_eastern_from_epoch_seconds_or_str(a.get("publishedAt"))
        src = ((a.get("source") or {}).get("name") or "unknown").strip()
        # Combine title + description (like your original approach)
        title = f"{a.get('title') or ''} {a.get('description') or ''}".strip()
        url = (a.get("url") or "").strip()
        rows.append((ticker, ts, src, title, url))

    return pd.DataFrame(rows, columns=["ticker", "ts", "source", "title", "url"])


def fetch_news(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Try NewsAPI (if key is present), else fall back to yfinance.
    """
    key = os.environ.get("NEWS_API_KEY")
    if key:
        try:
            df = news_newsapi(ticker, key, start, end)
            if not df.empty:
                return df
        except Exception:
            pass
    return news_yfinance(ticker)
