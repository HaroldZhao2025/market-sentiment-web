# src/market_sentiment/news_finnhub_daily.py
from __future__ import annotations

import os
import time
from typing import List, Tuple

import pandas as pd

# pip package name: finnhub-python ; import name: finnhub
try:
    import finnhub  # type: ignore
except Exception:
    finnhub = None


def _clean_text(x) -> str:
    try:
        s = str(x)
    except Exception:
        return ""
    return " ".join(s.split())


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


def _norm_ts_epoch_to_utc(x) -> pd.Timestamp:
    """Finnhub 'datetime' is seconds since epoch (UTC)."""
    if x is None:
        return pd.NaT
    try:
        xi = int(x)
        if xi > 10_000_000_000:  # just in case ms is ever returned
            xi = xi / 1000.0
        return pd.Timestamp.utcfromtimestamp(xi).tz_localize("UTC")
    except Exception:
        return pd.NaT


def fetch_finnhub_daily(
    ticker: str,
    start: str,
    end: str,
    *,
    rps: int = 10,             # ≤ 30 per Finnhub limits
    max_days: int | None = None,
) -> pd.DataFrame:
    """
    EXACT Finnhub usage (one request per day):

        import finnhub
        c = finnhub.Client(api_key="...")
        c.company_news('AAPL', _from="YYYY-MM-DD", to="YYYY-MM-DD")

    Iterates day-by-day from start..end, rate-limited to `rps`.
    """
    token = (
        os.getenv("FINNHUB_TOKEN")
        or os.getenv("FINNHUB_API_KEY")
        or os.getenv("FINNHUB_KEY")
    )
    if finnhub is None or not token:
        # No SDK or no token → empty DF
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    try:
        client = finnhub.Client(api_key=token)
    except Exception:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    rps = max(1, min(30, int(rps)))
    min_gap = 1.0 / float(rps)

    s = pd.Timestamp(start, tz="UTC").normalize()
    e = pd.Timestamp(end, tz="UTC").normalize()
    days = list(pd.date_range(s, e, freq="D", tz="UTC"))
    if max_days is not None and len(days) > max_days:
        days = days[:max_days]

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    last = 0.0

    for d in days:
        day = d.date().isoformat()

        # basic rate limiter
        now = time.time()
        wait = max(0.0, (last + min_gap) - now)
        if wait > 0:
            time.sleep(wait)
        last = time.time()

        try:
            arr = client.company_news(ticker, _from=day, to=day) or []
        except Exception:
            continue

        for it in arr:
            ts = _norm_ts_epoch_to_utc(it.get("datetime"))
            if pd.isna(ts):
                continue
            title = _clean_text(it.get("headline") or "")
            if not title:
                continue
            url = it.get("url") or ""
            text = _clean_text(it.get("summary") or title)
            rows.append((ts, title, url, text))

    return _mk_df(rows, ticker)
