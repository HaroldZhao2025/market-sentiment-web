# src/market_sentiment/news_finnhub_daily.py
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional

import pandas as pd

# MUST match sample usage:
#   import finnhub
#   finnhub_client = finnhub.Client(api_key="xxxx")
#   finnhub_client.company_news('AAPL', _from="2025-06-01", to="2025-06-01")
try:
    import finnhub  # type: ignore
except Exception:
    finnhub = None


def _get_token() -> Optional[str]:
    for k in ("FINNHUB_TOKEN", "FINNHUB_API_KEY", "FINNHUB_KEY"):
        v = (os.environ.get(k) or "").strip()
        if v:
            return v
    return None


def _dates_utc(start: str, end: str) -> List[str]:
    s = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    e = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    out = []
    cur = s
    while cur <= e:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out


def _clean(s) -> str:
    try:
        return " ".join(str(s).split())
    except Exception:
        return ""


def fetch_finnhub_daily_news(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Exactly the sample call, repeated per day:
        finnhub_client.company_news('AAPL', _from="YYYY-MM-DD", to="YYYY-MM-DD")
    Returns DataFrame: [ticker, ts (UTC), title, url, text]
    """
    if finnhub is None:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    token = _get_token()
    if not token:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    try:
        client = finnhub.Client(api_key=token)
    except Exception:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for day in _dates_utc(start, end):
        try:
            data = client.company_news(ticker, _from=day, to=day) or []
        except Exception:
            # Try to be resilient; continue to next day
            data = []

        for it in data:
            # Finnhub sample: datetime is epoch seconds
            ts = it.get("datetime", None)
            try:
                ts = pd.to_datetime(int(ts), unit="s", utc=True)
            except Exception:
                continue

            head = _clean(it.get("headline", ""))
            if not head:
                continue
            url = _clean(it.get("url", ""))

            text = _clean(it.get("summary", "")) or head
            rows.append((ts, head, url, text))

        # finesse rate limits
        time.sleep(0.1)

    if not rows:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    df = pd.DataFrame(rows, columns=["ts", "title", "url", "text"])
    df["ticker"] = ticker
    df = df.sort_values("ts").drop_duplicates(["title", "url"]).reset_index(drop=True)
    return df[["ticker", "ts", "title", "url", "text"]]
