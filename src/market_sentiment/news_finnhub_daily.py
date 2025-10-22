# src/market_sentiment/news_finnhub_daily.py
from __future__ import annotations

import os
import time
from typing import List, Tuple
import pandas as pd

try:
    import finnhub  # pip install finnhub-python
except Exception as e:
    finnhub = None


def _get_token() -> str | None:
    for k in ("FINNHUB_TOKEN", "FINNHUB_API_KEY", "FINNHUB_KEY"):
        v = os.environ.get(k)
        if v and isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _epoch_to_utc_ts(x) -> pd.Timestamp:
    try:
        x = int(x)
        # Finnhub 'datetime' is seconds (not ms)
        return pd.to_datetime(x, unit="s", utc=True)
    except Exception:
        return pd.NaT


def _mk_df(rows: List[Tuple[pd.Timestamp, str, str, str]], ticker: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    df = pd.DataFrame(rows, columns=["ts", "title", "url", "text"])
    df["ticker"] = ticker
    df = df.dropna(subset=["ts"]).drop_duplicates(["title", "url"])
    df = df.sort_values("ts").reset_index(drop=True)
    return df[["ticker", "ts", "title", "url", "text"]]


def fetch_finnhub_daily_news(
    ticker: str,
    start: str,
    end: str,
    rps: float = 20.0,           # 20 req/s < 30 req/s Finnhub cap
    max_retries: int = 3,
) -> pd.DataFrame:
    """
    EXACT daily call per your requirement:
        finnhub.Client(...).company_news(ticker, _from=YYYY-MM-DD, to=YYYY-MM-DD)

    - Walks day-by-day [start, end] inclusive
    - Global rate-limit via a fixed delay (1/rps)
    - Retries with exponential backoff (handles HTTP 429 / transient faults)
    """
    token = _get_token()
    if finnhub is None or not token:
        # Return empty, so the rest of the pipeline still works
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    client = finnhub.Client(api_key=token)

    s = pd.to_datetime(start, utc=True).normalize()
    e = pd.to_datetime(end,   utc=True).normalize()
    days = pd.date_range(s, e, freq="D", inclusive="both")

    delay = max(1.0 / max(rps, 1.0), 0.05)  # 0.05s => 20 req/s

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for day in days:
        ds = day.date().isoformat()
        de = ds  # per-day request
        # retry loop
        wait = 0.5
        for attempt in range(max_retries):
            try:
                items = client.company_news(ticker, _from=ds, to=de) or []
                for it in items:
                    ts = _epoch_to_utc_ts(it.get("datetime"))
                    title = str(it.get("headline") or "").strip()
                    url = str(it.get("url") or "").strip()
                    text = str(it.get("summary") or title).strip()
                    if not title and not text:
                        continue
                    rows.append((ts, title or text, url, text or title))
                break  # success
            except Exception:
                if attempt == max_retries - 1:
                    # give up this day; continue to next day
                    break
                time.sleep(wait)
                wait = min(wait * 2.0, 4.0)
        time.sleep(delay)

    return _mk_df(rows, ticker)
