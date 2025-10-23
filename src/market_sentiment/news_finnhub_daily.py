# src/market_sentiment/news_finnhub_daily.py
from __future__ import annotations

import os
import time
from typing import List, Tuple, Iterable, Optional

import pandas as pd

# pip package: finnhub-python ; import name: finnhub
try:
    import finnhub
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


def _norm_ts_utc_epoch(x) -> pd.Timestamp:
    """Finnhub 'datetime' is seconds since epoch (UTC)."""
    try:
        xi = int(x)
    except Exception:
        return pd.NaT
    try:
        return pd.Timestamp.utcfromtimestamp(xi).tz_localize("UTC")
    except Exception:
        return pd.NaT


class _RateLimiter:
    """Simple per-process RPS limiter (single thread)."""
    def __init__(self, rps: int):
        rps = max(1, min(30, int(rps)))  # Finnhub docs: ≤30 req/sec
        self._gap = 1.0 / float(rps)
        self._last = 0.0

    def wait(self):
        now = time.time()
        to_wait = max(0.0, (self._last + self._gap) - now)
        if to_wait > 0:
            time.sleep(to_wait)
        self._last = time.time()


def _date_range(s: pd.Timestamp, e: pd.Timestamp) -> Iterable[str]:
    cur = s.normalize()
    end = e.normalize()
    while cur <= end:
        yield cur.date().isoformat()
        cur += pd.Timedelta(days=1)


def fetch_finnhub_daily(
    ticker: str,
    start: str,
    end: str,
    *,
    rps: int = 1,               # default ultra-safe; you can raise to 3–5 later
    on_429: str = "wait",       # "wait" only (no skip), kept param for clarity
    verbose: bool = False,
    max_wait_sec: Optional[int] = None,
) -> pd.DataFrame:
    """
    EXACT Finnhub usage (one request per day):

        import finnhub
        c = finnhub.Client(api_key="...")
        c.company_news('AAPL', _from="YYYY-MM-DD", to="YYYY-MM-DD")

    Never skips on 429: backs off and retries (bounded by max_wait_sec).
    """
    token = (
        os.getenv("FINNHUB_TOKEN")
        or os.getenv("FINNHUB_API_KEY")
        or os.getenv("FINNHUB_KEY")
    )
    if finnhub is None or not token:
        if verbose:
            print("[finnhub] SDK missing or token not set; returning empty frame.")
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    # External override for long waits, default 15 minutes
    if max_wait_sec is None:
        try:
            max_wait_sec = int(os.getenv("FINNHUB_MAX_WAIT_SEC", "900"))
        except Exception:
            max_wait_sec = 900

    try:
        client = finnhub.Client(api_key=token)
    except Exception as e:
        if verbose:
            print(f"[finnhub] client init error: {e}")
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    limiter = _RateLimiter(rps=rps)
    s = pd.Timestamp(start, tz="UTC")
    e = pd.Timestamp(end, tz="UTC")

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []

    for day in _date_range(s, e):
        # Always enforce RPS
        limiter.wait()

        # Robust 429 handling with exponential backoff
        attempt = 0
        sleep_total = 0.0
        backoff = 2.0  # start small

        while True:
            try:
                arr = client.company_news(ticker, _from=day, to=day) or []
                break  # success
            except Exception as ex:
                msg = str(ex)
                # FinnhubAPIException(status_code: 429) ... Remaining Limit: 0
                is_429 = ("status_code: 429" in msg) or ("API limit reached" in msg)
                attempt += 1

                if not is_429:
                    # Other transient network errors: small backoff
                    wait_for = min(10.0, backoff)
                else:
                    # When ratelimited, wait longer (try to catch window reset)
                    wait_for = min(90.0, max(backoff, 60.0))

                sleep_total += wait_for
                if verbose:
                    print(f"[finnhub] day={day} error: {ex} | attempt={attempt} | sleep={wait_for:.1f}s | slept={sleep_total:.1f}/{max_wait_sec}s")

                if sleep_total >= max_wait_sec:
                    # We do NOT skip the provider entirely; we stop retrying
                    # this day to respect workflow time, but we still proceed
                    # to the next date so the whole year is attempted.
                    if verbose:
                        print(f"[finnhub] day={day} reached max_wait_sec={max_wait_sec}; continuing to next day.")
                    arr = []
                    break

                time.sleep(wait_for)
                backoff = min(backoff * 1.6, 180.0)

        # Collect for the day
        for it in arr:
            ts = _norm_ts_utc_epoch(it.get("datetime"))
            if pd.isna(ts):
                continue
            title = _clean_text(it.get("headline") or "")
            if not title:
                continue
            url = it.get("url") or ""
            text = _clean_text((it.get("summary") or "") or title)
            rows.append((ts, title, url, text))

    if verbose:
        try:
            dcount = len(pd.Series([r[0].date() for r in rows]).unique()) if rows else 0
        except Exception:
            dcount = 0
        print(f"[finnhub] DONE {ticker}: rows={len(rows)} | days={dcount}")

    return _mk_df(rows, ticker)
