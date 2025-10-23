# src/market_sentiment/news_finnhub_daily.py
from __future__ import annotations

import os
import time
import random
from typing import List, Tuple, Optional

import pandas as pd

# pip name: finnhub-python ; import name: finnhub
try:
    import finnhub
    try:
        from finnhub.exceptions import FinnhubAPIException  # present in current SDKs
    except Exception:  # fallback: not all client versions expose this
        class FinnhubAPIException(Exception):
            pass
except Exception:
    finnhub = None
    class FinnhubAPIException(Exception):
        pass


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
    """
    Simple token-bucket/spacing limiter:
      - rps: max requests per second (soft cap)
      - jitter: random micro jitter to avoid thundering herd
    """
    def __init__(self, rps: float = 5.0, jitter: float = 0.10):
        self.min_gap = 1.0 / max(1.0, float(rps))
        self.last = 0.0
        self.jitter = float(jitter)

    def sleep(self):
        now = time.time()
        # target next slot + small jitter
        target = self.last + self.min_gap + random.random() * self.jitter * self.min_gap
        wait = max(0.0, target - now)
        if wait > 0:
            time.sleep(wait)
        self.last = time.time()


def _extract_retry_after_sec(e: Exception, default: int = 60) -> int:
    """Try to read Retry-After or estimate a safe backoff window."""
    # finnhub SDK often provides HTTPResponse on e.response (not guaranteed).
    try:
        resp = getattr(e, "response", None)
        if resp is not None:
            heads = getattr(resp, "headers", {}) or {}
            ra = heads.get("Retry-After") or heads.get("retry-after")
            if ra:
                try:
                    return max(default, int(float(ra)))
                except Exception:
                    pass
    except Exception:
        pass
    # Parse common text like 'Remaining Limit: 0' (not reliable, but helpful)
    try:
        msg = str(e)
        if "Remaining Limit: 0" in msg:
            # be conservative: 90 seconds
            return max(default, 90)
    except Exception:
        pass
    # Default fallback
    return default


def fetch_finnhub_daily(
    ticker: str,
    start: str,
    end: str,
    *,
    rps: int = 5,                 # ≤ 30; keep conservative for CI stability
    on_429: str = "skip",         # "skip" -> move to next day; "wait" -> sleep until reset then retry once
    max_days: Optional[int] = None,
    verbose: bool = False,
) -> pd.DataFrame:
    """
    EXACT Finnhub usage (one request per day):

        client = finnhub.Client(api_key=TOKEN)
        client.company_news('AAPL', _from='YYYY-MM-DD', to='YYYY-MM-DD')

    This iterates every UTC day from start..end (inclusive), rate-limiting requests.
    Handles 429 either by skipping the day or waiting and retrying once.

    Returns a frame with columns: [ticker, ts (UTC), title, url, text].
    """
    token = (
        os.getenv("FINNHUB_TOKEN")
        or os.getenv("FINNHUB_API_KEY")
        or os.getenv("FINNHUB_KEY")
    )
    if finnhub is None or not token:
        if verbose:
            print(f"[finnhub] SDK not available or token missing; returning empty for {ticker}")
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    try:
        client = finnhub.Client(api_key=token)
    except Exception as e:
        if verbose:
            print(f"[finnhub] init error for {ticker}: {e}")
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    rps = max(1, min(30, int(rps)))
    limiter = _RateLimiter(rps=rps)

    s = pd.Timestamp(start, tz="UTC").normalize()
    e = pd.Timestamp(end, tz="UTC").normalize()
    days = list(pd.date_range(s, e, freq="D", tz="UTC"))
    if max_days is not None and len(days) > max_days:
        days = days[:max_days]

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []

    for d in days:
        day = d.date().isoformat()

        limiter.sleep()  # soft spacing

        if verbose:
            print(f"[finnhub] {ticker} day={day} …", end="", flush=True)

        try:
            arr = client.company_news(ticker, _from=day, to=day) or []
        except FinnhubAPIException as ex:
            # Rate limit or other API faults
            if "429" in str(ex) or "limit" in str(ex).lower():
                if on_429 == "wait":
                    backoff = _extract_retry_after_sec(ex, default=60)
                    if verbose:
                        print(f" 429 -> waiting {backoff}s then retry", flush=True)
                    time.sleep(backoff)
                    # retry once
                    try:
                        arr = client.company_news(ticker, _from=day, to=day) or []
                    except Exception as ex2:
                        if verbose:
                            print(f" retry failed ({ex2}); skipping")
                        arr = []
                else:
                    if verbose:
                        print(" 429 -> skip")
                    arr = []
            else:
                if verbose:
                    print(f" ERROR {ex}; skip")
                arr = []
        except Exception as e2:
            if verbose:
                print(f" ERROR {e2}; skip")
            arr = []

        if verbose and not isinstance(arr, list):
            # safety: some SDK versions could return non-list on error
            print(" unexpected response type; coerced to []")
            arr = []

        cnt = 0
        for it in arr:
            ts = _norm_ts_utc_epoch(it.get("datetime"))
            if pd.isna(ts):
                continue
            title = _clean_text(it.get("headline") or "")
            if not title:
                continue
            url = it.get("url") or ""
            text = _clean_text(it.get("summary") or title)
            rows.append((ts, title, url, text))
            cnt += 1

        if verbose:
            print(f" {cnt} items")

    return _mk_df(rows, ticker)
