from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import List, Tuple, Optional

import pandas as pd

# pip dist: finnhub-python ; import name: finnhub
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


def _norm_ts_epoch_to_utc(x) -> pd.Timestamp:
    """Finnhub 'datetime' is seconds since epoch (UTC)."""
    try:
        xi = int(x)
    except Exception:
        return pd.NaT
    try:
        return pd.Timestamp.utcfromtimestamp(xi).tz_localize("UTC")
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


def _get_token() -> Optional[str]:
    return (
        os.getenv("FINNHUB_TOKEN")
        or os.getenv("FINNHUB_API_KEY")
        or os.getenv("FINNHUB_KEY")
    )


def _cache_path(cache_dir: str | Path, ticker: str, day: str) -> Path:
    p = Path(cache_dir) / "finnhub" / ticker.upper() / f"{day}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _read_cache(cache_dir: str | Path, ticker: str, day: str) -> list:
    f = _cache_path(cache_dir, ticker, day)
    if f.exists() and f.stat().st_size > 0:
        try:
            return json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []


def _write_cache(cache_dir: str | Path, ticker: str, day: str, arr: list) -> None:
    f = _cache_path(cache_dir, ticker, day)
    try:
        f.write_text(json.dumps(arr, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def fetch_finnhub_daily(
    ticker: str,
    start: str,
    end: str,
    *,
    rps: int = 1,                 # Finnhub free: <= 30 req/sec. We stay conservative.
    max_wait_sec: int = 600,      # upper bound for exponential backoff on 429
    cache_dir: str | Path = "data/news_cache",
    verbose: bool = False,
) -> pd.DataFrame:
    """
    EXACT usage per your requirement (one API call per day):

        finnhub_client = finnhub.Client(api_key="...")
        finnhub_client.company_news('AAPL', _from="YYYY-MM-DD", to="YYYY-MM-DD")

    We do that for every day between start..end (UTC), with:
      • local per-day JSON cache
      • rate limiting (rps)
      • 429 exponential backoff (capped by max_wait_sec)
    """
    token = _get_token()
    if finnhub is None or not token:
        if verbose:
            print("[finnhub] no SDK or token; returning empty frame")
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    try:
        client = finnhub.Client(api_key=token)
    except Exception as e:
        if verbose:
            print(f"[finnhub] client init error: {e}")
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    rps = max(1, min(30, int(rps)))
    min_gap = 1.0 / float(rps)

    s = pd.Timestamp(start, tz="UTC").normalize()
    e = pd.Timestamp(end, tz="UTC").normalize()
    days = list(pd.date_range(s, e, freq="D", tz="UTC"))

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    last_call = 0.0

    for dts in days:
        day = dts.date().isoformat()

        # 1) try cache first
        cached = _read_cache(cache_dir, ticker, day)
        if cached:
            if verbose:
                print(f"[finnhub] cache hit {ticker} {day}: {len(cached)}")
            arr = cached
        else:
            # 2) call API with rate limit + backoff
            #    ensure rps by spacing calls
            now = time.time()
            wait = max(0.0, (last_call + min_gap) - now)
            if wait > 0:
                time.sleep(wait)
            last_call = time.time()

            backoff = 2.0
            total_wait = 0.0
            while True:
                try:
                    arr = client.company_news(ticker, _from=day, to=day) or []
                    _write_cache(cache_dir, ticker, day, arr)
                    if verbose:
                        print(f"[finnhub] fetched {ticker} {day}: {len(arr)}")
                    break
                except Exception as e:
                    msg = str(e)
                    if "status_code: 429" in msg or "API limit reached" in msg:
                        # exponential backoff but don't blow CI minutes
                        if total_wait >= max_wait_sec:
                            if verbose:
                                print(f"[finnhub] 429 giving up for {day} after {total_wait:.0f}s; caching empty")
                            _write_cache(cache_dir, ticker, day, [])
                            arr = []
                            break
                        sleep_for = min(backoff, max_wait_sec - total_wait)
                        if verbose:
                            print(f"[finnhub] 429 on {day}; sleeping {sleep_for:.1f}s")
                        time.sleep(sleep_for)
                        total_wait += sleep_for
                        backoff = min(backoff * 2.0, 60.0)
                        continue
                    else:
                        if verbose:
                            print(f"[finnhub] error on {day}: {e}; caching empty")
                        _write_cache(cache_dir, ticker, day, [])
                        arr = []
                        break

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
