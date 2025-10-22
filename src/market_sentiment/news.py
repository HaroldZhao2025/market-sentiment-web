# src/market_sentiment/news.py
from __future__ import annotations

import os
import time
from typing import Callable, List, Optional, Tuple

import pandas as pd
import yfinance as yf

# Optional dependency (follow Finnhub docs)
try:
    import finnhub  # pip install finnhub-python
except Exception:
    finnhub = None


# ------------------------
# Helpers
# ------------------------

def _clean_text(x) -> str:
    try:
        s = str(x)
    except Exception:
        return ""
    return " ".join(s.split())


def _norm_ts(x) -> pd.Timestamp:
    """Accept epoch seconds/ms or ISO strings -> tz-aware UTC Timestamp."""
    if x is None:
        return pd.NaT
    # epoch seconds / ms
    try:
        xi = int(x)
        if xi > 10_000_000_000:
            xi = xi / 1000.0
        return pd.Timestamp.utcfromtimestamp(float(xi)).tz_localize("UTC")
    except Exception:
        pass
    # generic parse
    try:
        return pd.to_datetime(x, utc=True, errors="coerce")
    except Exception:
        return pd.NaT


def _mk_df(
    rows: List[Tuple[pd.Timestamp, str, str, str, str]],
    ticker: str,
    keep_source: bool = False,
) -> pd.DataFrame:
    """
    rows: (ts, title, url, text, src)
    """
    if not rows:
        cols = ["ticker", "ts", "title", "url", "text"] + (["src"] if keep_source else [])
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(rows, columns=["ts", "title", "url", "text", "src"])
    df["ticker"] = ticker
    df = df.dropna(subset=["ts"])
    df["title"] = df["title"].map(_clean_text)
    df["text"] = df["text"].map(_clean_text)
    df["url"] = df["url"].fillna("")

    # Deduplicate on (title,url)
    df = df.sort_values("ts").drop_duplicates(["title", "url"]).reset_index(drop=True)

    cols = ["ticker", "ts", "title", "url", "text"]
    if keep_source:
        cols.append("src")
    return df[cols]


def _window_filter(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    return df[(df["ts"] >= s) & (df["ts"] <= e)].copy()


# ------------------------
# Providers
# ------------------------

def _prov_finnhub_daily(
    ticker: str,
    start: str,
    end: str,
    *,
    rps_env: str = "FINNHUB_RPS",
    keep_source: bool = False,
) -> pd.DataFrame:
    """
    EXACT Finnhub usage per docs:
        import finnhub
        client = finnhub.Client(api_key="...")
        client.company_news('AAPL', _from="YYYY-MM-DD", to="YYYY-MM-DD")
    We call this ONCE PER DAY across the full range to guarantee daily coverage.
    Rate limited by FINNHUB_RPS (default 10; hard-capped at 30).
    """
    token = (
        os.getenv("FINNHUB_TOKEN")
        or os.getenv("FINNHUB_API_KEY")
        or os.getenv("FINNHUB_KEY")
    )
    if finnhub is None or not token:
        cols = ["ticker", "ts", "title", "url", "text"] + (["src"] if keep_source else [])
        return pd.DataFrame(columns=cols)

    try:
        client = finnhub.Client(api_key=token)
    except Exception:
        cols = ["ticker", "ts", "title", "url", "text"] + (["src"] if keep_source else [])
        return pd.DataFrame(columns=cols)

    # Throttle (requests per second)
    try:
        rps = int(os.getenv(rps_env, "10"))
    except Exception:
        rps = 10
    rps = max(1, min(30, rps))
    min_gap = 1.0 / float(rps)

    s = pd.Timestamp(start, tz="UTC").normalize()
    e = pd.Timestamp(end, tz="UTC").normalize()
    days = list(pd.date_range(s, e, freq="D", tz="UTC"))

    rows: List[Tuple[pd.Timestamp, str, str, str, str]] = []
    last = 0.0

    for d in days:
        day_str = d.date().isoformat()  # 'YYYY-MM-DD'
        # rate-limit
        now = time.time()
        delay = (last + min_gap) - now
        if delay > 0:
            time.sleep(delay)
        last = time.time()

        arr = None
        for attempt in range(3):
            try:
                arr = client.company_news(ticker, _from=day_str, to=day_str) or []
                break
            except Exception:
                time.sleep(0.4 * (attempt + 1))
        if arr is None:
            arr = []

        for it in arr:
            ts = _norm_ts(it.get("datetime"))
            if pd.isna(ts):
                continue
            title = _clean_text(it.get("headline") or "")
            if not title:
                continue
            url = it.get("url") or ""
            text = _clean_text(it.get("summary") or title)
            rows.append((ts, title, url, text, "finnhub"))

    return _window_filter(_mk_df(rows, ticker, keep_source), start, end)


def _prov_yfinance_all(
    ticker: str,
    start: str,
    end: str,
    *,
    count: int = 240,
    keep_source: bool = False,
) -> pd.DataFrame:
    """
    Yahoo Finance via yfinance:
        t = yf.Ticker("MSFT")
        items = t.get_news(count=240, tab="all")
    Fallback to .news if get_news unavailable.
    """
    cnt = max(1, min(int(count or 240), 240))
    rows: List[Tuple[pd.Timestamp, str, str, str, str]] = []
    try:
        t = yf.Ticker(ticker)
        if hasattr(t, "get_news"):
            items = t.get_news(count=cnt, tab="all") or []
        else:
            items = getattr(t, "news", None) or []
    except Exception:
        items = []

    for it in items:
        content = it.get("content") if isinstance(it, dict) else None

        # Best-effort time extraction
        ts = _norm_ts(
            (it.get("providerPublishTime") if isinstance(it, dict) else None)
            or (it.get("provider_publish_time") if isinstance(it, dict) else None)
            or (content or {}).get("displayTime")
            or (content or {}).get("published")
            or (content or {}).get("pubDate")
        )
        if pd.isna(ts):
            continue

        title = _clean_text((content or {}).get("title") or it.get("title") or "")
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
        rows.append((ts, title, link, text, "yfinance"))

    return _window_filter(_mk_df(rows, ticker, keep_source), start, end)


# Public types/registry (keep minimal and exact)
Provider = Callable[..., pd.DataFrame]
PROVIDERS: List[Provider] = []  # not used by default path; we fetch explicitly


def fetch_news(
    ticker: str,
    start: str,
    end: str,
    *,
    finnhub_rps_env: str = "FINNHUB_RPS",
    yfinance_count: int = 240,
    keep_source: bool = False,
) -> pd.DataFrame:
    """
    Merge Finnhub (day-by-day) + yfinance(count=240) for full coverage, de-duped.
    """
    df_fh = _prov_finnhub_daily(ticker, start, end, rps_env=finnhub_rps_env, keep_source=keep_source)
    df_yf = _prov_yfinance_all(ticker, start, end, count=yfinance_count, keep_source=keep_source)

    frames = [df for df in (df_fh, df_yf) if df is not None and not df.empty]
    if not frames:
        cols = ["ticker", "ts", "title", "url", "text"] + (["src"] if keep_source else [])
        return pd.DataFrame(columns=cols)

    out = pd.concat(frames, ignore_index=True)
    out["url"] = out["url"].fillna("")
    out = (
        _window_filter(out, start, end)
        .drop_duplicates(["title", "url"])
        .sort_values("ts")
        .reset_index(drop=True)
    )
    return out
