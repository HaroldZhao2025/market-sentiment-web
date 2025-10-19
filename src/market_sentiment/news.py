# src/market_sentiment/news.py
from __future__ import annotations

import calendar
import re
import time
from typing import Callable, List, Optional, Tuple

import pandas as pd
import feedparser
import yfinance as yf

# Optional HuggingFace dataset fallback (full-year+ coverage).
# If you don't want it, you can disable by setting USE_HF_DATASET=0.
try:
    import os
    USE_HF_DATASET = os.environ.get("USE_HF_DATASET", "1") != "0"
    if USE_HF_DATASET:
        from huggingface_hub import hf_hub_download
except Exception:
    USE_HF_DATASET = False


# ------------------------
# Utilities
# ------------------------

def _clean_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _norm_ts_utc(x) -> pd.Timestamp:
    """
    Normalize many forms to tz-aware UTC Timestamp.
    Returns pd.NaT on failure or if an array-like sneaks in.
    """
    if x is None:
        return pd.NaT

    # Reject array-like (these cause "truth value is ambiguous" paths)
    if isinstance(x, (list, tuple, set, dict, pd.Series, pd.DatetimeIndex)):
        return pd.NaT

    # struct_time (common in RSS)
    if hasattr(x, "tm_year"):
        try:
            sec = calendar.timegm(x)  # treat as UTC
            return pd.Timestamp.utcfromtimestamp(sec).tz_localize("UTC")
        except Exception:
            return pd.NaT

    # try epoch (int or str int)
    try:
        xi = int(x)
        if xi > 10_000_000_000:  # milliseconds
            xi = xi / 1000.0
        return pd.Timestamp.utcfromtimestamp(xi).tz_localize("UTC")
    except Exception:
        pass

    # generic parse (UTC)
    try:
        ts = pd.to_datetime(x, utc=True, errors="coerce")
        # Force scalar Timestamp output; if array slipped through, bail
        if isinstance(ts, (pd.Series, pd.DatetimeIndex)):
            return pd.NaT
        return ts
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
    df = df.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    return df[["ticker", "ts", "title", "url", "text"]]


def _window_filter(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if df.empty:
        return df
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    return df[(df["ts"] >= s) & (df["ts"] <= e)]


# ------------------------
# Providers (free)
# ------------------------

def _prov_yfinance(
    ticker: str, start: str, end: str, company: str | None = None, limit: int = 200
) -> pd.DataFrame:
    """
    yfinance .news (no key). Often 10 items, so treat as an additive source.
    """
    try:
        t = yf.Ticker(ticker)
        raw = getattr(t, "news", None)
    except Exception:
        raw = None

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    if isinstance(raw, list):
        for item in raw[:limit]:
            ts = _norm_ts_utc(item.get("providerPublishTime"))
            if ts is pd.NaT:
                continue
            title = item.get("title") or ""
            url = item.get("link") or item.get("url") or ""
            summary = item.get("summary") or ""
            rows.append((ts, title, url, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_google_rss(
    ticker: str, start: str, end: str, company: str | None = None, limit: int = 300
) -> pd.DataFrame:
    """
    Google News RSS (no key). IMPORTANT: 'when:' is relative to now.
    We set when:365d to fetch roughly last year (best effort).
    """
    from urllib.parse import quote_plus

    # Use symbol and company name to broaden recall
    terms = [f'"{ticker}"']
    if company:
        terms.append(f'"{company}"')
    q = " OR ".join(terms)

    # 365 days coverage relative to now
    q_enc = quote_plus(f"{q} when:365d")
    url = f"https://news.google.com/rss/search?q={q_enc}&hl=en-US&gl=US&ceid=US:en"

    feed = feedparser.parse(url)
    ents = getattr(feed, "entries", []) or []

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for i, entry in enumerate(ents):
        if i >= limit:
            break
        # Prefer struct_time → scalar Timestamp
        ts = (
            _norm_ts_utc(getattr(entry, "published_parsed", None))
            or _norm_ts_utc(getattr(entry, "updated_parsed", None))
            or _norm_ts_utc(getattr(entry, "published", None))
            or _norm_ts_utc(getattr(entry, "updated", None))
        )
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(
            getattr(entry, "summary", "") or getattr(entry, "description", "")
        )
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_yahoo_rss(
    ticker: str, start: str, end: str, company: str | None = None, limit: int = 300
) -> pd.DataFrame:
    """
    Yahoo Finance RSS (no key). 'count' can be large but returns vary.
    """
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&lang=en-US&region=US&count={limit}"
    feed = feedparser.parse(url)
    ents = getattr(feed, "entries", []) or []

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for entry in ents:
        ts = (
            _norm_ts_utc(getattr(entry, "published_parsed", None))
            or _norm_ts_utc(getattr(entry, "updated_parsed", None))
            or _norm_ts_utc(getattr(entry, "published", None))
            or _norm_ts_utc(getattr(entry, "updated", None))
        )
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(
            getattr(entry, "summary", "") or getattr(entry, "description", "")
        )
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_nasdaq_rss(
    ticker: str, start: str, end: str, company: str | None = None, limit: int = 200
) -> pd.DataFrame:
    """
    Nasdaq RSS (no key). Some runs reject connections—it's OK, we catch and continue.
    """
    url = f"https://www.nasdaq.com/feed/rssoutbound?symbol={ticker}"
    feed = feedparser.parse(url)
    ents = getattr(feed, "entries", []) or []

    rows: List[Tuple[pd.Timestamp, str, str, str]] = []
    for i, entry in enumerate(ents):
        if i >= limit:
            break
        ts = (
            _norm_ts_utc(getattr(entry, "published_parsed", None))
            or _norm_ts_utc(getattr(entry, "updated_parsed", None))
            or _norm_ts_utc(getattr(entry, "published", None))
            or _norm_ts_utc(getattr(entry, "updated", None))
        )
        if ts is pd.NaT:
            continue
        title = _clean_text(getattr(entry, "title", ""))
        link = getattr(entry, "link", "") or ""
        summary = _clean_text(
            getattr(entry, "summary", "") or getattr(entry, "description", "")
        )
        rows.append((ts, title, link, summary))

    df = _mk_df(rows, ticker)
    return _window_filter(df, start, end)


def _prov_hf_fnspid(
    ticker: str, start: str, end: str, company: str | None = None, limit: int = 5000
) -> pd.DataFrame:
    """
    HuggingFace dataset fallback to get *wide* coverage (year+).
    Repo: Zihan1004/FNSPID
    File: Stock_news/nasdaq_exteral_data.csv
    """
    if not USE_HF_DATASET:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    try:
        local_path = hf_hub_download(
            repo_id="Zihan1004/FNSPID",
            filename="Stock_news/nasdaq_exteral_data.csv",
            repo_type="dataset",
            local_dir=os.environ.get("RUNNER_TEMP", None) or os.getcwd(),
        )
    except Exception:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    try:
        # Big file—let pandas handle it efficiently
        df = pd.read_csv(local_path)
    except Exception:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    # Flexible schema mapping
    def col(*names):
        for n in names:
            if n in df.columns:
                return n
        return None

    c_ticker = col("ticker", "symbol", "TICKER")
    c_time = col("time", "timestamp", "date", "published_at")
    c_title = col("title", "headline")
    c_text = col("content", "text", "summary", "body")
    c_url = col("url", "link")

    if not c_ticker or not c_time or not (c_title or c_text):
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    sdf = df[df[c_ticker].astype(str).str.upper().eq(ticker.upper())].copy()
    if sdf.empty:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    sdf["ts"] = pd.to_datetime(sdf[c_time], utc=True, errors="coerce")
    sdf = sdf.dropna(subset=["ts"]).sort_values("ts")

    title = sdf[c_title] if c_title else ""
    text = sdf[c_text] if c_text else ""
    url = sdf[c_url] if c_url else ""

    out = pd.DataFrame(
        {
            "ts": sdf["ts"],
            "title": title.astype(str) if c_title else "",
            "url": url.astype(str) if c_url else "",
            "text": text.astype(str) if c_text else "",
        }
    )
    out = out.iloc[:limit].copy()
    out["ticker"] = ticker
    out = out.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
    out = out[["ticker", "ts", "title", "url", "text"]]
    return _window_filter(out, start, end)


# ------------------------
# Public API
# ------------------------

Provider = Callable[[str, str, str, Optional[str], int], pd.DataFrame]

# Order matters: try RSS first for recency, then yfinance, then HF dataset to backfill wide history.
_PROVIDERS: List[Provider] = [
    _prov_google_rss,
    _prov_yahoo_rss,
    _prov_nasdaq_rss,
    _prov_yfinance,
    _prov_hf_fnspid,  # huge coverage, last to avoid duplicates biasing recency too much
]


def fetch_news(
    ticker: str,
    start: str,
    end: str,
    company: str | None = None,
    max_per_provider: int = 300,
    max_total: Optional[int] = None,
) -> pd.DataFrame:
    """
    Combine providers, normalize & dedupe.
    Output columns: ticker, ts (UTC), title, url, text
    """
    frames: List[pd.DataFrame] = []
    for prov in _PROVIDERS:
        try:
            df = prov(ticker, start, end, company, max_per_provider) or pd.DataFrame()
        except Exception:
            df = pd.DataFrame()
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    df = pd.concat(frames, ignore_index=True)
    df["title"] = df["title"].map(_clean_text)
    df["text"] = df["text"].map(_clean_text)
    df = df.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)

    if max_total and len(df) > max_total:
        df = df.tail(max_total).reset_index(drop=True)

    return df
