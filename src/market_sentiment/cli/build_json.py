# src/market_sentiment/cli/build_json.py
from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

import pandas as pd

from market_sentiment.aggregate import (
    add_forward_returns,
    daily_sentiment_from_rows,
    join_and_fill_daily,
    _ensure_date_dtype,
)
from market_sentiment.finbert import FinBERT  # may be unavailable on CI; handled below
from market_sentiment.news import fetch_news
from market_sentiment.prices import fetch_prices_yf
from market_sentiment.writers import write_outputs


# -------------------------------
# FinBERT helpers (backward compat)
# -------------------------------

def _score_texts(fb: FinBERT, texts: List[str], batch: int) -> List[float]:
    try:
        return fb.score(texts, batch=batch)
    except TypeError:
        try:
            return fb.score(texts, batch_size=batch)
        except TypeError:
            return fb.score(texts)


def _score_rows_inplace(fb: Optional[FinBERT], df: pd.DataFrame, text_col: str, batch: int) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", text_col, "S"])
    df = df.copy()
    if fb is None:
        df["S"] = 0.0
        return df

    texts = df[text_col].astype(str).fillna("").tolist()
    if not texts:
        df["S"] = 0.0
        return df

    try:
        scores = _score_texts(fb, texts, batch=batch)
        df["S"] = pd.to_numeric(pd.Series(scores), errors="coerce").fillna(0.0)
    except Exception:
        df["S"] = 0.0
    return df


# -------------------------------
# Prices (with gentle throttling)
# -------------------------------

def _fetch_all_prices(tickers: List[str], start: str, end: str, max_workers: int) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = []
        for t in tickers:
            futs.append(ex.submit(fetch_prices_yf, t, start, end))
            time.sleep(0.15)  # reduce burst rate-limits on yfinance
        for f in as_completed(futs):
            try:
                df = f.result()
                if df is not None and len(df) > 0:
                    rows.append(df)
            except Exception:
                pass

    if not rows:
        return pd.DataFrame(columns=["date", "ticker", "open", "close"])
    prices = pd.concat(rows, ignore_index=True)
    prices = _ensure_date_dtype(prices, "date")
    prices = add_forward_returns(prices)
    return prices


# -------------------------------
# Company lookup (best-effort)
# -------------------------------

_COMPANY_CACHE: Dict[str, Optional[str]] = {}

def _company_name(ticker: str) -> Optional[str]:
    """Lightweight best-effort lookup via yfinance; cached and throttled."""
    if ticker in _COMPANY_CACHE:
        return _COMPANY_CACHE[ticker]
    name: Optional[str] = None
    try:
        import yfinance as yf
        y = yf.Ticker(ticker)
        # Try fast_info first (may not have name), then info (slower).
        info = {}
        try:
            info = y.get_info() if hasattr(y, "get_info") else (y.info or {})
        except Exception:
            try:
                info = y.info or {}
            except Exception:
                info = {}
        name = info.get("longName") or info.get("shortName") or None
    except Exception:
        name = None
    _COMPANY_CACHE[ticker] = name
    # tiny sleep to avoid hammering yfinance in full mode
    time.sleep(0.05)
    return name


# -------------------------------
# News fetching (resilient, with company)
# -------------------------------

def _yf_news_fallback(ticker: str, start: str, end: str, limit: int = 300) -> pd.DataFrame:
    """Last-resort: pull from yfinance.Ticker(...).news and coerce to your schema."""
    try:
        import yfinance as yf
    except Exception:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    try:
        raw = getattr(yf.Ticker(ticker), "news", None) or []
    except Exception:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    rows = []
    for item in (raw[:limit] if isinstance(raw, list) else []):
        content = item.get("content") if isinstance(item, dict) else None
        ts = (
            item.get("providerPublishTime")
            or item.get("provider_publish_time")
            or item.get("published_at")
            or item.get("pubDate")
            or ((content or {}).get("published") if isinstance(content, dict) else None)
            or ((content or {}).get("pubDate") if isinstance(content, dict) else None)
        )
        ts = pd.to_datetime(ts, utc=True, errors="coerce")
        if pd.isna(ts):
            continue

        def _first(*vals) -> str:
            for v in vals:
                if v:
                    s = str(v).strip()
                    if s:
                        return s
            return ""

        title = _first(item.get("title"), (content or {}).get("title"))
        if not title:
            continue
        link = _first(item.get("link"), item.get("url"), (content or {}).get("link"), (content or {}).get("url"))
        text = _first(item.get("summary"), (content or {}).get("summary"), (content or {}).get("content"), title)
        rows.append((ts, title, link, text))

    if not rows:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

    df = pd.DataFrame(rows, columns=["ts", "title", "url", "text"])
    df["ticker"] = ticker
    df = df.dropna(subset=["ts"]).drop_duplicates(["title", "url"]).sort_values("ts")
    df = df[(df["ts"] >= pd.to_datetime(start, utc=True)) &
            (df["ts"] <= pd.to_datetime(end, utc=True) + pd.Timedelta(days=1))]
    return df[["ticker", "ts", "title", "url", "text"]]


def _fetch_news_resilient(ticker: str, start: str, end: str, company: Optional[str], tries: int = 2) -> pd.DataFrame:
    last_err = None
    for _ in range(tries):
        try:
            # IMPORTANT: pass company (matches your smoke test behavior)
            df = fetch_news(ticker, start, end, company=company, max_per_provider=300)
            if df is not None and not df.empty:
                return df
        except Exception as e:
            last_err = e
        time.sleep(0.7)

    fb = _yf_news_fallback(ticker, start, end, limit=300)
    if fb is not None and not fb.empty:
        return fb

    if last_err:
        # swallow in CI; return empty schema
        pass
    return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])


# -------------------------------
# Fallback daily sentiment (no FinBERT or all-zero S)
# -------------------------------

def _fallback_daily_from_counts(news_rows: pd.DataFrame) -> pd.DataFrame:
    if news_rows is None or news_rows.empty:
        return pd.DataFrame(columns=["date", "ticker", "S_NEWS"])

    df = news_rows.copy()
    df["date"] = pd.to_datetime(df["ts"], utc=True, errors="coerce").dt.normalize()
    df = df.dropna(subset=["date", "ticker"])
    if df.empty:
        return pd.DataFrame(columns=["date", "ticker", "S_NEWS"])

    cnt = (df.groupby(["ticker", "date"], as_index=False)
           .size().rename(columns={"size": "n"}))

    out_rows = []
    for t, g in cnt.groupby("ticker"):
        g = g.sort_values("date").reset_index(drop=True)
        n = g["n"].astype(float)
        mean = float(n.mean())
        std = float(n.std(ddof=0))
        z = (n - mean) if std <= 1e-12 else (n - mean) / std
        s = (z.clip(-3, 3) / 3.0).astype(float)
        s_ma3 = s.rolling(3, min_periods=1).mean()
        out_rows.append(pd.DataFrame({"ticker": t, "date": g["date"], "S_NEWS": s_ma3}))
    if not out_rows:
        return pd.DataFrame(columns=["date", "ticker", "S_NEWS"])
    return pd.concat(out_rows, ignore_index=True)[["date", "ticker", "S_NEWS"]]


def _is_all_zero_or_missing(d_news: pd.DataFrame) -> bool:
    if d_news is None or d_news.empty:
        return True
    col = "S_NEWS" if "S_NEWS" in d_news.columns else ("S" if "S" in d_news.columns else None)
    if col is None:
        return True
    s = pd.to_numeric(d_news[col], errors="coerce").fillna(0.0)
    return float(s.abs().max()) <= 1e-12


# -------------------------------
# Summary based on written files
# -------------------------------

def _summarize_from_files(out_dir: str):
    out_dir = str(out_dir)
    try:
        tickers = json.load(open(os.path.join(out_dir, "_tickers.json")))
    except Exception:
        tickers = []
    have_files = 0
    with_news = 0
    with_nonzero_s = 0
    for t in tickers:
        f = os.path.join(out_dir, "ticker", f"{t}.json")
        if not os.path.exists(f):
            continue
        have_files += 1
        try:
            obj = json.load(open(f))
        except Exception:
            continue
        news = obj.get("news") or []
        if news:
            with_news += 1
        S = obj.get("S") or obj.get("sentiment") or []
        try:
            if any(abs(float(x or 0)) > 1e-12 for x in S):
                with_nonzero_s += 1
        except Exception:
            pass
    return tickers, have_files, with_news, with_nonzero_s


# -------------------------------
# Main
# -------------------------------

def main():
    p = argparse.ArgumentParser("Build JSON artifacts for the web app")
    p.add_argument("--universe", required=True, help="CSV with ticker column (or first column)")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--out", required=True, help="Output dir: apps/web/public/data")
    p.add_argument("--batch", type=int, default=16)
    p.add_argument("--cutoff-minutes", type=int, default=5)
    p.add_argument("--max-tickers", type=int, default=0)
    p.add_argument("--max-workers", type=int, default=8)
    a = p.parse_args()

    # Universe
    uni = pd.read_csv(a.universe)
    if "ticker" not in uni.columns:
        first = uni.columns[0]
        uni = uni.rename(columns={first: "ticker"})
    tickers: List[str] = sorted([str(x).strip().upper()
                                for x in uni["ticker"].dropna().unique().tolist()])
    if a.max_tickers and a.max_tickers > 0:
        tickers = tickers[: a.max_tickers]

    mode = f"{len(tickers)} tickers"
    print(f"Build JSON for {mode} | batch={a.batch} cutoff_min={a.cutoff_minutes} max_workers={a.max_workers}")

    # Prices
    print("Prices:")
    prices = _fetch_all_prices(tickers, a.start, a.end, max_workers=a.max_workers)
    if prices is None or prices.empty:
        raise RuntimeError("No prices downloaded.")
    print(f"  ✓ Downloaded prices for {prices['ticker'].nunique()} tickers, {len(prices)} rows.")

    # News (+ fallback sentiment)
    print("News+Earnings:")
    try:
        fb = FinBERT()
    except Exception:
        fb = None

    news_all: List[pd.DataFrame] = []
    for t in tickers:
        comp = _company_name(t)  # <-- key difference vs previous file
        n = _fetch_news_resilient(t, a.start, a.end, company=comp, tries=2)
        if n is None or n.empty:
            n = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
        n = _score_rows_inplace(fb, n, text_col="text", batch=a.batch)
        news_all.append(n)

    news_rows = pd.concat(news_all, ignore_index=True) if news_all else pd.DataFrame(
        columns=["ticker", "ts", "title", "url", "text", "S"]
    )

    # Earnings placeholder (kept schema-compatible)
    earn_rows = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])

    # Daily aggregation
    d_news = daily_sentiment_from_rows(news_rows, "news", cutoff_minutes=a.cutoff_minutes)
    d_earn = daily_sentiment_from_rows(earn_rows, "earn", cutoff_minutes=a.cutoff_minutes) \
        if not earn_rows.empty else pd.DataFrame(columns=["date", "ticker", "S_EARN"])

    # If missing/zero, fallback from counts
    if _is_all_zero_or_missing(d_news):
        d_news = _fallback_daily_from_counts(news_rows)

    if not d_news.empty:
        d_news["date"] = pd.to_datetime(d_news["date"], utc=True, errors="coerce")
    if not d_earn.empty:
        d_earn["date"] = pd.to_datetime(d_earn["date"], utc=True, errors="coerce")

    daily = join_and_fill_daily(d_news, d_earn)

    if "S" not in daily.columns:
        daily["S_NEWS"] = pd.to_numeric(daily.get("S_NEWS", 0.0), errors="coerce").fillna(0.0)
        daily["S_EARN"] = pd.to_numeric(daily.get("S_EARN", 0.0), errors="coerce").fillna(0.0)
        daily["S"] = daily["S_NEWS"] + daily["S_EARN"]

    # Panel
    panel = prices.merge(daily, on=["date", "ticker"], how="left")
    for c in ("S", "S_NEWS", "S_EARN"):
        if c not in panel.columns:
            panel[c] = 0.0
        panel[c] = pd.to_numeric(panel[c], errors="coerce").fillna(0.0)

    # Write outputs (writers handles both 3-arg and 4-arg signatures)
    write_outputs(panel, news_rows, earn_rows, a.out)

    # Summary from exported files (what the site will actually use)
    tickers_list, have_files, with_news, with_nonzero_s = _summarize_from_files(a.out)
    print("Summary:")
    print(f"  Tickers listed: {len(tickers_list)}")
    print(f"  Ticker JSON files: {have_files}")
    print(f"  Tickers with any news: {with_news}/{len(tickers_list)}")
    print(f"  Tickers with non-zero daily S: {with_nonzero_s}/{len(tickers_list)}")


if __name__ == "__main__":
    main()
