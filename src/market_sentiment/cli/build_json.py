# src/market_sentiment/cli/build_json.py
from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple

import pandas as pd

from market_sentiment.aggregate import (
    add_forward_returns,
    daily_sentiment_from_rows,
    join_and_fill_daily,
    _ensure_date_dtype,
)
from market_sentiment.finbert import FinBERT  # optional — handled defensively
from market_sentiment.news import fetch_news
from market_sentiment.prices import fetch_prices_yf
from market_sentiment.writers import write_outputs


# -------------------------------
# FinBERT helpers (version-safe)
# -------------------------------

def _score_texts(fb: FinBERT, texts: List[str], batch: int) -> List[float]:
    """Handle FinBERT.score(*args) signature differences across versions."""
    try:
        return fb.score(texts, batch=batch)
    except TypeError:
        try:
            return fb.score(texts, batch_size=batch)
        except TypeError:
            return fb.score(texts)


def _score_rows_inplace(
    fb: Optional[FinBERT], df: pd.DataFrame, text_col: str, batch: int
) -> pd.DataFrame:
    """
    Adds column 'S' in place.
      • If FinBERT missing or any error -> S=0.0 (writers will build a daily fallback).
      • Works even with empty frames (returns proper schema).
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", text_col, "S"])

    out = df.copy()
    if fb is None:
        out["S"] = 0.0
        return out

    texts = out[text_col].astype(str).fillna("").tolist()
    if not texts:
        out["S"] = 0.0
        return out

    try:
        scores = _score_texts(fb, texts, batch=batch)
        out["S"] = pd.to_numeric(pd.Series(scores), errors="coerce").fillna(0.0)
    except Exception:
        out["S"] = 0.0
    return out


# -------------------------------
# Prices (gentle throttling)
# -------------------------------

def _fetch_all_prices(tickers: List[str], start: str, end: str, max_workers: int) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = []
        for t in tickers:
            futs.append(ex.submit(fetch_prices_yf, t, start, end))
            time.sleep(0.12)  # soften YF rate-limits
        for f in as_completed(futs):
            try:
                df = f.result()
                if df is not None and len(df) > 0:
                    rows.append(df)
            except Exception:
                # Continue on single-ticker failures
                pass

    if not rows:
        return pd.DataFrame(columns=["date", "ticker", "open", "close"])

    prices = pd.concat(rows, ignore_index=True)
    prices = _ensure_date_dtype(prices, "date")  # -> naive date (UTC day)
    prices = add_forward_returns(prices)
    return prices


# -------------------------------
# Utilities
# -------------------------------

def _best_effort_company(ticker: str) -> Optional[str]:
    """Try to get a readable company name for better news queries."""
    try:
        import yfinance as yf  # local import so the module isn't required for unit tests
        info = yf.Ticker(ticker).get_info() or {}
        name = info.get("longName") or info.get("shortName")
        if name and 2 <= len(name) <= 80:
            return str(name).strip()
    except Exception:
        pass
    return None


def _summarize_from_files(out_dir: str) -> Tuple[List[str], int, int, int]:
    try:
        tickers = json.load(open(f"{out_dir}/_tickers.json", "r", encoding="utf-8"))
    except Exception:
        tickers = []
    have_files = with_news = with_nonzero_s = 0
    for t in tickers:
        f = f"{out_dir}/ticker/{t}.json"
        try:
            obj = json.load(open(f, "r", encoding="utf-8"))
        except Exception:
            continue
        have_files += 1
        if obj.get("news"):
            with_news += 1
        S = obj.get("S") or obj.get("sentiment") or []
        try:
            if any(abs(float(x or 0.0)) > 1e-12 for x in S):
                with_nonzero_s += 1
        except Exception:
            pass
    return tickers, have_files, with_news, with_nonzero_s


# -------------------------------
# Main
# -------------------------------

def main():
    p = argparse.ArgumentParser("Build JSON artifacts for the web app")
    p.add_argument("--universe", required=True, help="CSV with 'ticker' column (first col accepted)")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--out", required=True, help="Output dir: apps/web/public/data")
    p.add_argument("--batch", type=int, default=16)
    p.add_argument("--cutoff-minutes", type=int, default=5)
    p.add_argument("--max-tickers", type=int, default=0)
    p.add_argument("--max-workers", type=int, default=8)
    p.add_argument("--news-per-provider", type=int, default=6000,
                   help="Target articles per provider (UI still caps visible headlines).")
    a = p.parse_args()

    # Show which sources are enabled (for CI visibility)
    has_fh = bool(os.getenv("FINNHUB_TOKEN") or os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_KEY"))
    print(f"News sources: {'Finnhub, ' if has_fh else ''}GDELT, Yahoo Finance, Google RSS, Yahoo RSS, Nasdaq RSS")
    if not has_fh:
        print("  (Finnhub token not set; set FINNHUB_TOKEN to unlock deeper historical coverage)")

    # Universe (tolerate unnamed first column)
    uni = pd.read_csv(a.universe)
    if "ticker" not in uni.columns:
        uni = uni.rename(columns={uni.columns[0]: "ticker"})
    tickers = sorted(str(x).strip().upper() for x in uni["ticker"].dropna().unique().tolist())
    if a.max_tickers and a.max_tickers > 0:
        tickers = tickers[: a.max_tickers]

    print(
        f"Build JSON for {len(tickers)} tickers | batch={a.batch} "
        f"cutoff_min={a.cutoff_minutes} max_workers={a.max_workers} news_per_provider={a.news_per_provider}"
    )

    # -------- Prices --------
    print("Prices:")
    prices = _fetch_all_prices(tickers, a.start, a.end, max_workers=a.max_workers)
    if prices is None or prices.empty:
        raise RuntimeError("No prices downloaded.")
    print(f"  ✓ Downloaded prices for {prices['ticker'].nunique()} tickers, {len(prices)} rows.")

    # -------- News --------
    print("News+Earnings:")
    try:
        fb = FinBERT()
    except Exception:
        fb = None

    news_all: List[pd.DataFrame] = []
    for t in tickers:
        comp = _best_effort_company(t)
        try:
            # Request *deep* coverage; writers will use all days for S but show only 10 headlines.
            n = fetch_news(t, a.start, a.end, company=comp, max_per_provider=a.news_per_provider)
            if n is None or n.empty:
                # paranoid second pass without company hint
                n = fetch_news(t, a.start, a.end, company=None, max_per_provider=a.news_per_provider)
        except Exception:
            n = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

        # visibility for CI logs
        if n is None or n.empty:
            print(f"  {t}: news rows=0 | days=0 | company={'-' if not comp else comp}")
        else:
            dcount = n["ts"].dt.date.nunique() if "ts" in n.columns else 0
            print(f"  {t}: news rows={len(n)} | days={dcount} | company={'-' if not comp else comp}")

        # Score with FinBERT (graceful if missing)
        n = _score_rows_inplace(fb, n, text_col="text", batch=a.batch)
        news_all.append(n)

    news_rows = (
        pd.concat(news_all, ignore_index=True)
        if news_all else
        pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])
    )

    # Placeholder earnings frame (kept schema-compatible for future use)
    earn_rows = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])

    # -------- Aggregate daily sentiment --------
    d_news = daily_sentiment_from_rows(news_rows, "news", cutoff_minutes=a.cutoff_minutes)
    d_earn = (
        daily_sentiment_from_rows(earn_rows, "earn", cutoff_minutes=a.cutoff_minutes)
        if not earn_rows.empty else
        pd.DataFrame(columns=["date", "ticker", "S_EARN"])
    )

    # normalize date dtype to naive (merge key)
    if not d_news.empty:
        d_news["date"] = pd.to_datetime(d_news["date"], utc=True, errors="coerce").dt.tz_localize(None)
    if not d_earn.empty:
        d_earn["date"] = pd.to_datetime(d_earn["date"], utc=True, errors="coerce").dt.tz_localize(None)

    # Join + guarantee a composite S
    daily = join_and_fill_daily(d_news, d_earn)
    if "S" not in daily.columns:
        # .get may return scalar -> wrap with Series then fill
        s_news = pd.to_numeric(daily.get("S_NEWS", pd.Series(0.0, index=daily.index)), errors="coerce").fillna(0.0)
        s_earn = pd.to_numeric(daily.get("S_EARN", pd.Series(0.0, index=daily.index)), errors="coerce").fillna(0.0)
        daily["S"] = s_news + s_earn

    # -------- Build panel + write outputs --------
    # prices.date is already naive (UTC day)
    panel = prices.merge(daily, on=["date", "ticker"], how="left")
    for c in ("S", "S_NEWS", "S_EARN"):
        if c not in panel.columns:
            panel[c] = 0.0
        panel[c] = pd.to_numeric(panel[c], errors="coerce").fillna(0.0)

    # Writers:
    #  • keep only 10 headlines in JSON (for UI)
    #  • if S ~ 0 or FinBERT unavailable, synthesize a *daily* curve from news intensity
    write_outputs(panel, news_rows, earn_rows, a.out)

    # -------- Summary read from outputs (truth) --------
    tickers_list, have_files, with_news, with_nonzero_s = _summarize_from_files(a.out)
    print("Summary:")
    print(f"  Tickers listed: {len(tickers_list)}")
    print(f"  Ticker JSON files: {have_files}")
    print(f"  Tickers with any news: {with_news}/{len(tickers_list)}")
    print(f"  Tickers with non-zero daily S: {with_nonzero_s}/{len(tickers_list)}")


if __name__ == "__main__":
    main()
