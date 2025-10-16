# src/market_sentiment/cli/build_json.py
from __future__ import annotations

import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from tqdm import tqdm

from market_sentiment.prices import fetch_prices_yf
from market_sentiment.news import fetch_news
from market_sentiment.edgar import fetch_earnings_docs
from market_sentiment.finbert import FinBERT
from market_sentiment.aggregate import (
    daily_sentiment_from_rows,
    join_and_fill_daily,
    add_forward_returns,
)
from market_sentiment.writers import build_ticker_json, write_outputs


# ---------- NEW: tiny helper to align date dtypes ----------
def _to_naive_midnight(s: pd.Series) -> pd.Series:
    """
    Make a date-like Series tz-naive (no timezone) and normalized to midnight.
    Works whether input is tz-aware, tz-naive, strings, or timestamps.
    """
    # Force to UTC-aware first (handles both aware and naive inputs)
    s2 = pd.to_datetime(s, errors="coerce", utc=True)
    # Drop timezone (becomes naive) and normalize to 00:00:00
    return s2.dt.tz_localize(None).dt.normalize()
# -----------------------------------------------------------


def _fetch_all_prices(tickers, start, end, max_workers: int = 8) -> pd.DataFrame:
    rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(fetch_prices_yf, t, start, end): t for t in tickers}
        for f in tqdm(as_completed(futs), total=len(futs), desc="Prices"):
            rows.append(f.result())
    out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(
        columns=["date", "ticker", "open", "close"]
    )
    return out


def _score_rows_inplace(fb: FinBERT, df: pd.DataFrame, text_col: str, batch: int = 16) -> pd.DataFrame:
    if df.empty:
        return df
    texts = (df[text_col].fillna("").astype(str)).tolist()
    # Be tolerant of older/newer FinBERT.score signatures
    try:
        scores = fb.score(texts, batch=batch)
    except TypeError:
        scores = fb.score(texts)
    df["S"] = pd.to_numeric(pd.Series(scores), errors="coerce").fillna(0.0)
    return df


def main():
    ap = argparse.ArgumentParser("Build JSON artifacts for the web app")
    ap.add_argument("--universe", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--batch", type=int, default=16)
    ap.add_argument("--cutoff-minutes", type=int, default=5)
    ap.add_argument("--max-tickers", type=int, default=0)
    ap.add_argument("--max-workers", type=int, default=8)
    a = ap.parse_args()

    out_dir = Path(a.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Universe
    u = pd.read_csv(a.universe)
    tick_col = "ticker" if "ticker" in u.columns else ("Symbol" if "Symbol" in u.columns else None)
    if not tick_col:
        raise ValueError("Universe CSV must have 'ticker' or 'Symbol' column.")
    tickers = u[tick_col].astype(str).str.upper().dropna().unique().tolist()
    if a.max_tickers and a.max_tickers > 0:
        tickers = tickers[: a.max_tickers]

    print(f"\nBuild JSON for {len(tickers)} tickers | batch={a.batch} cutoff_min={a.cutoff_minutes} max_workers={a.max_workers}")

    # Prices
    prices = _fetch_all_prices(tickers, a.start, a.end, max_workers=a.max_workers).copy()
    # ---------- KEY FIX #1: make prices['date'] tz-naive midnight ----------
    prices["date"] = _to_naive_midnight(prices["date"])

    # News + Earnings + FinBERT
    fb = FinBERT()
    news_rows = []
    earn_rows = []
    for t in tqdm(tickers, desc="News+Earnings"):
        n = fetch_news(t, a.start, a.end)
        e = fetch_earnings_docs(t, a.start, a.end)
        if not n.empty:
            n = _score_rows_inplace(fb, n, text_col="text", batch=a.batch)
            news_rows.append(n[["ticker", "ts", "title", "url", "text", "S"]])
        if not e.empty:
            e = _score_rows_inplace(fb, e, text_col="text", batch=a.batch)
            earn_rows.append(e[["ticker", "ts", "title", "url", "text", "S"]])

    news_rows = pd.concat(news_rows, ignore_index=True) if news_rows else pd.DataFrame(
        columns=["ticker", "ts", "title", "url", "text", "S"]
    )
    earn_rows = pd.concat(earn_rows, ignore_index=True) if earn_rows else pd.DataFrame(
        columns=["ticker", "ts", "title", "url", "text", "S"]
    )

    # Aggregate to daily
    d_news = daily_sentiment_from_rows(news_rows, kind="news", cutoff_minutes=a.cutoff_minutes)
    d_earn = daily_sentiment_from_rows(earn_rows, kind="earn", cutoff_minutes=a.cutoff_minutes)
    daily = join_and_fill_daily(d_news, d_earn)

    # ---------- KEY FIX #2: make daily['date'] tz-naive midnight ----------
    daily["date"] = _to_naive_midnight(daily["date"])

    # Merge & compute returns
    panel = prices.merge(daily, on=["date", "ticker"], how="left").fillna({"S": 0.0})
    panel = add_forward_returns(panel)

    # Write all outputs expected by the web app
    write_outputs(panel, news_rows, earn_rows, out_dir)

    # Small summary to the logs
    by_t = panel.groupby("ticker")["S"].apply(lambda s: (s.abs() > 0).sum())
    nz = int((by_t > 0).sum())
    print("\nSummary:")
    print(f"  Tickers listed: {len(tickers)}")
    print(f"  Tickers with non-zero daily S: {nz}/{len(tickers)}")


if __name__ == "__main__":
    main()
