# src/market_sentiment/cli/build_json.py
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

import pandas as pd
from tqdm import tqdm

from market_sentiment.prices import fetch_prices_yf
from market_sentiment.news import fetch_news
from market_sentiment.finbert import FinBERT
from market_sentiment.aggregate import (
    daily_sentiment_from_rows,
    join_and_fill_daily,
    add_forward_returns,
    _ensure_date_dtype,  # internal helper to normalize dates
)
from market_sentiment.writers import write_outputs


def _fetch_all_prices(tickers: List[str], start: str, end: str, max_workers: int = 8) -> pd.DataFrame:
    rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(fetch_prices_yf, t, start, end): t for t in tickers}
        for f in tqdm(as_completed(futs), total=len(futs), desc="Prices"):
            try:
                df = f.result()
                if df is not None and not df.empty:
                    rows.append(df)
            except Exception:
                # ignore bad tickers but keep going
                pass
    if rows:
        out = pd.concat(rows, ignore_index=True)
        return out[["date", "ticker", "open", "close"]]
    return pd.DataFrame(columns=["date", "ticker", "open", "close"])


def main() -> None:
    ap = argparse.ArgumentParser("Build JSON artifacts for the web app")
    ap.add_argument("--universe", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--batch", type=int, default=16)
    ap.add_argument("--cutoff-minutes", type=int, default=5, dest="cutoff_minutes")
    ap.add_argument("--max-workers", type=int, default=8, dest="max_workers")
    args = ap.parse_args()

    uni = pd.read_csv(args.universe)
    # accept either a 'ticker' column or first column as tickers
    if "ticker" in uni.columns:
        tickers: List[str] = sorted([str(x).strip().upper() for x in uni["ticker"].dropna().unique().tolist()])
    else:
        tickers = sorted([str(x).strip().upper() for x in uni.iloc[:, 0].dropna().unique().tolist()])

    print(f"Build JSON for {len(tickers)} tickers | batch={args.batch} cutoff_min={args.cutoff_minutes} max_workers={args.max_workers}")

    # Prices
    prices = _fetch_all_prices(tickers, args.start, args.end, max_workers=args.max_workers)
    if prices.empty:
        raise RuntimeError("No prices downloaded.")
    prices = _ensure_date_dtype(prices, "date")
    print(f"  ✓ Downloaded prices for {prices['ticker'].nunique()} tickers, {len(prices)} rows.")

    # News (+ scoring)
    fb = None
    try:
        fb = FinBERT()
    except Exception:
        fb = None

    news_parts = []
    for t in tqdm(tickers, desc="News+Earnings"):
        n = fetch_news(t, args.start, args.end)
        if n is None or n.empty:
            continue
        if fb is not None and not n.empty:
            try:
                n["S"] = fb.score(n["title"].fillna("").astype(str).tolist(), batch_size=args.batch)
            except TypeError:
                # older method signature
                n["S"] = fb.score(n["title"].fillna("").astype(str).tolist())
        else:
            n["S"] = 0.0
        n["kind"] = "news"
        news_parts.append(n)

    news_all = pd.concat(news_parts, ignore_index=True) if news_parts else pd.DataFrame(
        columns=["ticker", "ts", "title", "url", "text", "S", "kind"]
    )

    # Aggregate daily
    d_news = daily_sentiment_from_rows(news_all, "news", cutoff_minutes=args.cutoff_minutes)
    d_earn = pd.DataFrame(columns=["date", "ticker", "S"])  # optional placeholder
    daily = join_and_fill_daily(d_news, d_earn)

    # Merge & returns
    daily = _ensure_date_dtype(daily, "date")
    panel = prices.merge(daily, on=["date", "ticker"], how="left").fillna({"S": 0.0})
    panel = add_forward_returns(panel)

    # Write artifacts (writers.py requires: panel, news_rows, earn_rows, out_dir)
    write_outputs(panel, news_all, d_earn, args.out)

    # Summary
    tickers_list = sorted(panel["ticker"].unique().tolist())
    anynews = news_all["ticker"].nunique() if not news_all.empty else 0
    nz = panel.groupby("ticker")["S"].apply(lambda s: (s.abs() > 1e-12).any()).sum()

    print("Summary:")
    print(f"  Tickers listed: {len(tickers_list)}")
    print(f"  Ticker JSON files: {len(tickers_list)}")
    print(f"  Tickers with any news: {anynews}/{len(tickers_list)}")
    print(f"  Tickers with non-zero daily S: {nz}/{len(tickers_list)}")


if __name__ == "__main__":
    main()
