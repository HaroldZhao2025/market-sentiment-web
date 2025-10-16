# src/market_sentiment/cli/build_json.py
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List

import pandas as pd
from tqdm import tqdm

from market_sentiment.prices import fetch_prices_yf_many
from market_sentiment.news import fetch_news
from market_sentiment.edgar import fetch_earnings_docs
from market_sentiment.finbert import FinBERT
from market_sentiment.aggregate import (
    daily_sentiment_from_rows,
    join_and_fill_daily,
    add_forward_returns,
)
from market_sentiment.writers import write_outputs


def main():
    ap = argparse.ArgumentParser("Build JSON artifacts for the web app")
    ap.add_argument("--universe", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--batch", type=int, default=16)
    ap.add_argument("--cutoff-minutes", type=int, default=5)
    ap.add_argument("--max-tickers", type=int, default=0)   # 0 = all
    ap.add_argument("--max-workers", type=int, default=8)   # kept for compatibility (not used directly)
    ap.add_argument("--chunk-size", type=int, default=50)
    ap.add_argument("--tries", type=int, default=3)
    args = ap.parse_args()

    uni = pd.read_csv(args.universe)
    tickers: List[str] = sorted([str(x).strip().upper() for x in uni["ticker"].dropna().unique().tolist()])
    if args.max_tickers and args.max_tickers > 0:
        tickers = tickers[: args.max_tickers]

    print(f"\nBuild JSON for {len(tickers)} tickers | batch={args.batch} cutoff_min={args.cutoff_minutes} max_workers={args.max_workers}")
    print("Prices:")

    prices = fetch_prices_yf_many(
        tickers=tickers,
        start=args.start,
        end=args.end,
        chunk_size=args.chunk_size,
        tries=args.tries,
        pause=2.0,
    )
    if prices.empty:
        raise RuntimeError("No prices downloaded.")
    print(f"  ✓ Downloaded prices for {prices['ticker'].nunique()} tickers, {len(prices)} rows.")

    prices = add_forward_returns(prices)

    fb = FinBERT()
    print("News+Earnings:")
    news_rows_all = []
    earn_rows_all = []
    for t in tqdm(tickers, desc="News+Earnings"):
        n = fetch_news(t, args.start, args.end)
        e = fetch_earnings_docs(t, args.start, args.end)

        if n is not None and not n.empty:
            n = n.copy()
            # NOTE: pass batch_size (not 'batch')
            n["S"] = fb.score(n["text"].astype(str).tolist(), batch_size=args.batch)
            news_rows_all.append(n[["ticker", "ts", "title", "url", "text", "S"]])

        if e is not None and not e.empty:
            e = e.copy()
            e["S"] = fb.score(e["text"].astype(str).tolist(), batch_size=args.batch)
            earn_rows_all.append(e[["ticker", "ts", "title", "url", "text", "S"]])

    news_rows = pd.concat(news_rows_all, ignore_index=True) if news_rows_all else pd.DataFrame(columns=["ticker","ts","title","url","text","S"])
    earn_rows = pd.concat(earn_rows_all, ignore_index=True) if earn_rows_all else pd.DataFrame(columns=["ticker","ts","title","url","text","S"])

    d_news = daily_sentiment_from_rows(news_rows, "news", cutoff_minutes=args.cutoff_minutes)
    d_earn = daily_sentiment_from_rows(earn_rows, "earn", cutoff_minutes=args.cutoff_minutes)
    daily = join_and_fill_daily(d_news, d_earn)

    panel = prices.merge(daily, on=["date", "ticker"], how="left")
    for c, default in [("S", 0.0), ("news_count", 0), ("earn_count", 0)]:
        panel[c] = pd.to_numeric(panel.get(c, default), errors="coerce").fillna(default)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    write_outputs(panel, news_rows, earn_rows, out_dir)

    # Summary & smoke checks
    tickers_listed = sorted(panel["ticker"].unique().tolist())
    ticker_files = list((out_dir / "ticker").glob("*.json"))
    with_news = (daily.groupby("ticker")["news_count"].sum() > 0) if not daily.empty else pd.Series(dtype=bool)
    nz = (daily.groupby("ticker")["S"].apply(lambda s: (s.abs() > 0).sum()).fillna(0).astype(int)) if not daily.empty else pd.Series(dtype=int)

    print("\nSummary:")
    print(f"  Tickers listed: {len(tickers_listed)}")
    print(f"  Ticker JSON files: {len(ticker_files)}")
    print(f"  Tickers with any news: {int(with_news.sum())}/{len(tickers_listed)}" if len(tickers_listed) else "  Tickers with any news: 0/0")
    print(f"  Tickers with non-zero daily S: {int((nz > 0).sum())}/{len(tickers_listed)}" if len(tickers_listed) else "  Tickers with non-zero daily S: 0/0")

    with open(out_dir / "_tickers.json", "w") as f:
        json.dump(tickers_listed, f)


if __name__ == "__main__":
    main()
