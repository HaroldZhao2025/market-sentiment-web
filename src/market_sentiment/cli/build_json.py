# src/market_sentiment/cli/build_json.py
from __future__ import annotations

import argparse
import sys
from typing import List
import pandas as pd

from market_sentiment.finbert import FinBERT
from market_sentiment.news import fetch_news
from market_sentiment.aggregate import (
    add_forward_returns,
    daily_sentiment_from_rows,
    join_and_fill_daily,
)
from market_sentiment.prices import fetch_prices_yf
from market_sentiment.writers import write_outputs


def _read_universe(path: str) -> List[str]:
    df = pd.read_csv(path, dtype=str)
    cols = [c.lower().strip() for c in df.columns.tolist()]
    df.columns = cols
    # Accept both 'ticker' and 'symbol'
    col = None
    if "ticker" in df.columns:
        col = "ticker"
    elif "symbol" in df.columns:
        col = "symbol"
    elif len(df.columns) == 1:
        # Single unnamed column -> treat as tickers
        col = df.columns[0]
    else:
        raise ValueError(
            f"Universe CSV must contain 'ticker' or 'symbol' column. Found columns: {df.columns.tolist()}"
        )
    tickers = (
        df[col].dropna().map(lambda s: str(s).strip().upper()).tolist()
    )
    tickers = [t for t in tickers if t]
    if not tickers:
        raise ValueError("No tickers found in universe CSV.")
    return sorted(list(dict.fromkeys(tickers)))


def main() -> None:
    ap = argparse.ArgumentParser("Build JSON artifacts for the web app")
    ap.add_argument("--universe", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--batch", type=int, default=16)
    ap.add_argument("--cutoff-minutes", type=int, default=5)
    ap.add_argument("--max-workers", type=int, default=8)
    args = ap.parse_args()

    tickers = _read_universe(args.universe)
    print(f"Build JSON for {len(tickers)} tickers | batch={args.batch} cutoff_min={args.cutoff_minutes} max_workers={args.max_workers}")

    # 1) Prices (vectorized by ticker via yfinance)
    print("Prices:")
    prices_rows = []
    for t in tickers:
        try:
            dfp = fetch_prices_yf(t, args.start, args.end)
        except Exception as e:
            dfp = pd.DataFrame(columns=["date","ticker","open","close"])
        if not dfp.empty:
            prices_rows.append(dfp)
    prices = pd.concat(prices_rows, ignore_index=True) if prices_rows else pd.DataFrame(columns=["date","ticker","open","close"])
    if prices.empty:
        raise RuntimeError("No prices downloaded.")
    prices = add_forward_returns(prices)
    print(f"  ✓ Downloaded prices for {len(tickers)} tickers, {len(prices)} rows.")

    # 2) News + FinBERT
    print("News+Earnings:")
    fb = FinBERT()
    news_rows = []
    for t in tickers:
        df = fetch_news(t, args.start, args.end, company=None, max_per_provider=120)
        if df.empty:
            news_rows.append(pd.DataFrame(columns=["ticker","ts","title","url","text","S"]))
            continue
        texts = df["text"].astype(str).tolist()
        try:
            S = fb.score(texts, batch=args.batch)  # if supported
        except TypeError:
            S = fb.score(texts)                     # fallback
        df["S"] = pd.to_numeric(pd.Series(S), errors="coerce").fillna(0.0)
        news_rows.append(df)
    news_all = pd.concat(news_rows, ignore_index=True) if news_rows else pd.DataFrame(columns=["ticker","ts","title","url","text","S"])

    # 3) Aggregate to daily
    d_news = daily_sentiment_from_rows(news_all, kind="news", cutoff_minutes=args.cutoff_minutes)
    d_earn = pd.DataFrame(columns=["date","ticker","S","count"])  # keep earnings path pluggable
    daily = join_and_fill_daily(d_news, d_earn)

    # 4) Join with prices to create panel & write outputs
    panel = prices.merge(daily.rename(columns={"count":"news_count"}), on=["date","ticker"], how="left") \
                  .fillna({"S":0.0, "news_count":0})
    write_outputs(panel, news_all, args.out)

    # 5) Summary
    have_news_per_t = news_all.groupby("ticker")["S"].apply(lambda s: (s.abs() > 1e-12).any()).reset_index()
    nz = int(have_news_per_t["S"].sum())
    print("Summary:")
    print(f"  Tickers listed: {len(tickers)}")
    print(f"  Ticker JSON files: {len(tickers)}")
    print(f"  Tickers with any news: {nz}/{len(tickers)}")
    print(f"  Tickers with non-zero daily S: {int((daily.groupby('ticker')['S'].apply(lambda s: (s.abs()>1e-12).any())).sum())}/{len(tickers)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("FATAL:", repr(e), file=sys.stderr)
        raise
