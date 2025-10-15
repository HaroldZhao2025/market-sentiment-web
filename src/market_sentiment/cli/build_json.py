from __future__ import annotations
import argparse
from pathlib import Path
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
from tqdm import tqdm

from market_sentiment.prices import fetch_prices_yf
from market_sentiment.news import fetch_news
from market_sentiment.edgar import fetch_earnings_docs
from market_sentiment.finbert import FinBERT, add_finbert_score
from market_sentiment.aggregate import (
    add_forward_returns, daily_sentiment_from_rows, join_and_fill_daily
)
from market_sentiment.writers import build_ticker_json, write_outputs

def _load_universe(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    # accept Symbol or ticker column
    if "Symbol" in df.columns:
        df = df.rename(columns={"Symbol":"ticker"})
    elif "ticker" not in df.columns:
        raise ValueError("Universe must have a 'Symbol' or 'ticker' column")
    df["ticker"] = df["ticker"].astype(str).str.upper()
    return df

def _fetch_all_prices(tickers: list[str], start: str, end: str, max_workers: int = 8) -> pd.DataFrame:
    rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(fetch_prices_yf, t, start, end): t for t in tickers}
        for f in tqdm(as_completed(futs), total=len(futs), desc="Prices"):
            rows.append(f.result())
    if not rows:
        return pd.DataFrame(columns=["date","ticker","open","close"])
    df = pd.concat(rows, ignore_index=True)
    return df.drop_duplicates(["date","ticker"]).sort_values(["ticker","date"]).reset_index(drop=True)

def _score_rows_inplace(fb: FinBERT, rows: pd.DataFrame, text_col: str, batch_size: int):
    scored = add_finbert_score(rows, text_col=text_col, fb=fb, batch_size=batch_size)
    # ensure required columns are present
    for c in ("S",):
        if c not in scored.columns:
            scored[c] = 0.0
    return scored

def main():
    ap = argparse.ArgumentParser("Build JSON artifacts for the web app")
    ap.add_argument("--universe", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--batch", type=int, default=16)
    ap.add_argument("--cutoff-minutes", type=int, default=5)
    ap.add_argument("--max-tickers", type=int, default=0, help="0 = full")
    ap.add_argument("--max-workers", type=int, default=8)
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    uni = _load_universe(args.universe)
    tickers = sorted(uni["ticker"].astype(str).str.upper().unique().tolist())
    if args.max_tickers and args.max_tickers > 0:
        tickers = tickers[:args.max_tickers]

    print(f"Build JSON for {len(tickers)} tickers | batch={args.batch} cutoff_min={args.cutoff_minutes} max_workers={args.max_workers}")

    # Prices
    prices = _fetch_all_prices(tickers, args.start, args.end, max_workers=args.max_workers)
    prices = add_forward_returns(prices)

    # Prepare FinBERT once
    fb = FinBERT()

    # Collect scored texts
    all_news, all_earn = [], []

    for t in tqdm(tickers, desc="News+Earnings"):
        news = fetch_news(t, args.start, args.end)
        earn = fetch_earnings_docs(t, args.start, args.end)

        if not news.empty:
            news = _score_rows_inplace(fb, news.assign(kind="news"), text_col="text", batch_size=args.batch)
            all_news.append(news)

        if not earn.empty:
            earn = _score_rows_inplace(fb, earn.assign(kind="earn"), text_col="text", batch_size=args.batch)
            all_earn.append(earn)

    news_rows = pd.concat(all_news, ignore_index=True) if all_news else pd.DataFrame(columns=["ticker","ts","title","url","text","S"])
    earn_rows = pd.concat(all_earn, ignore_index=True) if all_earn else pd.DataFrame(columns=["ticker","ts","title","url","text","S"])

    # Daily aggregates
    d_news = daily_sentiment_from_rows(news_rows, "news", cutoff_minutes=args.cutoff_minutes)
    d_earn = daily_sentiment_from_rows(earn_rows, "earn", cutoff_minutes=args.cutoff_minutes)
    daily = join_and_fill_daily(d_news, d_earn)

    # Panel for portfolio join on returns
    panel = prices.merge(daily, on=["date","ticker"], how="left").fillna({"S":0.0})

    # Top news per ticker by |S|
    if not news_rows.empty:
        top_news = news_rows.copy()
        top_news["absS"] = top_news["S"].abs()
    else:
        top_news = pd.DataFrame(columns=["ticker","ts","title","url","text","S","absS"])

    # Build per-ticker JSON
    per_ticker = {}
    for t in tqdm(tickers, desc="WriteTicker"):
        obj = build_ticker_json(t, prices, daily, top_news)
        per_ticker[t] = obj

    # Write outputs (tickers, portfolio, per-ticker files)
    write_outputs(out_dir, tickers, panel, per_ticker)

    # Summary for CI logs
    tick_files = [p.name[:-5] for p in out_dir.glob("*.json") if p.name not in ("_tickers.json","portfolio.json")]
    have_sent = daily.groupby("ticker")["S"].apply(lambda s: (s != 0).any() if not s.empty else False)
    print("Summary:")
    print("  Tickers listed:", len(tickers))
    print("  Ticker JSON files:", len(tick_files))
    print(f"  Tickers with any news: {news_rows['ticker'].nunique() if not news_rows.empty else 0}/{len(tickers)}")
    nz = have_sent[have_sent].index.tolist()
    print(f"  Tickers with non-zero daily S: {len(nz)}/{len(tickers)}")
    samp = daily.groupby("ticker")["S"].apply(lambda s: np.mean(np.abs(s)) if not s.empty else 0.0).sort_values(ascending=False).head(5)
    for sym, v in samp.items():
        cnt = (daily.query("ticker==@sym and S!=0").shape[0])
        print(f"   {sym}: mean|S|={v:.4f} (nz_points={cnt})")

if __name__ == "__main__":
    main()
