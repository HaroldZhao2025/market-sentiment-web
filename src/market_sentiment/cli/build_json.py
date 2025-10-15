# src/market_sentiment/cli/build_json.py
from __future__ import annotations
import argparse
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm

from market_sentiment.finbert import FinBERT, score_texts
from market_sentiment.aggregate import (
    add_forward_returns,
    daily_sentiment_from_rows,
    combine_daily,
    safe_merge_prices_daily,
)
from market_sentiment.writers import build_ticker_json, write_json
from market_sentiment.edgar import fetch_earnings_docs

# news module is allowed to vary in your repo; adapt at runtime
from market_sentiment import news as news_mod
from market_sentiment import prices as prices_mod

def _get_news_fn():
    for name in ("fetch_news_yf", "news_yfinance", "fetch_news"):
        if hasattr(news_mod, name):
            return getattr(news_mod, name)
    raise ImportError("No news fetcher found in market_sentiment.news "
                      "(expected one of: fetch_news_yf, news_yfinance, fetch_news)")

def _get_prices_fn():
    for name in ("fetch_prices_yf", "prices_yf", "get_prices_yf"):
        if hasattr(prices_mod, name):
            return getattr(prices_mod, name)
    raise ImportError("No price fetcher found in market_sentiment.prices "
                      "(expected one of: fetch_prices_yf, prices_yf, get_prices_yf)")

def _load_universe(path: Path) -> List[str]:
    df = pd.read_csv(path)
    # support either 'symbol' or 'ticker'
    for c in ("symbol","ticker","Symbol","Ticker"):
        if c in df.columns:
            syms = df[c].astype(str).str.upper().str.strip().tolist()
            return syms
    raise KeyError("Universe CSV must contain a 'symbol' or 'ticker' column.")

def _score_texts(fb: FinBERT, texts: List[str], batch: int) -> List[float]:
    # Use the flexible helper we shipped; accepts batch_size param
    return score_texts(fb, texts, batch_size=int(batch))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", type=Path, required=True)
    ap.add_argument("--start", type=str, required=True)
    ap.add_argument("--end", type=str, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--batch", type=int, default=16)
    ap.add_argument("--cutoff-minutes", type=int, default=5)
    ap.add_argument("--max-workers", type=int, default=8)  # (not used here; left for future parallelization)
    ap.add_argument("--max-tickers", type=int, default=0, help="0 = ALL tickers in universe")
    ap.add_argument("--max-news-per-ticker", type=int, default=200, help="limit to top-N news rows (most recent)")
    args = ap.parse_args()

    tickers = _load_universe(args.universe)
    if args.max_tickers and args.max_tickers > 0:
        tickers = tickers[: args.max_tickers]

    news_fn = _get_news_fn()
    prices_fn = _get_prices_fn()

    fb = FinBERT()

    all_meta = []
    outdir = args.out
    outdir.mkdir(parents=True, exist_ok=True)

    # Collect for portfolio after the loop
    all_daily = []
    all_prices = []

    pbar = tqdm(tickers, desc="Build JSON", total=len(tickers))
    for t in pbar:
        # ---- prices ----
        p = prices_fn(t, args.start, args.end)  # expected columns include ['date','ticker',<close>]
        if p is None or len(p) == 0:
            # still write an empty skeleton so the site doesn't 404
            write_json({"ticker": t, "series": {}, "top_news": []}, outdir, f"{t}.json")
            continue
        p["ticker"] = t
        p = add_forward_returns(p)
        all_prices.append(p[["date","ticker",_pick_close_column(p),"ret_cc_1d"]])

        # ---- news ----
        news = news_fn(t, args.start, args.end)
        if news is None:
            news = pd.DataFrame(columns=["ts","title","url","text"])
        if len(news) > args.max_news_per_ticker:
            news = news.sort_values("ts", ascending=False).head(args.max_news_per_ticker)

        if len(news) > 0:
            scores = _score_texts(fb, news["text"].astype(str).tolist(), batch=args.batch)
            news = news.copy()
            news["S"] = scores
        else:
            news = pd.DataFrame(columns=["ts","title","url","text","S"])

        d_news = daily_sentiment_from_rows(news.assign(ticker=t), kind="news", cutoff_minutes=args.cutoff_minutes)

        # ---- earnings (EDGAR) ----
        earn = fetch_earnings_docs(t, args.start, args.end)
        if earn is None:
            earn = pd.DataFrame(columns=["ts","title","url","text"])
        if len(earn) > 0:
            e_scores = _score_texts(fb, earn["text"].astype(str).tolist(), batch=args.batch)
            earn = earn.copy()
            earn["S"] = e_scores
        else:
            earn = pd.DataFrame(columns=["ts","title","url","text","S"])

        d_earn = daily_sentiment_from_rows(earn.assign(ticker=t), kind="earn", cutoff_minutes=args.cutoff_minutes)

        # ---- combine daily ----
        d = combine_daily(d_news, d_earn)
        all_daily.append(d)

        # top news for ticker page
        top_news = None
        if len(news) > 0:
            top_news = news.sort_values("ts", ascending=False).head(25)[["ts","title","url","S"]]

        # ---- build single-ticker json ----
        ser = build_ticker_json(t, prices=p, daily=d, top_news=top_news)
        write_json(ser, outdir, f"{t}.json")

        # meta info for debugging
        nonzero = float(np.mean(np.abs(d["S"])) > 0) if len(d) else 0.0
        all_meta.append((t, int(len(news)), int(len(earn)), float(nonzero)))

    # ---- panel & portfolio json (only if we had any data) ----
    daily = pd.concat(all_daily, ignore_index=True) if all_daily else pd.DataFrame(columns=["date","ticker","S"])
    prices = pd.concat(all_prices, ignore_index=True) if all_prices else pd.DataFrame(columns=["date","ticker"])

    # Write tickers manifest
    manifest = {"tickers": tickers}
    write_json(manifest, outdir, "_tickers.json")

    # ---- Debug summary printed to stdout for CI logs ----
    nz = [t for (t, n, e, nonzero) in all_meta if nonzero > 0]
    had_news = [t for (t, n, e, _) in all_meta if n > 0]
    print(f"Tickers listed: {len(tickers)}")
    print(f"Tickers with any news: {len(had_news)}/{len(tickers)}")
    print(f"Tickers with non-zero daily S: {len(nz)}/{len(tickers)}")
    if len(nz) > 0:
        print("Sample non-zero S tickers:", ", ".join(nz[:10]))

if __name__ == "__main__":
    main()
