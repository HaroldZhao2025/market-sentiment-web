from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List

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
from market_sentiment.writers import write_outputs


def _fetch_all_prices(tickers: List[str], start: str, end: str, max_workers: int = 8) -> pd.DataFrame:
    rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(fetch_prices_yf, t, start, end) for t in tickers]
        for f in as_completed(futs):
            rows.append(f.result())
    if not rows:
        return pd.DataFrame(columns=["date", "ticker", "open", "close"])
    return pd.concat(rows, ignore_index=True).dropna()


def _score_inplace(fb: FinBERT, df: pd.DataFrame, text_col: str, batch: int) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    texts = df[text_col].astype(str).tolist()
    # fb.score returns a list[float] (values in [-1,1])
    scores = fb.score(texts, batch=batch)
    s = pd.to_numeric(pd.Series(scores), errors="coerce").fillna(0.0)
    df = df.copy()
    df["S"] = s.values
    return df


def main():
    ap = argparse.ArgumentParser("Build JSON artifacts for the web app")
    ap.add_argument("--universe", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--batch", type=int, default=16)
    ap.add_argument("--cutoff-minutes", type=int, default=5)
    ap.add_argument("--max-tickers", type=int, default=0)   # 0 = all
    ap.add_argument("--max-workers", type=int, default=8)
    a = ap.parse_args()

    uni = pd.read_csv(a.universe)
    tickers = sorted([str(x).strip().upper() for x in uni["ticker"].dropna().unique().tolist()])
    if a.max_tickers and a.max_tickers > 0:
        tickers = tickers[: a.max_tickers]

    print(f"\nBuild JSON for {len(tickers)} tickers | batch={a.batch} cutoff_min={a.cutoff_minutes} max_workers={a.max_workers}")

    # 1) Prices
    print("Prices:")
    prices = _fetch_all_prices(tickers, a.start, a.end, max_workers=a.max_workers)
    prices = prices.dropna()
    if prices.empty:
        raise RuntimeError("No prices downloaded.")
    prices = add_forward_returns(prices)

    # 2) News + Earnings -> score with FinBERT
    fb = FinBERT()
    print("News+Earnings:")
    news_rows_all = []
    earn_rows_all = []
    for t in tqdm(tickers, desc="News+Earnings"):
        n = fetch_news(t, a.start, a.end)
        e = fetch_earnings_docs(t, a.start, a.end)
        # Score texts (title+text best; we already have 'text' normalized)
        n = _score_inplace(fb, n, text_col="text", batch=a.batch)
        e = _score_inplace(fb, e, text_col="text", batch=a.batch)
        if n is not None and not n.empty:
            news_rows_all.append(n[["ticker", "ts", "title", "url", "text", "S"]])
        if e is not None and not e.empty:
            earn_rows_all.append(e[["ticker", "ts", "title", "url", "text", "S"]])

    news_rows = pd.concat(news_rows_all, ignore_index=True) if news_rows_all else pd.DataFrame(columns=["ticker","ts","title","url","text","S"])
    earn_rows = pd.concat(earn_rows_all, ignore_index=True) if earn_rows_all else pd.DataFrame(columns=["ticker","ts","title","url","text","S"])

    # 3) Daily aggregates
    d_news = daily_sentiment_from_rows(news_rows, "news", cutoff_minutes=a.cutoff_minutes)
    d_earn = daily_sentiment_from_rows(earn_rows, "earn", cutoff_minutes=a.cutoff_minutes)
    daily = join_and_fill_daily(d_news, d_earn)

    # 4) Join panel
    panel = prices.merge(daily, on=["date", "ticker"], how="left")
    for c, default in [("S", 0.0), ("news_count", 0), ("earn_count", 0)]:
        panel[c] = pd.to_numeric(panel.get(c, default), errors="coerce").fillna(default)

    # 5) Write all outputs
    out_dir = Path(a.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    write_outputs(panel, news_rows, earn_rows, out_dir)

    # 6) Summary
    tickers_listed = sorted(panel["ticker"].unique().tolist())
    ticker_files = list((out_dir / "ticker").glob("*.json"))
    tickers_with_files = sorted([p.stem for p in ticker_files])

    nz = (daily.groupby("ticker")["S"].apply(lambda s: (s.abs() > 0).sum()).fillna(0).astype(int))
    with_news = (daily.groupby("ticker")["news_count"].sum() > 0)

    print("\nSummary:")
    print(f"  Tickers listed: {len(tickers_listed)}")
    print(f"  Ticker JSON files: {len(tickers_with_files)}")
    print(f"  Tickers with any news: {with_news.sum()}/{len(tickers_listed)}")
    print(f"  Tickers with non-zero daily S: {(nz > 0).sum()}/{len(tickers_listed)}")

    # write _tickers.json again for sanity (list of strings)
    with open(out_dir / "_tickers.json", "w") as f:
        json.dump(tickers_listed, f)


if __name__ == "__main__":
    main()
