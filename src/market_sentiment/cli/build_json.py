# src/market_sentiment/cli/build_json.py
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

import pandas as pd

from market_sentiment.aggregate import (
    add_forward_returns,
    daily_sentiment_from_rows,
    join_and_fill_daily,
    _ensure_date_dtype,
)
from market_sentiment.finbert import FinBERT
from market_sentiment.news import fetch_news
from market_sentiment.prices import fetch_prices_yf
from market_sentiment.writers import write_outputs


def _score_texts(fb: FinBERT, texts: List[str], batch: int) -> List[float]:
    """
    Be compatible with different FinBERT.score signatures:
      - score(texts, batch=?)
      - score(texts, batch_size=?)
      - score(texts)
    """
    try:
        return fb.score(texts, batch=batch)
    except TypeError:
        try:
            return fb.score(texts, batch_size=batch)
        except TypeError:
            return fb.score(texts)


def _fetch_all_prices(tickers: List[str], start: str, end: str, max_workers: int) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(fetch_prices_yf, t, start, end): t for t in tickers}
        for f in as_completed(futs):
            df = f.result()
            if df is not None and len(df) > 0:
                rows.append(df)

    if not rows:
        return pd.DataFrame(columns=["date", "ticker", "open", "close"])
    prices = pd.concat(rows, ignore_index=True)
    prices = _ensure_date_dtype(prices, "date")
    prices = add_forward_returns(prices)
    return prices


def _score_rows_inplace(fb: FinBERT, df: pd.DataFrame, text_col: str, batch: int) -> pd.DataFrame:
    if df is None or df.empty:
        df = pd.DataFrame(columns=["ticker", "ts", "title", "url", text_col, "S"])
        return df

    texts = df[text_col].astype(str).fillna("").tolist()
    if len(texts) == 0:
        df["S"] = 0.0
        return df

    scores = _score_texts(fb, texts, batch=batch)
    df = df.copy()
    df["S"] = pd.to_numeric(pd.Series(scores), errors="coerce").fillna(0.0)
    return df


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

    # Read universe; tolerate first column not named 'ticker'
    uni = pd.read_csv(a.universe)
    if "ticker" not in uni.columns:
        first = uni.columns[0]
        uni = uni.rename(columns={first: "ticker"})
    tickers: List[str] = sorted(
        [str(x).strip().upper() for x in uni["ticker"].dropna().unique().tolist()]
    )
    if a.max_tickers and a.max_tickers > 0:
        tickers = tickers[: a.max_tickers]

    mode = f"{len(tickers)} tickers"
    print(f"Build JSON for {mode} | batch={a.batch} cutoff_min={a.cutoff_minutes} max_workers={a.max_workers}")

    # ---------------- Prices ----------------
    print("Prices:")
    prices = _fetch_all_prices(tickers, a.start, a.end, max_workers=a.max_workers)
    if prices is None or prices.empty:
        raise RuntimeError("No prices downloaded.")
    print(f"  ✓ Downloaded prices for {prices['ticker'].nunique()} tickers, {len(prices)} rows.")

    # ---------------- News + (optional) earnings ----------------
    print("News+Earnings:")
    fb = FinBERT()
    news_all: List[pd.DataFrame] = []
    # (If you add earnings rows, append them in a list and concat similarly.)
    for t in tickers:
        # fetch, score
        n = fetch_news(t, a.start, a.end)
        n = _score_rows_inplace(fb, n, text_col="text", batch=a.batch)
        news_all.append(n)

    news_rows = pd.concat(news_all, ignore_index=True) if news_all else pd.DataFrame(
        columns=["ticker", "ts", "title", "url", "text", "S"]
    )

    # ---------------- Aggregate daily sentiment ----------------
    d_news = daily_sentiment_from_rows(news_rows, "news", cutoff_minutes=a.cutoff_minutes)
    # If you have earnings rows, compute d_earn similarly; otherwise use empty frame:
    d_earn = pd.DataFrame(columns=["date", "ticker", "S"])

    daily = join_and_fill_daily(d_news, d_earn)
    # Safety: ensure S is present even if upstream changed join logic
    if "S" not in daily.columns:
        daily["S_NEWS"] = pd.to_numeric(daily.get("S_NEWS", 0.0), errors="coerce").fillna(0.0)
        daily["S_EARN"] = pd.to_numeric(daily.get("S_EARN", 0.0), errors="coerce").fillna(0.0)
        daily["S"] = daily["S_NEWS"] + daily["S_EARN"]

    # ---------------- Build panel ----------------
    panel = prices.merge(daily, on=["date", "ticker"], how="left")
    for c in ("S", "S_NEWS", "S_EARN"):
        if c not in panel.columns:
            panel[c] = 0.0
        panel[c] = pd.to_numeric(panel[c], errors="coerce").fillna(0.0)

    # ---------------- Write outputs ----------------
    write_outputs(panel, news_rows, a.out)

    # ---------------- Summary ----------------
    # count tickers file was written for is inferred by writers; here we recompute:
    listed = len(tickers)
    with_news = d_news["ticker"].nunique() if not d_news.empty else 0
    nz = panel.groupby("ticker")["S"].apply(lambda s: (s.abs() > 1e-12).any()).sum()

    print("Summary:")
    print(f"  Tickers listed: {listed}")
    print(f"  Ticker JSON files: {listed}")  # writers produces one per ticker in the universe
    print(f"  Tickers with any news: {with_news}/{listed}")
    print(f"  Tickers with non-zero daily S: {nz}/{listed}")


if __name__ == "__main__":
    main()
