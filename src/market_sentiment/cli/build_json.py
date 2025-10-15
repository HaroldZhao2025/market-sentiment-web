# src/market_sentiment/cli/build_json.py
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from market_sentiment.finbert import FinBERT
from market_sentiment.news import fetch_news
from market_sentiment.edgar import fetch_earnings_docs
from market_sentiment.aggregate import add_forward_returns, daily_sentiment_from_rows
from market_sentiment.prices import fetch_prices_yf
from market_sentiment.universe import fetch_sp500  # if needed elsewhere
from market_sentiment.writers import (
    build_ticker_json,
    write_index_files,
    write_portfolio_json,
)


def _read_universe(path: str | Path) -> list[str]:
    df = pd.read_csv(path)
    # Try common column names
    for col in ["ticker", "Ticker", "symbol", "Symbol"]:
        if col in df.columns:
            syms = df[col].astype(str).str.upper().str.replace(".", "-", regex=False)
            return list(pd.unique(syms))
    # If the file is a one-column CSV of tickers
    if df.shape[1] == 1:
        return list(pd.unique(df.iloc[:, 0].astype(str).str.upper()))
    raise ValueError("Universe CSV must have a 'ticker'/'symbol' column or a single column of tickers.")


def _score_rows_inplace(fb: FinBERT, rows: pd.DataFrame, text_col: str, batch: int) -> pd.DataFrame:
    if rows.empty:
        rows["S"] = []
        return rows
    texts = rows[text_col].fillna("").astype(str).tolist()
    scores = fb.score(texts, batch=batch)  # returns list[float]
    rows = rows.copy()
    rows["S"] = pd.Series(scores, index=rows.index)
    return rows


def _safe_fetch_prices(ticker: str, start: str, end: str) -> pd.DataFrame:
    p = fetch_prices_yf(ticker, start, end)
    # Ensure normalized columns
    if "date" not in p.columns:
        # yfinance returns DatetimeIndex sometimes
        if p.index.name and "Date" in p.index.name or isinstance(p.index, pd.DatetimeIndex):
            p = p.reset_index().rename(columns={p.columns[0]: "date"})
    p["ticker"] = ticker
    return p


def main():
    ap = argparse.ArgumentParser("Build JSON artifacts for the web app")
    ap.add_argument("--universe", required=True, help="CSV of tickers")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--batch", type=int, default=16)
    ap.add_argument("--cutoff-minutes", type=int, default=5)
    ap.add_argument("--max-tickers", type=int, default=0, help="0=all")
    ap.add_argument("--max-workers", type=int, default=8)
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    tickers = _read_universe(args.universe)
    if args.max_tickers and args.max_tickers > 0:
        tickers = tickers[: args.max_tickers]

    print(f"Build JSON for {len(tickers)} tickers | batch={args.batch} cutoff_min={args.cutoff_minutes} max_workers={args.max_workers}")

    fb = FinBERT()  # CPU on CI

    # Gather prices (parallel)
    prices_list: list[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futs = {ex.submit(_safe_fetch_prices, t, args.start, args.end): t for t in tickers}
        for fut in tqdm(as_completed(futs), total=len(futs), desc="Prices"):
            t = futs[fut]
            try:
                p = fut.result()
                prices_list.append(p)
            except Exception as e:
                print(f"Price fetch failed for {t}: {e}")

    if not prices_list:
        raise RuntimeError("No prices fetched; cannot proceed.")

    prices = pd.concat(prices_list, ignore_index=True)
    prices["date"] = pd.to_datetime(prices["date"]).dt.tz_localize(None)
    prices = add_forward_returns(prices)

    # Gather & score news + earnings (parallel)
    def _collect_one(t: str) -> tuple[str, pd.DataFrame, pd.DataFrame]:
        # You can pass company name here if you have it; ticker alone still works
        news = fetch_news(t, args.start, args.end)
        earn = fetch_earnings_docs(t, args.start, args.end)

        # Score with FinBERT
        if not news.empty:
            news = _score_rows_inplace(fb, news, text_col="text", batch=args.batch)
        if not earn.empty:
            earn = _score_rows_inplace(fb, earn, text_col="text", batch=args.batch)

        return t, news, earn

    news_rows_list: list[pd.DataFrame] = []
    earn_rows_list: list[pd.DataFrame] = []

    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futs = {ex.submit(_collect_one, t): t for t in tickers}
        for fut in tqdm(as_completed(futs), total=len(futs), desc="News+Earnings"):
            t = futs[fut]
            try:
                _, n, e = fut.result()
                if not n.empty:
                    news_rows_list.append(n)
                if not e.empty:
                    earn_rows_list.append(e)
            except Exception as exn:
                print(f"Collect failed for {t}: {exn}")

    news_rows = pd.concat(news_rows_list, ignore_index=True) if news_rows_list else pd.DataFrame(
        columns=["ticker", "ts", "title", "url", "text", "S"]
    )
    earn_rows = pd.concat(earn_rows_list, ignore_index=True) if earn_rows_list else pd.DataFrame(
        columns=["ticker", "ts", "title", "url", "text", "S"]
    )

    # Daily aggregation
    d_news = daily_sentiment_from_rows(news_rows, kind="news", cutoff_minutes=args.cutoff_minutes) if not news_rows.empty else pd.DataFrame(columns=["date","ticker","S","news_count"])
    d_earn = daily_sentiment_from_rows(earn_rows, kind="earn", cutoff_minutes=args.cutoff_minutes) if not earn_rows.empty else pd.DataFrame(columns=["date","ticker","S","earn_count"])

    # Merge daily (sum S, sum counts)
    if d_news.empty and d_earn.empty:
        raise RuntimeError("No daily sentiment was generated (both news and earnings empty).")

    daily = None
    if not d_news.empty and not d_earn.empty:
        daily = (
            d_news.merge(d_earn, on=["date", "ticker"], how="outer", suffixes=("_news", "_earn"))
                 .fillna({"S_news": 0.0, "S_earn": 0.0, "news_count": 0, "earn_count": 0})
        )
        daily["S"] = daily["S_news"] + daily["S_earn"]
    elif not d_news.empty:
        daily = d_news.rename(columns={"S": "S_news"})
        daily["S_earn"] = 0.0
        daily["earn_count"] = 0
        daily["S"] = daily["S_news"]
    else:
        daily = d_earn.rename(columns={"S": "S_earn"})
        daily["S_news"] = 0.0
        daily["news_count"] = 0
        daily["S"] = daily["S_earn"]

    daily = daily[["date", "ticker", "S", "S_news", "S_earn", "news_count", "earn_count"]]
    daily["date"] = pd.to_datetime(daily["date"]).dt.tz_localize(None)

    # Build per-ticker JSON + index + portfolio
    wrote = 0
    tickers_with_any_news = set(news_rows["ticker"].unique()) if not news_rows.empty else set()
    tickers_nonzero_S = set(daily.loc[daily["S"].abs() > 0, "ticker"].unique())

    index_tickers = []
    for t in tqdm(tickers, desc="Write JSON"):
        p = prices.loc[prices["ticker"] == t, ["date", "ticker", "close", "ret_cc_1d"]].copy()
        d = daily.loc[daily["ticker"] == t, ["date", "ticker", "S", "S_news", "S_earn", "news_count", "earn_count"]].copy()
        top_news = news_rows.loc[news_rows["ticker"] == t, ["ts", "title", "url", "S"]].copy().sort_values("ts", ascending=False).head(15)

        if p.empty and d.empty and top_news.empty:
            continue

        obj = build_ticker_json(t, p, d, top_news)
        (Path(args.out) / f"{t}.json").write_text(obj, encoding="utf-8")
        index_tickers.append(t)
        wrote += 1

    write_index_files(index_tickers, Path(args.out))
    write_portfolio_json(daily, Path(args.out))

    # Diagnostics for CI logs
    print("=== Build diagnostics ===")
    print(f"Tickers listed: {len(tickers)}")
    print(f"Ticker JSON files: {wrote}")
    print(f"Tickers with any news: {len(tickers_with_any_news)}/{len(tickers)}")
    print(f"Tickers with non-zero sentiment S: {len(tickers_nonzero_S)}/{len(tickers)}")

    # Show a couple examples with |S|>0
    if not daily.empty:
        means = (
            daily.groupby("ticker")["S"]
                 .agg(lambda s: float(s.abs().mean()))
                 .sort_values(ascending=False)
        )
        top = means.head(5)
        if not top.empty:
            print("Sample tickers with mean|S| > 0:")
            for t, v in top.items():
                nz = int((daily[(daily["ticker"] == t) & (daily["S"].abs() > 0)]).shape[0])
                print(f"  {t}: mean|S|={v:.4f} (nz_points={nz})")


if __name__ == "__main__":
    main()
