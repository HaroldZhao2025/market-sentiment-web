# src/market_sentiment/cli/build_json.py
from __future__ import annotations
import argparse, json, os
from pathlib import Path
from tqdm import tqdm
import pandas as pd

from market_sentiment.prices import fetch_prices_yf
from market_sentiment.news import fetch_news_yf
from market_sentiment.finbert import FinBERT, score_texts
from market_sentiment.aggregate import (
    add_forward_returns,
    daily_sentiment_from_rows,
)
from market_sentiment.writers import (
    build_ticker_json,
    write_ticker_json,
    write_tickers_index,
    write_portfolio_json,
)

def _read_universe(csv_path: Path) -> list[str]:
    df = pd.read_csv(csv_path)
    cols = [c.lower() for c in df.columns]
    if "symbol" in cols:
        tickers = df["symbol"].astype(str).str.strip().str.upper().tolist()
    elif "ticker" in cols:
        tickers = df["ticker"].astype(str).str.strip().str.upper().tolist()
    else:
        # assume single-column CSV of symbols
        tickers = df.iloc[:, 0].astype(str).str.strip().str.upper().tolist()
    tickers = [t for t in tickers if t and t != "N/A"]
    return sorted(set(tickers))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", type=Path, required=True)
    ap.add_argument("--start", type=str, required=True)
    ap.add_argument("--end", type=str, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--batch", type=int, default=16)
    ap.add_argument("--max-workers", type=int, default=8)
    # IMPORTANT: default 0 == ALL. (No implicit 200 cap.)
    ap.add_argument("--max-tickers", type=int, default=0)
    args = ap.parse_args()

    outdir: Path = args.out
    outdir.mkdir(parents=True, exist_ok=True)

    tickers = _read_universe(args.universe)
    if args.max_tickers and args.max_tickers > 0:
        tickers = tickers[: args.max_tickers]

    # Fetch prices for ALL tickers (no cap)
    prices = fetch_prices_yf(tickers, args.start, args.end, max_workers=args.max_workers)
    if prices.empty:
        raise SystemExit("No prices fetched; cannot proceed.")
    prices = add_forward_returns(prices)

    # News -> rows: ['ticker','ts','title','url','text']
    all_news_rows: list[pd.DataFrame] = []
    for t in tqdm(tickers, desc="Fetch news", total=len(tickers)):
        try:
            df = fetch_news_yf(t, args.start, args.end)
            if df is not None and not df.empty:
                # enforce schema + ticker col
                df = df[["ts", "title", "url", "text"]].copy()
                df["ticker"] = t
                all_news_rows.append(df)
        except Exception:
            # keep going if one ticker fails
            continue

    news_rows = pd.concat(all_news_rows, ignore_index=True) if all_news_rows else pd.DataFrame(columns=["ts","title","url","text","ticker"])

    # Score with FinBERT if any texts exist
    if not news_rows.empty:
        fb = FinBERT()
        scores = score_texts(fb, news_rows["text"].tolist(), batch_size=args.batch)
        news_rows["score"] = scores
    else:
        news_rows["score"] = pd.Series(dtype=float)

    # Aggregate daily news sentiment per ticker/date
    d_news = daily_sentiment_from_rows(news_rows, kind="news", cutoff_minutes=5)

    # Combine daily frames (earnings optional; skip if none)
    # If you later add earnings rows dataframe named earn_rows with "score", do:
    # d_earn = daily_sentiment_from_rows(earn_rows, kind="earn", cutoff_minutes=5)
    # merged daily:
    daily = d_news.copy()
    if daily.empty:
        # keep schema so downstream merge doesn't crash
        daily = pd.DataFrame(columns=["date","ticker","S","S_news","S_earn","news_count","earn_count"])

    # Build per-ticker JSON (write empty JSON for tickers without data)
    # Also build portfolio from daily S and returns
    # Prepare quick lookup frames
    prices_by_t = prices.set_index(["ticker","date"]).sort_index()

    # write JSON per ticker
    written = set()
    for t in tqdm(tickers, desc="Write per-ticker JSON"):
        # per-ticker prices
        p = prices_by_t.loc[(t,), :].reset_index() if (t,) in prices_by_t.index.levels[0] or t in prices_by_t.index.get_level_values(0) else pd.DataFrame(columns=["date","close"])
        # per-ticker daily sentiment
        d = daily[daily["ticker"] == t].copy() if not daily.empty else pd.DataFrame(columns=["date","S"])
        # top news for ticker
        top = (news_rows[news_rows["ticker"] == t].copy() if not news_rows.empty else pd.DataFrame(columns=["ts","title","url","score"]))
        obj = build_ticker_json(t, p, d, top)
        write_ticker_json(outdir, t, obj)
        written.add(t)

    # Ensure minimal JSON for any ticker that somehow wasn’t written
    missing = [t for t in tickers if t not in written]
    for t in missing:
        obj = {"ticker": t, "series": [], "news": []}
        write_ticker_json(outdir, t, obj)

    # Write _tickers.json as the FULL universe (so Next prerenders every ticker)
    write_tickers_index(outdir, tickers)

    # Portfolio from daily S and forward returns (only where both exist)
    # Merge daily S back with returns
    if not daily.empty:
        # Normalize types
        daily2 = daily[["date","ticker","S"]].copy()
        daily2["date"] = pd.to_datetime(daily2["date"])
        with_rets = (
            prices[["date","ticker","ret_cc_1d"]]
            .merge(daily2, on=["date","ticker"], how="inner")
            .rename(columns={"ret_cc_1d":"y","S":"signal"})
        )
    else:
        with_rets = pd.DataFrame(columns=["date","ticker","y","signal"])

    write_portfolio_json(outdir, with_rets)

if __name__ == "__main__":
    main()
