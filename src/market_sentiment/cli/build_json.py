from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
from tqdm import tqdm

from market_sentiment.utils import ensure_dir, load_sp500_csv
from market_sentiment.prices import fetch_prices
from market_sentiment.news import fetch_news
from market_sentiment.edgar import fetch_earnings_docs
from market_sentiment.finbert import FinBERT
from market_sentiment.aggregate import add_forward_returns, aggregate_daily_news, combine_news_earnings
from market_sentiment.earnings_sentiment import score_earnings_daily
from market_sentiment.writers import build_ticker_json, write_ticker_json, write_index_json, write_portfolio_json

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", type=Path, required=True)
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default=pd.Timestamp.utcnow().strftime("%Y-%m-%d"))
    ap.add_argument("--out", type=Path, default=Path("apps/web/public/data"))
    ap.add_argument("--batch", type=int, default=16)
    ap.add_argument("--limit", type=int, default=505)
    ap.add_argument("--w_news", type=float, default=1.0)
    ap.add_argument("--w_earn", type=float, default=1.5)
    args = ap.parse_args()

    tickers = load_sp500_csv(args.universe)[: args.limit]
    ensure_dir(args.out); ensure_dir(args.out / "earnings")

    fb = FinBERT()
    summary_rows = []
    panel_parts = []

    for t in tqdm(tickers, desc="Build JSON"):
        # prices
        prices = fetch_prices(t, args.start, args.end)
        if prices.empty:
            continue
        prices = add_forward_returns(prices)
        rets = prices[["date","ret_cc_1d"]].rename(columns={"ret_cc_1d":"y"}).copy()
        rets["ticker"] = t

        # news (free)
        news = fetch_news(t, args.start, args.end)
        if not news.empty:
            probs = fb.score_batch(news["title"].fillna("").tolist(), batch_size=args.batch, max_length=96)
            scored = pd.DataFrame(probs).rename(
                columns={"positive":"pos","negative":"neg","neutral":"neu","confidence":"conf"}
            )
            scored.insert(0, "ticker", t)
            scored.insert(1, "ts", news["ts"].values)
            scored.insert(2, "source", news["source"].values)
            scored.insert(3, "title", news["title"].values)
            scored.insert(4, "url", news["url"].values)
            d_news = aggregate_daily_news(scored)
        else:
            d_news = pd.DataFrame(columns=["date","ticker","S_news","news_count"])

        # earnings (free, EDGAR)
        docs = fetch_earnings_docs(t, args.start, args.end)
        d_earn = score_earnings_daily(fb, docs) if not docs.empty else pd.DataFrame(columns=["date","S_earn"])

        # combine signals
        daily = combine_news_earnings(d_news, d_earn, ticker=t, w_news=args.w_news, w_earn=args.w_earn)

        # per-ticker JSON
        obj = build_ticker_json(t, prices, daily, news, earnings_events=docs)
        write_ticker_json(obj, args.out)

        # index + panel
        last_total = float(daily["S_total"].iloc[-1]) if not daily.empty else 0.0
        pred = float(np.tanh((daily["S_total"].rolling(7, min_periods=1).mean().iloc[-1] if not daily.empty else 0.0)/2.0) * 0.02)
        summary_rows.append({"ticker": t, "S_total": last_total, "predicted_return": pred})

        if not daily.empty:
            sig = daily[["date","ticker","S_total"]].copy()
            sig["score"] = np.tanh(sig["S_total"] / 2.0) * 0.02
            joined = sig.merge(rets, on=["date","ticker"], how="left")
            panel_parts.append(joined[["date","ticker","score","y"]])

    # index.json
    idx = pd.DataFrame(summary_rows).sort_values("predicted_return", ascending=False)
    write_index_json(idx, args.out)

    # portfolio.json
    if len(panel_parts) > 0:
        panel = pd.concat(panel_parts, ignore_index=True).dropna(subset=["y"])
        from market_sentiment.portfolio import daily_long_short
        pnl = daily_long_short(panel, 0.9, 0.1)
        if not pnl.empty:
            write_portfolio_json(pnl, args.out)

    print("Done ->", args.out)

if __name__ == "__main__":
    main()
