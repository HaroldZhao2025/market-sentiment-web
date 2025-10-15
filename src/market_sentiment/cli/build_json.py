from __future__ import annotations
import argparse
from pathlib import Path
import sys
import numpy as np
import pandas as pd
from tqdm import tqdm

from market_sentiment.utils import ensure_dir, load_sp500_csv
from market_sentiment.prices import fetch_prices
from market_sentiment.news import fetch_news
from market_sentiment.transcripts import fetch_transcripts
from market_sentiment.finbert import FinBERT
from market_sentiment.aggregate import add_forward_returns, aggregate_daily_news, combine_news_earnings
from market_sentiment.earnings_sentiment import score_earnings_daily
from market_sentiment.writers import build_ticker_json, write_ticker_json, write_index_json, write_portfolio_json, write_earnings_json


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", type=Path, required=True)
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default=pd.Timestamp.utcnow().strftime("%Y-%m-%d"))
    ap.add_argument("--out", type=Path, default=Path("apps/web/public/data"))
    ap.add_argument("--cutoff", type=int, default=30)
    ap.add_argument("--batch", type=int, default=16)
    ap.add_argument("--limit", type=int, default=505)
    ap.add_argument("--w_news", type=float, default=1.0)
    ap.add_argument("--w_earn", type=float, default=1.5)
    args = ap.parse_args()

    tickers = load_sp500_csv(args.universe)[: args.limit]
    ensure_dir(args.out); ensure_dir(args.out / "earnings")

    # FinBERT (hard requirement)
    try:
        fb = FinBERT()
    except Exception as e:
        print("ERROR: FinBERT not available:", e, file=sys.stderr)
        sys.exit(1)

    summary_rows = []
    panel_parts = []

    for t in tqdm(tickers, desc="Build JSON"):
        # Prices + forward return
        prices = fetch_prices(t, args.start, args.end)
        if prices.empty:
            continue
        prices = add_forward_returns(prices)
        rets = prices[["date","ret_cc_1d"]].rename(columns={"ret_cc_1d":"y"}).copy()
        rets["ticker"] = t

        # News fetching + scoring (titles)
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

        # Earnings transcripts + scoring (long text)
        er = fetch_transcripts(t)
        if not er.empty:
            d_earn = score_earnings_daily(fb, er)
            write_earnings_json(t, er, args.out)  # raw events for the earnings page
        else:
            d_earn = pd.DataFrame(columns=["date","S_earn"])

        # Combine
        daily = combine_news_earnings(d_news, d_earn, ticker=t, w_news=args.w_news, w_earn=args.w_earn)

        # JSON per ticker
        obj = build_ticker_json(t, prices, daily, news)
        write_ticker_json(obj, args.out)

        # home index ranker
        if not daily.empty:
            last_total = float(daily["S_total"].iloc[-1])
        else:
            last_total = 0.0
        pred = float(np.tanh(last_total / 2.0) * 0.02)
        summary_rows.append({"ticker": t, "S_total": last_total, "predicted_return": pred})

        # portfolio panel on combined sentiment
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
