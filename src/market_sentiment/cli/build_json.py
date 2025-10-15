from __future__ import annotations
import argparse
from pathlib import Path
import sys
import pandas as pd
import numpy as np
from tqdm import tqdm

from market_sentiment.utils import ensure_dir, load_sp500_csv
from market_sentiment.prices import fetch_prices
from market_sentiment.news import fetch_news
from market_sentiment.transcripts import fetch_transcripts
from market_sentiment.sentiment import lexicon_score
from market_sentiment.aggregate import (
    apply_cutoff_and_roll,
    add_forward_returns,
    aggregate_daily,
)
from market_sentiment.writers import (
    build_ticker_json,
    write_ticker_json,
    write_index_json,
    write_portfolio_json,
    write_earnings_json,
)
from market_sentiment.portfolio import daily_long_short


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", type=Path, required=True)
    ap.add_argument("--start", default="2023-01-01")
    ap.add_argument("--end", default="2023-12-31")
    ap.add_argument("--out", type=Path, default=Path("apps/web/public/data"))
    ap.add_argument("--cutoff", type=int, default=30)
    ap.add_argument("--sentiment", choices=["lexicon", "finbert"], default="lexicon")
    ap.add_argument("--batch", type=int, default=32, help="FinBERT batch size")
    ap.add_argument("--limit", type=int, default=505, help="limit universe size")
    args = ap.parse_args()

    tickers = load_sp500_csv(args.universe)[: args.limit]
    ensure_dir(args.out)

    # Lazy import FinBERT only if requested; otherwise keep CI lean
    fb = None
    use_finbert = args.sentiment == "finbert"
    if use_finbert:
        try:
            from market_sentiment.finbert import FinBERT  # noqa: WPS433
            fb = FinBERT()
        except Exception as e:
            print(
                f"Warning: FinBERT unavailable ({e!r}). Falling back to lexicon.",
                file=sys.stderr,
            )
            use_finbert = False
            fb = None

    summary_rows: list[dict] = []
    panel_parts: list[pd.DataFrame] = []  # for portfolio: per-day per-ticker scores + returns

    for t in tqdm(tickers, desc="Build JSON"):
        # 1) Prices + forward returns (y on T for T+1 move)
        prices = fetch_prices(t, args.start, args.end)
        if prices.empty:
            continue
        prices = add_forward_returns(prices)
        rets = prices[["date", "ret_cc_1d"]].rename(columns={"ret_cc_1d": "y"}).copy()
        rets["ticker"] = t

        # 2) News
        news = fetch_news(t, args.start, args.end)
        if news.empty:
            news = pd.DataFrame(columns=["ticker", "ts", "source", "title", "url"])

        # 3) Sentiment scoring
        if use_finbert and len(news) > 0:
            probs = fb.score_batch(news["title"].fillna("").tolist(), batch_size=args.batch)
            scored = pd.DataFrame(probs).rename(
                columns={"positive": "pos", "negative": "neg", "neutral": "neu"}
            )
            scored.insert(0, "ticker", t)
            scored.insert(1, "ts", news["ts"].values)
            scored.insert(2, "source", news["source"].values)
            scored.insert(3, "title", news["title"].values)
            scored.insert(4, "url", news["url"].values)
        else:
            rows = []
            for _, r in news.iterrows():
                pos, neg, neu, conf = lexicon_score(r.get("title", ""))
                rows.append((t, r["ts"], r["source"], r["title"], r["url"], pos, neg, neu, conf))
            scored = pd.DataFrame(
                rows,
                columns=["ticker", "ts", "source", "title", "url", "pos", "neg", "neu", "conf"],
            )

        # 4) Daily aggregation -> S(t, d)
        if scored.empty:
            daily = pd.DataFrame({"date": [], "ticker": [], "S": []})
        else:
            scored = apply_cutoff_and_roll(scored, args.cutoff)
            daily = aggregate_daily(scored)

        # 5) Per-ticker JSON for the site
        obj = build_ticker_json(t, prices, daily, news)
        write_ticker_json(obj, args.out)

        # 6) Earnings transcripts JSON
        er = fetch_transcripts(t)
        if not er.empty:
            write_earnings_json(t, er, args.out)

        # 7) Latest summary for home page
        last_s = daily[daily["ticker"] == t]["S"].tail(1)
        S = float(last_s.values[0]) if len(last_s) > 0 else 0.0
        pred_last = float(np.tanh(S / 5.0) * 0.01)
        summary_rows.append({"ticker": t, "S": S, "predicted_return": pred_last})

        # 8) Portfolio panel: merge daily scores with same-day forward return y
        if not daily.empty:
            sig = daily[["date", "ticker", "S"]].copy()
            sig["score"] = np.tanh(sig["S"] / 5.0) * 0.01
            joined = sig.merge(rets, on=["date", "ticker"], how="left")
            panel_parts.append(joined[["date", "ticker", "score", "y"]])

    # Overview JSON
    idx = pd.DataFrame(summary_rows).sort_values("predicted_return", ascending=False)
    write_index_json(idx, args.out)

    # Portfolio JSON (proper timeseries)
    if len(panel_parts) > 0:
        panel = pd.concat(panel_parts, ignore_index=True)
        panel = panel.dropna(subset=["y"])
        pnl = daily_long_short(panel, 0.9, 0.1)  # long top decile, short bottom decile
        if not pnl.empty:
            write_portfolio_json(pnl, args.out)

    print("Done ->", args.out)


if __name__ == "__main__":
    main()
