# src/market_sentiment/cli/build_json.py
from __future__ import annotations
import argparse, sys
from pathlib import Path
import pandas as pd
from tqdm import tqdm

from market_sentiment.prices import fetch_prices_yf
from market_sentiment.news import fetch_news
from market_sentiment.edgar import fetch_earnings_docs
from market_sentiment.finbert import FinBERT
from market_sentiment.aggregate import add_forward_returns, daily_sentiment_from_rows, combine_news_earn
from market_sentiment.portfolio import daily_long_short
from market_sentiment.writers import build_ticker_json, write_json, write_tickers

def _read_universe(path: str) -> list[str]:
    df = pd.read_csv(path)
    for c in ("symbol","Symbol","ticker","Ticker"):
        if c in df.columns:
            return [str(x).upper() for x in df[c].dropna().tolist()]
    # fallback: treat first column as ticker
    return [str(x).upper() for x in df.iloc[:,0].dropna().tolist()]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--batch", type=int, default=16)
    args = ap.parse_args()

    out_dir = Path(args.out)
    tickers = _read_universe(args.universe)[: args.limit]

    fb = FinBERT(batch_size=args.batch)

    all_prices = []
    all_daily = []
    all_news_rows = []

    for t in tqdm(tickers, desc="Build JSON"):
        # Prices
        p = fetch_prices_yf(t, args.start, args.end)
        if p.empty:
            continue
        all_prices.append(p)

        # NEWS (multi-source)
        n = fetch_news(t, args.start, args.end)
        # EARNINGS via EDGAR (may be empty if SEC blocks)
        try:
            er = fetch_earnings_docs(t, args.start, args.end)
        except Exception:
            er = pd.DataFrame(columns=["ts","title","url","text"])

        # tag ticker
        if not n.empty: n["ticker"] = t
        if not er.empty: er["ticker"] = t

        # If everything empty, still produce price-only JSON later
        scored_news = pd.DataFrame()
        scored_er = pd.DataFrame()

        # Score NEWS with FinBERT
        if not n.empty:
            sn = fb.score(n["text"].tolist())
            n = n.reset_index(drop=True).join(sn.drop(columns=["text"]))
            scored_news = n

        # Score EARNINGS with FinBERT
        if not er.empty:
            se = fb.score(er["text"].tolist())
            er = er.reset_index(drop=True).join(se.drop(columns=["text"]))
            scored_er = er

        # Heuristic fallback: if EDGAR blocked, try to classify “earnings transcript” articles from news
        if scored_er.empty and not scored_news.empty:
            mask = scored_news["title"].str.contains(r"earnings", case=False, na=False) & \
                   scored_news["title"].str.contains(r"transcript", case=False, na=False)
            tmp = scored_news[mask].copy()
            if not tmp.empty:
                tmp = tmp.rename(columns={"ts":"ts","title":"title","url":"url"})
                scored_er = tmp[["ts","title","url","text","p_pos","p_neg","p_neu","s","label","conf","ticker"]]

        # Aggregate daily
        d_news = daily_sentiment_from_rows(scored_news, "news")
        d_earn = daily_sentiment_from_rows(scored_er, "earn")
        d = combine_news_earn(d_news, d_earn)
        all_daily.append(d)

        # Keep some news rows for UI
        top_news = scored_news.copy() if not scored_news.empty else pd.DataFrame(columns=["ts","title","url","s","source","ticker"])

        # Build ticker JSON object and write
        obj = build_ticker_json(t, p, d, top_news)
        write_json(out_dir, t, obj)

    if not all_prices:
        print("No prices fetched; nothing to write."); sys.exit(0)

    prices = pd.concat(all_prices, ignore_index=True)
    prices = add_forward_returns(prices)

    _daily = [d for d in all_daily if d is not None and not d.empty]
    daily = pd.concat(_daily, ignore_index=True) if _daily else pd.DataFrame(
        columns=["date","ticker","S","S_news","S_earn","news_count","earn_count"]
    )
    
    # panel for portfolio
    panel = prices[["date","ticker","close","ret_cc_1d"]].merge(
        daily[["date","ticker","S"]], on=["date","ticker"], how="left"
    )
    panel["S"] = panel["S"].fillna(0.0)

    # portfolio json
    pnl = daily_long_short(panel, 0.9, 0.1)
    portfolio = {
        "series": [{"date": str(r["date"]).split(" ")[0], "ret": float(r["ret"]), "cum": float(r["cum"])} for _, r in pnl.iterrows()],
        "meta": {"long_q": 0.9, "short_q": 0.1}
    }
    write_json(out_dir, "portfolio", portfolio)

    # tickers list for web
    write_tickers(out_dir, sorted(set(prices["ticker"].unique().tolist())))

if __name__ == "__main__":
    main()
