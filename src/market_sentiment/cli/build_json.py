# src/market_sentiment/cli/build_json.py
from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd
from tqdm import tqdm

from market_sentiment.finbert import FinBERT          # you provided this
from market_sentiment.news import news_yfinance       # you provided this (free Yahoo)
from market_sentiment.edgar import fetch_earnings_docs# you provided this (free EDGAR)
from market_sentiment.prices import fetch_panel_yf
from market_sentiment.aggregate import add_forward_returns, aggregate_daily
from market_sentiment.writers import build_ticker_json, write_json, write_portfolio
from market_sentiment.portfolio import daily_long_short

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", type=str, required=True, help="CSV with a 'Symbol' or 'ticker' column")
    ap.add_argument("--start", type=str, required=True)
    ap.add_argument("--end", type=str, required=True)
    ap.add_argument("--out", type=str, required=True)
    ap.add_argument("--batch", type=int, default=16)
    ap.add_argument("--limit", type=int, default=200, help="Max tickers to process for CI speed")
    ap.add_argument("--cutoff", type=int, default=30, help="Minutes before close to roll to T+1")
    return ap.parse_args()

def _read_universe(path: str, limit: int | None) -> list[str]:
    df = pd.read_csv(path)
    tick_col = "ticker" if "ticker" in df.columns else "Symbol" if "Symbol" in df.columns else None
    if tick_col is None:
        raise ValueError("Universe CSV must have a 'ticker' or 'Symbol' column.")
    ticks = [str(x).upper() for x in df[tick_col].tolist()]
    if limit:
        ticks = ticks[:limit]
    # Ensure MSFT for SSG demo if universe is tiny
    if "MSFT" not in ticks:
        ticks = ["MSFT"] + ticks
    # Deduplicate preserving order
    seen=set(); out=[]
    for t in ticks:
        if t not in seen:
            out.append(t); seen.add(t)
    return out

def _score_texts(fb: FinBERT, texts: list[str]) -> list[float]:
    """
    Map texts -> signed sentiment score in [-1,1]
    """
    if not texts:
        return []
    preds = fb.predict(texts)  # you implemented this; should return list of dicts
    # Expect each item like {'label': 'positive'|'negative'|'neutral', 'score': float}
    signed=[]
    for p in preds:
        lab = (p.get("label") or "").lower()
        sc  = float(p.get("score", 0.0))
        if lab.startswith("pos"): signed.append(+sc)
        elif lab.startswith("neg"): signed.append(-sc)
        else: signed.append(0.0)
    return signed

def main():
    args = parse_args()
    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)

    tickers = _read_universe(args.universe, args.limit)
    # 1) Prices
    prices = fetch_panel_yf(tickers, args.start, args.end)
    prices = add_forward_returns(prices)

    # 2) Init FinBERT once
    fb = FinBERT()

    # Hold all per-ticker frames for portfolio step
    all_daily = []
    per_ticker_news = []
    per_ticker_earn = []

    for t in tqdm(tickers, desc="Build JSON"):
        p = prices[prices["ticker"] == t]
        if p.empty:
            continue

        # --- NEWS (Yahoo free) ---
        try:
            nd = news_yfinance(t)  # must return ['ts','title','url','text'] minimally
        except Exception:
            nd = pd.DataFrame(columns=["ts","title","url","text"])
        if nd is None: nd = pd.DataFrame(columns=["ts","title","url","text"])
        # filter to window & drop empties
        if not nd.empty:
            nd["ts"] = pd.to_datetime(nd["ts"], utc=True, errors="coerce")
            nd = nd.dropna(subset=["ts","text"])
            nd = nd[(nd["ts"] >= pd.to_datetime(args.start, utc=True)) &
                    (nd["ts"] <= pd.to_datetime(args.end,   utc=True))]
        if not nd.empty:
            nd["ticker"] = t
            # score with FinBERT
            nd["S"] = _score_texts(fb, nd["text"].astype(str).tolist())
        # --- EARNINGS (EDGAR free) ---
        try:
            er = fetch_earnings_docs(t, args.start, args.end)  # ['ts','title','url','text']
        except Exception:
            er = pd.DataFrame(columns=["ts","title","url","text"])
        if er is None: er = pd.DataFrame(columns=["ts","title","url","text"])
        if not er.empty:
            er["ts"] = pd.to_datetime(er["ts"], utc=True, errors="coerce")
            er = er.dropna(subset=["ts","text"])
            er["ticker"] = t
            er["S"] = _score_texts(fb, er["text"].astype(str).tolist())

        # --- DAILY AGG ---
        daily = aggregate_daily(nd if not nd.empty else None,
                                er if not er.empty else None,
                                cutoff_minutes_before_close=args.cutoff)
        if not daily.empty:
            daily["ticker"] = t
            all_daily.append(daily)
        if not nd.empty:
            per_ticker_news.append(nd[["ts","ticker","title","url","S"]])
        if not er.empty:
            per_ticker_earn.append(er[["ts","ticker","title","url","S"]])

    if not all_daily:
        # fail hard so CI never deploys an empty site
        raise SystemExit("No daily sentiment was generated. Aborting.")

    panel = pd.concat(all_daily, ignore_index=True)
    news_all = pd.concat(per_ticker_news, ignore_index=True) if per_ticker_news else pd.DataFrame(columns=["ts","ticker","title","url","S"])
    earn_all = pd.concat(per_ticker_earn, ignore_index=True) if per_ticker_earn else pd.DataFrame(columns=["ts","ticker","title","url","S"])

    # 3) Write one JSON per ticker
    saved = []
    for t in sorted(panel["ticker"].unique()):
        obj = build_ticker_json(t, prices, panel, news_all, earn_all)
        if len(obj.get("series", [])) == 0:
            continue
        write_json(outdir / f"{t}.json", obj)
        saved.append(t)

    # 4) Tickers index
    write_json(outdir / "_tickers.json", saved)

    # 5) Portfolio (long top decile, short bottom decile using next-day returns)
    rets = prices[["date","ticker","ret_cc_1d"]].copy()
    joined = pd.merge(panel[["date","ticker","S_ew"]], rets, on=["date","ticker"], how="left")
    port = daily_long_short(joined, 0.9, 0.1, score_col="S_ew", ret_col="ret_cc_1d")
    write_portfolio(outdir / "portfolio.json", port)

    # Sanity guard
    if len(saved) < 5:
        raise SystemExit(f"Only {len(saved)} tickers generated. Aborting to avoid empty site.")

if __name__ == "__main__":
    main()
