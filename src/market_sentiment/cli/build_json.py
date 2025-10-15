# src/market_sentiment/cli/build_json.py
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
from tqdm import tqdm

from market_sentiment.finbert import FinBERT
from market_sentiment.news import news_yfinance
from market_sentiment.edgar import fetch_earnings_docs
from market_sentiment.prices import fetch_panel_yf, add_forward_returns
from market_sentiment.aggregate import aggregate_daily
from market_sentiment.writers import build_ticker_json, write_json, write_portfolio
from market_sentiment.portfolio import daily_long_short

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", required=True, help="CSV with 'ticker' or 'Symbol' column")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--batch", type=int, default=16)
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--cutoff", type=int, default=30)
    return ap.parse_args()

def _read_universe(path: str, limit: int | None) -> list[str]:
    df = pd.read_csv(path)
    col = "ticker" if "ticker" in df.columns else "Symbol" if "Symbol" in df.columns else None
    if col is None:
        raise ValueError("Universe CSV must have 'ticker' or 'Symbol'")
    ticks = [str(x).upper() for x in df[col].tolist()]
    if limit:
        ticks = ticks[:limit]
    # ensure some high-news names are present
    seeds = ["MSFT","AAPL","GOOGL","AMZN","META"]
    for s in reversed(seeds):
        if s in ticks:
            ticks.remove(s)
            ticks.insert(0, s)
        else:
            ticks.insert(0, s)
    seen=set(); out=[]
    for t in ticks:
        if t not in seen:
            out.append(t); seen.add(t)
    return out

def _signed_scores_from_preds(preds) -> list[float]:
    out=[]
    for p in preds:
        lab = (p.get("label") or "").lower()
        sc  = float(p.get("score", 0.0))
        out.append(sc if lab.startswith("pos") else (-sc if lab.startswith("neg") else 0.0))
    return out

def main():
    a = parse_args()
    outdir = Path(a.out); outdir.mkdir(parents=True, exist_ok=True)
    tickers = _read_universe(a.universe, a.limit)

    # 1) Prices (batch)
    prices = fetch_panel_yf(tickers, a.start, a.end)
    prices = add_forward_returns(prices)
    if prices.empty:
        # Fail clearly here so we know to investigate networking / Yahoo
        raise SystemExit("Prices were empty from yfinance. Check network or date window.")

    # 2) FinBERT once
    fb = FinBERT()

    all_daily = []
    news_all = []
    earn_all = []

    for t in tqdm(tickers, desc="Build JSON"):
        p = prices[prices["ticker"] == t]
        if p.empty:
            continue

        # NEWS (free, Yahoo)
        nd = news_yfinance(t, a.start, a.end)
        if not nd.empty:
            nd["ticker"] = t
            preds = fb.predict(nd["text"].astype(str).tolist())
            nd["S"] = _signed_scores_from_preds(preds)
            news_all.append(nd[["ts","ticker","title","url","S"]])

        # EARNINGS (free, EDGAR)
        er = fetch_earnings_docs(t, a.start, a.end)
        if not er.empty:
            er["ticker"] = t
            preds = fb.predict(er["text"].astype(str).tolist())
            er["S"] = _signed_scores_from_preds(preds)
            earn_all.append(er[["ts","ticker","title","url","S"]])

        daily = aggregate_daily(nd if not nd.empty else None,
                                er if not er.empty else None,
                                cutoff_minutes_before_close=a.cutoff)
        if daily.empty:
            # emit zeros aligned to price dates to keep UI populated
            z = p[["date"]].copy(); z["ticker"] = t
            for col in ["S_news","news_count","S_earn","earn_count","S_total","S_ew"]:
                z[col] = 0.0 if col.startswith("S_") else 0
            daily = z
        all_daily.append(daily)

        obj = build_ticker_json(t, prices, daily, nd if not nd.empty else None, er if not er.empty else None)
        if len(obj.get("series", [])) > 0:
            write_json(outdir / f"{t}.json", obj)

    # Index & Portfolio
    saved = sorted({p["ticker"] for p in prices[prices["date"].notna()]})  # use price presence as proxy
    write_json(outdir / "_tickers.json", saved)

    panel = pd.concat(all_daily, ignore_index=True)
    rets = prices[["date","ticker","ret_cc_1d"]]
    joined = pd.merge(panel[["date","ticker","S_ew"]], rets, on=["date","ticker"], how="left")
    port = daily_long_short(joined, 0.9, 0.1, score_col="S_ew", ret_col="ret_cc_1d")
    write_portfolio(outdir / "portfolio.json", port)

if __name__ == "__main__":
    main()
