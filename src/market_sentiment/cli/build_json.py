from __future__ import annotations
import argparse, json
from pathlib import Path
from typing import List
import pandas as pd
from tqdm import tqdm

from market_sentiment.finbert import FinBERT
from market_sentiment.news import fetch_news
from market_sentiment.edgar import fetch_earnings_docs
from market_sentiment.prices import fetch_prices_yf
from market_sentiment.aggregate import (
    add_forward_returns, apply_cutoff_and_roll, daily_news_signal,
    daily_earnings_signal, combine_daily_signals, safe_merge_on_date_ticker,
)
from market_sentiment.writers import build_ticker_json, write_index, write_portfolio_json
from market_sentiment.portfolio import daily_long_short
from market_sentiment.universe import fetch_sp500

def _score_rows_fb(fb: FinBERT, rows: pd.DataFrame, text_col: str) -> pd.DataFrame:
    if rows.empty: return rows.assign(S_item=[])
    texts = rows[text_col].fillna(rows.get("title","")).astype(str).tolist()
    scores = fb.score_batch(texts, batch_size=16, max_length=128) or []
    s_item = [float(s["positive"]) - float(s["negative"]) for s in scores] if scores else [0.0]*len(texts)
    out = rows.copy(); out["S_item"] = s_item; return out

def _load_universe(path: Path | None) -> List[str]:
    if path is None or not Path(path).exists():
        df = fetch_sp500(); return sorted(df["Symbol"].astype(str).unique().tolist())
    df = pd.read_csv(path); cols=[c.lower() for c in df.columns]
    symcol = df.columns[cols.index("symbol")] if "symbol" in cols else (df.columns[cols.index("ticker")] if "ticker" in cols else df.columns[0])
    return sorted(df[symcol].astype(str).unique().tolist())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", type=Path, default=Path("data/sp500.csv"))
    ap.add_argument("--start", type=str, required=True)
    ap.add_argument("--end", type=str, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--batch", type=int, default=16)
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--cutoff", type=int, default=30)
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    tickers = _load_universe(args.universe)[: args.limit]
    fb = FinBERT()

    panels = []
    rets_list = []
    written = []

    for t in tqdm(tickers, desc="Build JSON"):
        try:
            prices = fetch_prices_yf(t, args.start, args.end)
            if prices.empty: continue

            raw_news = fetch_news(t, args.start, args.end)
            if not raw_news.empty:
                raw_news["ticker"]=t
                news_scored = _score_rows_fb(fb, raw_news, text_col="text")
                news_rolled = apply_cutoff_and_roll(news_scored, args.cutoff)
                news_daily = daily_news_signal(news_rolled[["ticker","effective_date","S_item"]])
            else:
                news_daily = pd.DataFrame(columns=["date","ticker","S_news","news_count"])

            earn_raw = fetch_earnings_docs(t, args.start, args.end)
            if not earn_raw.empty:
                earn_raw = earn_raw.assign(ticker=t)
                earn_scored = _score_rows_fb(fb, earn_raw, text_col="text")
                earn_daily = daily_earnings_signal(earn_scored[["ticker","ts","S_item"]])
            else:
                earn_daily = pd.DataFrame(columns=["date","ticker","S_earn","earn_count"])

            sig = combine_daily_signals(news_daily, earn_daily)
            rets = add_forward_returns(prices)

            panels.append(sig[["date","ticker","S"]].copy())
            rets_list.append(rets[["date","ticker","ret_cc_1d"]].copy())

            obj = build_ticker_json(t, prices, sig, raw_news)
            with open(args.out / f"{t}.json", "w") as f:
                json.dump(obj, f, separators=(",",":"), ensure_ascii=False)
            written.append(t)
        except Exception as ex:
            print(f"[WARN] {t}: {ex}")

    # index + portfolio
    if written: write_index(args.out, written)
    if panels and rets_list:
        daily = pd.concat(panels, ignore_index=True)
        rets  = pd.concat(rets_list, ignore_index=True)
        joined = safe_merge_on_date_ticker(daily, rets, how="left").rename(columns={"ret_cc_1d":"y","S":"x"})
        pnl = daily_long_short(joined[["date","ticker","x","y"]], 0.9, 0.1)
        # latest cross-section for Top/Bottom lists
        latest = joined[joined["date"]==joined["date"].max()].sort_values("x")
        bot = latest.head(10)["ticker"].tolist()
        top = latest.tail(10)["ticker"].tolist()[::-1]
        write_portfolio_json(args.out, pnl, top, bot)

if __name__ == "__main__":
    main()
