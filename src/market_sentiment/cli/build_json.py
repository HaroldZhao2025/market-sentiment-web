from __future__ import annotations
import argparse, json, os
from pathlib import Path
from typing import List, Dict
import pandas as pd
from tqdm import tqdm

from market_sentiment.finbert import FinBERT
from market_sentiment.news import fetch_news   # your free Yahoo/News loader
from market_sentiment.edgar import fetch_earnings_docs
from market_sentiment.prices import fetch_prices_yf
from market_sentiment.aggregate import (
    add_forward_returns,
    apply_cutoff_and_roll,
    daily_news_signal,
    daily_earnings_signal,
    combine_daily_signals,
    safe_merge_on_date_ticker,
)
from market_sentiment.writers import build_ticker_json
from market_sentiment.universe import fetch_sp500


def _score_rows_fb(fb: FinBERT, rows: pd.DataFrame, text_col: str) -> pd.DataFrame:
    """Score rows with FinBERT; returns rows with S_item column (pos - neg)."""
    if rows.empty:
        return rows.assign(S_item=[])
    texts = rows[text_col].fillna("").astype(str).tolist()
    scores = fb.score_batch(texts, batch_size=16, max_length=128)
    if not scores:
        return rows.assign(S_item=0.0)
    s_item = [float(s["positive"]) - float(s["negative"]) for s in scores]
    out = rows.copy()
    out["S_item"] = s_item
    return out


def _load_universe(path: Path | None) -> List[str]:
    if path is None or not Path(path).exists():
        df = fetch_sp500()
        return sorted(df["Symbol"].astype(str).unique().tolist())
    df = pd.read_csv(path)
    cols = [c.lower() for c in df.columns]
    if "symbol" in cols:
        symcol = df.columns[cols.index("symbol")]
    elif "ticker" in cols:
        symcol = df.columns[cols.index("ticker")]
    else:
        symcol = df.columns[0]
    return sorted(df[symcol].astype(str).unique().tolist())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", type=Path, default=Path("data/sp500.csv"))
    ap.add_argument("--start", type=str, required=True)
    ap.add_argument("--end", type=str, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--batch", type=int, default=16)
    ap.add_argument("--limit", type=int, default=200, help="max tickers to process (SSG footprint)")
    ap.add_argument("--cutoff", type=int, default=30, help="minutes before close to roll to T+1")
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)

    tickers = _load_universe(args.universe)[: args.limit]
    fb = FinBERT()

    # For building a cross-sectional panel later
    all_daily: List[pd.DataFrame] = []
    all_rets:  List[pd.DataFrame] = []

    for t in tqdm(tickers, desc="Build JSON"):
        try:
            # 1) Prices
            prices = fetch_prices_yf(t, args.start, args.end)  # must yield ['date','ticker','open','close',...]
            if prices.empty:
                continue

            # 2) News (free sources; your news.fetch function)
            raw_news = fetch_news(t, args.start, args.end)  # ['ts','title','url','text','ticker']
            if not raw_news.empty:
                raw_news["ticker"] = t
                news_scored = _score_rows_fb(fb, raw_news.assign(text=raw_news["text"].fillna(raw_news["title"])),
                                             text_col="text")
                news_rolled = apply_cutoff_and_roll(news_scored, args.cutoff)
                news_daily = daily_news_signal(news_rolled[["ticker", "effective_date", "S_item"]])
            else:
                news_daily = pd.DataFrame(columns=["date","ticker","S_news","news_count"])

            # 3) Earnings (EDGAR free)
            earn_raw = fetch_earnings_docs(t, args.start, args.end)  # ['ts','title','url','text']
            if not earn_raw.empty:
                earn_raw = earn_raw.assign(ticker=t)
                earn_scored = _score_rows_fb(fb, earn_raw, text_col="text")
                earn_daily = daily_earnings_signal(earn_scored[["ticker", "ts", "S_item"]])
            else:
                earn_daily = pd.DataFrame(columns=["date","ticker","S_earn","earn_count"])

            # 4) Signals (combined)
            sig = combine_daily_signals(news_daily, earn_daily)

            # 5) Returns
            rets = add_forward_returns(prices)  # ['date','ticker','ret_cc_1d','ret_oc_1d']

            # 6) Keep for portfolio panel
            all_daily.append(sig[["date","ticker","S"]].copy())
            all_rets.append(rets[["date","ticker","ret_cc_1d"]].copy())

            # 7) Write per-ticker JSON for the site
            obj = build_ticker_json(t, prices, sig, raw_news)  # writer handles serialization
            with open(args.out / f"{t}.json", "w") as f:
                json.dump(obj, f, separators=(",", ":"), ensure_ascii=False)
        except Exception as ex:
            # Continue with other tickers
            print(f"[WARN] {t}: {ex}")

    # ---- Portfolio panel (long/short outside this CLI or inside if you prefer) ----
    if all_daily and all_rets:
        daily = pd.concat(all_daily, ignore_index=True)
        rets  = pd.concat(all_rets,  ignore_index=True)
        joined = safe_merge_on_date_ticker(daily, rets, how="left").rename(columns={"ret_cc_1d":"y", "S":"x"})
        # Save cross-section panel for debugging (optional)
        joined.to_csv(args.out / "_panel.csv", index=False)


if __name__ == "__main__":
    main()
