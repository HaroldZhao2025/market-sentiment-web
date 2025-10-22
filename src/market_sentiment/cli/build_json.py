# src/market_sentiment/cli/build_json.py
from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple

import pandas as pd

from market_sentiment.aggregate import (
    add_forward_returns,
    daily_sentiment_from_rows,
    join_and_fill_daily,
    _ensure_date_dtype,
)
from market_sentiment.finbert import FinBERT
from market_sentiment.news import _prov_finnhub, _prov_yfinance
from market_sentiment.prices import fetch_prices_yf
from market_sentiment.writers import write_outputs


# ----------------------------
# FinBERT scoring
# ----------------------------

def _score_texts(fb: FinBERT, texts: List[str], batch: int) -> List[float]:
    try:
        return fb.score(texts, batch=batch)
    except TypeError:
        try:
            return fb.score(texts, batch_size=batch)
        except TypeError:
            return fb.score(texts)

def _score_rows_inplace(
    fb: Optional[FinBERT], df: pd.DataFrame, text_col: str, batch: int
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", text_col, "S"])
    out = df.copy()
    if fb is None:
        out["S"] = 0.0
        return out
    texts = out[text_col].astype(str).fillna("").tolist()
    if not texts:
        out["S"] = 0.0
        return out
    try:
        scores = _score_texts(fb, texts, batch=batch)
        out["S"] = pd.to_numeric(pd.Series(scores), errors="coerce").fillna(0.0)
    except Exception:
        out["S"] = 0.0
    out["S"] = out["S"].astype(float).round(4)
    return out


# ----------------------------
# Prices
# ----------------------------

def _fetch_all_prices(tickers: List[str], start: str, end: str, max_workers: int) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = []
        for t in tickers:
            futs.append(ex.submit(fetch_prices_yf, t, start, end))
            time.sleep(0.12)  # soften YF rate-limits
        for f in as_completed(futs):
            try:
                df = f.result()
                if df is not None and len(df) > 0:
                    rows.append(df)
            except Exception:
                pass
    if not rows:
        return pd.DataFrame(columns=["date", "ticker", "open", "close"])
    prices = pd.concat(rows, ignore_index=True)
    prices = _ensure_date_dtype(prices, "date")
    prices = add_forward_returns(prices)
    return prices


# ----------------------------
# Summary util
# ----------------------------

def _summarize_from_files(out_dir: str) -> Tuple[List[str], int, int, int]:
    try:
        tickers = json.load(open(f"{out_dir}/_tickers.json", "r", encoding="utf-8"))
    except Exception:
        tickers = []
    have_files = with_news = with_nonzero_s = 0
    for t in tickers:
        f = f"{out_dir}/ticker/{t}.json"
        try:
            obj = json.load(open(f, "r", encoding="utf-8"))
        except Exception:
            continue
        have_files += 1
        if obj.get("news"):
            with_news += 1
        S = obj.get("S") or obj.get("sentiment") or []
        try:
            if any(abs(float(x or 0.0)) > 1e-12 for x in S):
                with_nonzero_s += 1
        except Exception:
            pass
    return tickers, have_files, with_news, with_nonzero_s


# ----------------------------
# Main
# ----------------------------

def main():
    p = argparse.ArgumentParser("Build JSON artifacts for the web app")
    p.add_argument("--universe", required=True, help="CSV with 'ticker' column (first col accepted)")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--out", required=True, help="Output dir: apps/web/public/data")
    p.add_argument("--batch", type=int, default=16)
    p.add_argument("--cutoff-minutes", type=int, default=5)
    p.add_argument("--max-tickers", type=int, default=0)
    p.add_argument("--max-workers", type=int, default=8)
    p.add_argument("--news-per-provider", type=int, default=240,
                   help="For yfinance: count=; Finnhub goes day-by-day.")
    a = p.parse_args()

    # Universe
    uni = pd.read_csv(a.universe)
    if "ticker" not in uni.columns:
        uni = uni.rename(columns={uni.columns[0]: "ticker"})
    tickers = sorted(str(x).strip().upper() for x in uni["ticker"].dropna().unique().tolist())
    if a.max_tickers and a.max_tickers > 0:
        tickers = tickers[: a.max_tickers]

    print(f"Build JSON for {len(tickers)} tickers")

    # Prices
    prices = _fetch_all_prices(tickers, a.start, a.end, max_workers=a.max_workers)
    if prices is None or prices.empty:
        raise RuntimeError("No prices downloaded.")
    print(f"  ✓ Prices for {prices['ticker'].nunique()} tickers, rows={len(prices)}")

    # FinBERT
    try:
        fb = FinBERT()
    except Exception:
        fb = None

    # News (Finnhub day-by-day + yfinance count=240)
    news_all: List[pd.DataFrame] = []
    for t in tickers:
        try:
            n_fh = _prov_finnhub(t, a.start, a.end, None, limit=240)
        except Exception:
            n_fh = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
        try:
            n_yf = _prov_yfinance(t, a.start, a.end, None, limit=a.news_per_provider)
        except Exception:
            n_yf = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

        # Merge & dedup by (title, url)
        if len(n_fh) or len(n_yf):
            n = pd.concat([n_fh, n_yf], ignore_index=True)
            n["url"] = n["url"].fillna("")
            n = (
                n.drop_duplicates(["title", "url"])
                 .sort_values("ts")
                 .reset_index(drop=True)
            )
            n = n[(n["ts"] >= pd.to_datetime(a.start, utc=True)) &
                  (n["ts"] <= pd.to_datetime(a.end, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1))]
        else:
            n = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

        days = 0 if n.empty else n["ts"].dt.date.nunique()
        print(f"  {t}: finnhub_rows={len(n_fh):4d}, yfinance_rows={len(n_yf):4d} | merged_rows={len(n):4d} | days={days}")

        # FinBERT (4 decimals)
        n = _score_rows_inplace(fb, n, text_col="text", batch=a.batch)
        news_all.append(n)

    news_rows = (
        pd.concat(news_all, ignore_index=True)
        if news_all else
        pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])
    )

    # Aggregate to daily S (by ticker & date)
    d_news = daily_sentiment_from_rows(news_rows, "news", cutoff_minutes=a.cutoff_minutes)
    if not d_news.empty:
        d_news["date"] = pd.to_datetime(d_news["date"], utc=True, errors="coerce").dt.tz_localize(None)
        for c in ("S_NEWS", "S"):
            if c in d_news.columns:
                d_news[c] = pd.to_numeric(d_news[c], errors="coerce").fillna(0.0).round(4)

    # (no earnings yet)
    d_earn = pd.DataFrame(columns=["date", "ticker", "S_EARN"])

    # Compose final daily with a composite S
    daily = join_and_fill_daily(d_news, d_earn)
    if "S" not in daily.columns:
        daily["S_NEWS"] = pd.to_numeric(daily.get("S_NEWS", 0.0), errors="coerce").fillna(0.0)
        daily["S_EARN"] = pd.to_numeric(daily.get("S_EARN", 0.0), errors="coerce").fillna(0.0)
        daily["S"] = (daily["S_NEWS"] + daily["S_EARN"]).round(4)

    # Merge prices + daily sentiment
    panel = prices.merge(daily, on=["date", "ticker"], how="left")
    for c in ("S", "S_NEWS", "S_EARN"):
        if c not in panel.columns:
            panel[c] = 0.0
        panel[c] = pd.to_numeric(panel[c], errors="coerce").fillna(0.0).round(4)

    # Write artifacts
    write_outputs(panel, news_rows, None, a.out)

    # Summary from files
    tickers_list, have_files, with_news, with_nonzero_s = _summarize_from_files(a.out)
    print("Summary:")
    print(f"  Tickers listed: {len(tickers_list)}")
    print(f"  Ticker JSON files: {have_files}")
    print(f"  Tickers with any news: {with_news}/{len(tickers_list)}")
    print(f"  Tickers with non-zero daily S: {with_nonzero_s}/{len(tickers_list)}")


if __name__ == "__main__":
    main()
