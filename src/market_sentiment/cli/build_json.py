from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple
from pathlib import Path

import pandas as pd

from market_sentiment.aggregate import (
    add_forward_returns,
    daily_sentiment_from_rows,
    join_and_fill_daily,
    _ensure_date_dtype,
)
from market_sentiment.finbert import FinBERT
from market_sentiment.news import fetch_news
from market_sentiment.prices import fetch_prices_yf
from market_sentiment.writers import write_outputs
from market_sentiment.news_enforcer import ensure_top_n_news_from_store


# ---------------- FinBERT helpers ----------------

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
        # keep 4 decimals as requested
        out["S"] = pd.to_numeric(pd.Series(scores), errors="coerce").fillna(0.0).round(4)
    except Exception:
        out["S"] = 0.0
    return out


# ---------------- Prices (parallel) ----------------

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


# ---------------- Diagnostics ----------------

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


def _best_effort_company(ticker: str) -> Optional[str]:
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).get_info() or {}
        name = info.get("longName") or info.get("shortName")
        if name and 2 <= len(name) <= 80:
            return str(name).strip()
    except Exception:
        pass
    return None


# ---------------- Main ----------------

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

    # News controls
    p.add_argument("--cache-dir", default="data/news_cache")
    p.add_argument("--finnhub-rps", type=int, default=1, help="Finnhub requests per second (<=30)")
    p.add_argument("--finnhub-max-wait-sec", type=int, default=600, help="Max total backoff per day on 429")
    p.add_argument("--yfinance-count", type=int, default=240, help="yfinance get_news(count=...)")

    a = p.parse_args()

    # Universe
    uni = pd.read_csv(a.universe)
    if "ticker" not in uni.columns:
        uni = uni.rename(columns={uni.columns[0]: "ticker"})
    tickers = sorted(str(x).strip().upper() for x in uni["ticker"].dropna().unique().tolist())
    if a.max_tickers and a.max_tickers > 0:
        tickers = tickers[: a.max_tickers]

    print(
        f"Build JSON for {len(tickers)} tickers\n"
        f"  range={a.start}..{a.end}\n"
        f"  finnhub_rps={a.finnhub_rps} finnhub_max_wait={a.finnhub_max_wait_sec}s yfinance_count={a.yfinance_count}\n"
        f"  cache_dir={a.cache_dir}"
    )

    # Prices
    print("Prices:")
    prices = _fetch_all_prices(tickers, a.start, a.end, max_workers=a.max_workers)
    if prices is None or prices.empty:
        raise RuntimeError("No prices downloaded.")
    print(f"  ✓ Prices for {prices['ticker'].nunique()} tickers, rows={len(prices)}")

    # News + FinBERT
    try:
        fb = FinBERT()
    except Exception:
        fb = None
        print("  ! FinBERT unavailable, S defaults to 0.0")

    news_all: List[pd.DataFrame] = []
    for t in tickers:
        comp = _best_effort_company(t)
        # Always include Finnhub + yfinance (merged inside fetch_news)
        n = fetch_news(
            t, a.start, a.end, company=comp,
            cache_dir=a.cache_dir,
            finnhub_rps=a.finnhub_rps,
            finnhub_max_wait_sec=a.finnhub_max_wait_sec,
            yfinance_count=a.yfinance_count,
            verbose=True,
        )
        dcount = n["ts"].dt.date.nunique() if not n.empty else 0
        print(f"News: {t}: rows={len(n)} | unique_days={dcount}")
        n = _score_rows_inplace(fb, n, text_col="text", batch=a.batch)
        news_all.append(n)

    news_rows = (
        pd.concat(news_all, ignore_index=True)
        if news_all else
        pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])
    )
    earn_rows = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])  # placeholder

    # Aggregate to daily sentiment (news only for now)
    d_news = daily_sentiment_from_rows(news_rows, "news", cutoff_minutes=a.cutoff_minutes)
    d_earn = pd.DataFrame(columns=["date", "ticker", "S_EARN"])

    # normalize merge key
    if not d_news.empty:
        d_news["date"] = pd.to_datetime(d_news["date"], utc=True, errors="coerce").dt.tz_localize(None)

    daily = join_and_fill_daily(d_news, d_earn)
    if "S" not in daily.columns:
        s_news = pd.to_numeric(daily.get("S_NEWS", pd.Series(0.0, index=daily.index)), errors="coerce").fillna(0.0)
        s_earn = pd.to_numeric(daily.get("S_EARN", pd.Series(0.0, index=daily.index)), errors="coerce").fillna(0.0)
        daily["S"] = s_news + s_earn

    # Panel & write outputs
    panel = prices.merge(daily, on=["date", "ticker"], how="left")
    for c in ("S", "S_NEWS", "S_EARN"):
        if c not in panel.columns:
            panel[c] = 0.0
        panel[c] = pd.to_numeric(panel[c], errors="coerce").fillna(0.0)

    def _df_rows_from_items(ticker: str, items: List[dict]) -> pd.DataFrame:
        rows = []
        for it in items:
            ts = pd.to_datetime(
                it.get("ts")
                or (it.get("raw", {}) or {}).get("content", {}).get("displayTime")
                or (it.get("raw", {}) or {}).get("pubDate"),
                errors="coerce", utc=True
            )
            rows.append({
                "ticker": ticker,
                "ts": ts,
                "title": it.get("headline") or it.get("title") or "",
                "url": it.get("url") or "",
                "text": it.get("summary") or it.get("text") or "",
                "S": 0.0,
            })
        return pd.DataFrame(rows, columns=["ticker", "ts", "title", "url", "text", "S"])

    if news_rows is not None and not news_rows.empty:
        try:
            s_map = news_rows.dropna(subset=["url"]).set_index("url")["S"].to_dict()
        except Exception:
            s_map = {}

        out_parts: List[pd.DataFrame] = []
        for t in tickers:
            df_t = news_rows[news_rows["ticker"] == t].copy()
            cur_items = [
                {
                    "ts": (r["ts"].isoformat() if pd.notnull(r["ts"]) else None),
                    "headline": r.get("title", ""),
                    "summary": r.get("text", ""),
                    "url": r.get("url", "")
                }
                for _, r in df_t.iterrows()
            ]
            top10 = ensure_top_n_news_from_store(
                symbol=t,
                current_items=cur_items,
                data_dir=Path("data"),
                n=10,
                providers=("yfinance", "finnhub", "newsapi"),
                history_budget=200
            )
            df_top10 = _df_rows_from_items(t, top10)
            if "url" in df_top10.columns and s_map:
                df_top10["S"] = df_top10["url"].map(s_map).fillna(0.0)
            out_parts.append(df_top10)

        news_rows_for_write = pd.concat(out_parts, ignore_index=True)
    else:
        news_rows_for_write = news_rows

    # 原先：write_outputs(panel, news_rows, earn_rows, a.out)
    write_outputs(panel, news_rows_for_write, earn_rows, a.out)

    # Summary (from written files)
    tickers_list, have_files, with_news, with_nonzero_s = _summarize_from_files(a.out)
    print("Summary:")
    print(f"  Tickers listed: {len(tickers_list)}")
    print(f"  Ticker JSON files: {have_files}")
    print(f"  Tickers with any news: {with_news}/{len(tickers_list)}")
    print(f"  Tickers with non-zero daily S: {with_nonzero_s}/{len(tickers_list)}")


if __name__ == "__main__":
    main()
