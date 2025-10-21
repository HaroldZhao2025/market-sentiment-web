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
from market_sentiment.finbert import FinBERT  # optional — handled defensively
from market_sentiment.news import fetch_news
from market_sentiment.prices import fetch_prices_yf
from market_sentiment.writers import write_outputs


# -------------------------
# Helpers
# -------------------------

def _score_texts(fb: FinBERT, texts: List[str], batch: int) -> List[float]:
    """Call FinBERT with a resilient signature."""
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
    """Add column S to df by scoring text_col; if FinBERT unavailable -> zeros."""
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
    return out


def _fetch_all_prices(
    tickers: List[str], start: str, end: str, max_workers: int
) -> pd.DataFrame:
    """Threaded yfinance download with gentle rate limiting."""
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
    prices = _ensure_date_dtype(prices, "date")  # -> naive date
    prices = add_forward_returns(prices)
    return prices


def _best_effort_company(ticker: str) -> Optional[str]:
    """Try to get a human company name for better RSS/GDELT recall."""
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).get_info() or {}
        name = info.get("longName") or info.get("shortName")
        if name and 2 <= len(name) <= 80:
            return str(name).strip()
    except Exception:
        pass
    return None


def _summarize_from_files(out_dir: str) -> Tuple[List[str], int, int, int]:
    """Tiny health summary printed at the end of the run."""
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


def _safe_num_col(df: pd.DataFrame, col: str) -> pd.Series:
    """Return a numeric Series for df[col] if present, else zeros of proper index length."""
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    # fall back to scalar zeros with correct index
    return pd.Series(0.0, index=df.index, dtype="float64")


# -------------------------
# CLI main
# -------------------------

def main():
    p = argparse.ArgumentParser("Build JSON artifacts for the web app")
    p.add_argument("--universe", required=True, help="CSV with 'ticker' column (first col accepted)")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--out", required=True, help="Output dir: apps/web/public/data")
    p.add_argument("--batch", type=int, default=16, help="FinBERT batch size")
    p.add_argument("--cutoff-minutes", type=int, default=5, help="de-dup window for same-day headlines")
    p.add_argument("--max-tickers", type=int, default=0)
    p.add_argument("--max-workers", type=int, default=8)
    p.add_argument("--max-per-provider", type=int, default=6000, help="news depth requested from each provider")
    a = p.parse_args()

    # universe
    uni = pd.read_csv(a.universe)
    if "ticker" not in uni.columns:
        uni = uni.rename(columns={uni.columns[0]: "ticker"})
    tickers = sorted(str(x).strip().upper() for x in uni["ticker"].dropna().unique().tolist())
    if a.max_tickers and a.max_tickers > 0:
        tickers = tickers[: a.max_tickers]

    print(f"Build JSON for {len(tickers)} tickers | batch={a.batch} cutoff_min={a.cutoff_minutes} max_workers={a.max_workers}")

    # -------- Prices --------
    print("Prices:")
    prices = _fetch_all_prices(tickers, a.start, a.end, max_workers=a.max_workers)
    if prices is None or prices.empty:
        raise RuntimeError("No prices downloaded.")
    print(f"  ✓ Downloaded prices for {prices['ticker'].nunique()} tickers, {len(prices)} rows.")

    # -------- News --------
    print("News+Earnings:")
    try:
        fb = FinBERT()
    except Exception:
        fb = None

    news_all: List[pd.DataFrame] = []
    for t in tickers:
        comp = _best_effort_company(t)
        try:
            # ask providers for *a lot* (headlines list stays 10 in writers, but S uses full period)
            n = fetch_news(t, a.start, a.end, company=comp, max_per_provider=a.max_per_provider)
            if n is None or n.empty:
                n = fetch_news(t, a.start, a.end, company=None, max_per_provider=a.max_per_provider)
        except Exception:
            n = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

        if n is None or n.empty:
            print(f"  {t}: news rows=0 | days=0 | company={'-' if not comp else comp}")
        else:
            dcount = n["ts"].dt.date.nunique() if "ts" in n.columns else 0
            print(f"  {t}: news rows={len(n)} | days={dcount} | company={'-' if not comp else comp}")

        # FinBERT scoring (on 'text', which already mirrors title when body missing)
        n = _score_rows_inplace(fb, n, text_col="text", batch=a.batch)
        news_all.append(n)

    news_rows = (
        pd.concat(news_all, ignore_index=True)
        if news_all else
        pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])
    )

    # earnings placeholder (kept for structure)
    earn_rows = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])

    # -------- Aggregate daily sentiment (compat; writers will still compute per-day from news rows) --------
    d_news = daily_sentiment_from_rows(news_rows, "news", cutoff_minutes=a.cutoff_minutes)
    d_earn = (
        daily_sentiment_from_rows(earn_rows, "earn", cutoff_minutes=a.cutoff_minutes)
        if not earn_rows.empty else
        pd.DataFrame(columns=["date", "ticker", "S_EARN"])
    )

    # normalize merge dtypes
    if not d_news.empty:
        d_news["date"] = pd.to_datetime(d_news["date"], utc=True, errors="coerce").dt.tz_localize(None)
    if not d_earn.empty:
        d_earn["date"] = pd.to_datetime(d_earn["date"], utc=True, errors="coerce").dt.tz_localize(None)

    daily = join_and_fill_daily(d_news, d_earn)

    # safe numeric columns (avoid the earlier 'float has no fillna' crash)
    if not daily.empty:
        daily["S_NEWS"] = _safe_num_col(daily, "S_NEWS")
        daily["S_EARN"] = _safe_num_col(daily, "S_EARN")
        if "S" not in daily.columns:
            daily["S"] = daily["S_NEWS"] + daily["S_EARN"]

    # -------- Build panel + write outputs --------
    panel = prices.merge(daily, on=["date", "ticker"], how="left")
    for c in ("S", "S_NEWS", "S_EARN"):
        if c not in panel.columns:
            panel[c] = 0.0
        panel[c] = pd.to_numeric(panel[c], errors="coerce").fillna(0.0)

    # writers.py will compute per-day S from the *scored* news rows across the full window
    write_outputs(panel, news_rows, earn_rows, a.out)

    # -------- Summary --------
    tickers_list, have_files, with_news, with_nonzero_s = _summarize_from_files(a.out)
    print("Summary:")
    print(f"  Tickers listed: {len(tickers_list)}")
    print(f"  Ticker JSON files: {have_files}")
    print(f"  Tickers with any news: {with_news}/{len(tickers_list)}")
    print(f"  Tickers with non-zero daily S: {with_nonzero_s}/{len(tickers_list)}")


if __name__ == "__main__":
    main()
