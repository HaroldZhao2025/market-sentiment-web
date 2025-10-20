# src/market_sentiment/cli/build_json.py
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple

import pandas as pd

from market_sentiment.aggregate import (
    add_forward_returns,
    _ensure_date_dtype,
)
from market_sentiment.finbert import FinBERT
from market_sentiment.news import fetch_news
from market_sentiment.prices import fetch_prices_yf
from market_sentiment.writers import write_outputs


def _score_texts(fb: FinBERT, texts: List[str], batch: int) -> List[float]:
    """Be compatible with different FinBERT.score signatures."""
    try:
        return fb.score(texts, batch=batch)
    except TypeError:
        try:
            return fb.score(texts, batch_size=batch)
        except TypeError:
            return fb.score(texts)


def _fetch_all_prices(tickers: List[str], start: str, end: str, max_workers: int) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_prices_yf, t, start, end): t for t in tickers}
        for fut in as_completed(futures):
            try:
                df = fut.result()
            except Exception:
                df = pd.DataFrame()
            if df is not None and not df.empty:
                rows.append(df)

    if not rows:
        return pd.DataFrame(columns=["date", "ticker", "open", "close"])
    prices = pd.concat(rows, ignore_index=True)
    prices = _ensure_date_dtype(prices, "date")
    prices = add_forward_returns(prices)
    return prices


def _best_effort_company(ticker: str) -> Optional[str]:
    """Try to fetch a company name to improve RSS queries; safe to fail."""
    try:
        import yfinance as yf
        info = (getattr(yf.Ticker(ticker), "info", {}) or {})
        nm = (info.get("longName") or info.get("shortName") or "").strip()
        return nm or None
    except Exception:
        return None


def _score_rows_keep(df: pd.DataFrame, fb: Optional[FinBERT], text_col: str, batch: int) -> pd.DataFrame:
    """
    Score rows; if anything fails, KEEP rows with S=0.0 (so aggregation can't drop them).
    """
    base_cols = ["ticker", "ts", "title", "url", text_col]
    if df is None or df.empty:
        return pd.DataFrame(columns=base_cols + ["S"])

    out = df.copy()
    texts = out.get(text_col, pd.Series([], dtype=str)).astype(str).fillna("").tolist()
    if fb is None or len(texts) == 0:
        out["S"] = 0.0
        return out

    try:
        scores = _score_texts(fb, texts, batch=batch)
        out["S"] = pd.to_numeric(pd.Series(scores), errors="coerce").fillna(0.0)
    except Exception:
        out["S"] = 0.0
    return out


def _window(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    s = pd.to_datetime(start, utc=True)
    e = pd.to_datetime(end, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    # make sure ts is tz-aware UTC
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    return df[(df["ts"] >= s) & (df["ts"] <= e)]


def _daily_from_rows(news_rows: pd.DataFrame) -> pd.DataFrame:
    """
    Explicit, robust aggregation: daily S = mean(S) by (date,ticker).
    This bypasses any helper that may drop rows due to cutoff/timezone quirks.
    """
    if news_rows is None or news_rows.empty:
        return pd.DataFrame(columns=["date", "ticker", "S"])

    df = news_rows.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts", "ticker"])
    if df.empty:
        return pd.DataFrame(columns=["date", "ticker", "S"])

    df["S"] = pd.to_numeric(df.get("S", 0.0), errors="coerce").fillna(0.0)
    df["date"] = df["ts"].dt.normalize()  # midnight UTC, tz-aware
    daily = (
        df.groupby(["date", "ticker"], as_index=False)["S"]
        .mean(numeric_only=True)
        .rename(columns={"S": "S"})
    )
    return daily[["date", "ticker", "S"]]


def _safe_write_outputs(panel: pd.DataFrame, news_rows: pd.DataFrame, out_dir: str) -> None:
    """
    Call writers.write_outputs with (panel, news_rows, out_dir)
    or (panel, news_rows, earn_rows, out_dir) depending on installed version.
    """
    try:
        # Newer signature in your message
        empty_earn = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])
        write_outputs(panel, news_rows, empty_earn, out_dir)
    except TypeError:
        # Older signature: (panel, news_rows, out_dir)
        write_outputs(panel, news_rows, out_dir)


def main():
    p = argparse.ArgumentParser("Build JSON artifacts for the web app")
    p.add_argument("--universe", required=True, help="CSV with ticker column (or first column)")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--out", required=True, help="Output dir: apps/web/public/data")
    p.add_argument("--batch", type=int, default=16)
    p.add_argument("--max-tickers", type=int, default=0)
    p.add_argument("--max-workers", type=int, default=8)
    a = p.parse_args()

    # -------- Universe --------
    uni = pd.read_csv(a.universe)
    if "ticker" not in uni.columns:
        uni = uni.rename(columns={uni.columns[0]: "ticker"})
    tickers = sorted(str(x).strip().upper() for x in uni["ticker"].dropna().unique())
    if a.max_tickers and a.max_tickers > 0:
        tickers = tickers[: a.max_tickers]

    print(f"Build JSON for {len(tickers)} tickers | batch={a.batch} max_workers={a.max_workers}")

    # -------- Prices --------
    print("Prices:")
    prices = _fetch_all_prices(tickers, a.start, a.end, max_workers=a.max_workers)
    if prices is None or prices.empty:
        raise RuntimeError("No prices downloaded.")
    print(f"  ✓ Downloaded prices for {prices['ticker'].nunique()} tickers, {len(prices)} rows.")

    # -------- News (robust path) --------
    print("News+Earnings:")
    try:
        fb = FinBERT()
    except Exception:
        fb = None

    news_frames: List[pd.DataFrame] = []
    per_ticker_counts: List[Tuple[str, int]] = []

    for t in tickers:
        comp = _best_effort_company(t)
        raw = fetch_news(t, a.start, a.end, company=comp, max_per_provider=300)
        raw = _window(raw, a.start, a.end)  # hard window (defensive)

        scored = _score_rows_keep(raw, fb, text_col="text", batch=a.batch)

        # Guarantee schema
        for c in ("ticker", "ts", "title", "url", "text", "S"):
            if c not in scored.columns:
                scored[c] = "" if c in ("title", "url", "text") else (pd.NaT if c == "ts" else 0.0)
        # Make sure ticker column is correct
        scored["ticker"] = t

        news_frames.append(scored)
        per_ticker_counts.append((t, len(scored)))

    news_rows = pd.concat(news_frames, ignore_index=True) if news_frames else pd.DataFrame(
        columns=["ticker", "ts", "title", "url", "text", "S"]
    )

    # -------- Daily sentiment (explicit aggregation, no drops) --------
    daily = _daily_from_rows(news_rows)

    # -------- Panel join --------
    panel = prices.merge(daily, on=["date", "ticker"], how="left")
    panel["S"] = pd.to_numeric(panel.get("S", 0.0), errors="coerce").fillna(0.0)

    # -------- Write outputs --------
    _safe_write_outputs(panel, news_rows, a.out)

    # -------- Summary --------
    with_news = sum(1 for (t, n) in per_ticker_counts if n > 0)
    unique_days = 0
    if not news_rows.empty:
        unique_days = (
            pd.to_datetime(news_rows["ts"], utc=True, errors="coerce")
            .dt.date.nunique()
        )
    nz_tickers = int(panel.groupby("ticker")["S"].apply(lambda s: (s.abs() > 1e-12).any()).sum())

    print("Summary:")
    print(f"  Tickers listed: {len(tickers)}")
    print(f"  Ticker JSON files: {len(tickers)}")
    print(f"  Tickers with any news: {with_news}/{len(tickers)}")
    print(f"  Tickers with non-zero daily S: {nz_tickers}/{len(tickers)}")
    print(f"  News rows total: {len(news_rows)} | unique days covered: {unique_days}")


if __name__ == "__main__":
    main()
