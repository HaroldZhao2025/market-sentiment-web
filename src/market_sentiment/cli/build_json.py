# src/market_sentiment/cli/build_json.py
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

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


def _score_texts(fb: FinBERT, texts: List[str], batch: int) -> List[float]:
    """
    Be compatible with different FinBERT.score signatures:
      - score(texts, batch=?)
      - score(texts, batch_size=?)
      - score(texts)
    """
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
        futs = {ex.submit(fetch_prices_yf, t, start, end): t for t in tickers}
        for f in as_completed(futs):
            try:
                df = f.result()
            except Exception:
                df = pd.DataFrame()
            if df is not None and len(df) > 0:
                rows.append(df)

    if not rows:
        return pd.DataFrame(columns=["date", "ticker", "open", "close"])
    prices = pd.concat(rows, ignore_index=True)
    prices = _ensure_date_dtype(prices, "date")
    prices = add_forward_returns(prices)
    return prices


def _score_rows_inplace(
    fb: Optional[FinBERT], df: pd.DataFrame, text_col: str, batch: int
) -> pd.DataFrame:
    """
    Returns a frame with an 'S' column. If FinBERT is None or scoring fails,
    emits zeros so downstream aggregation still works (charts won’t disappear).
    """
    base_cols = ["ticker", "ts", "title", "url", text_col]
    if df is None or df.empty:
        return pd.DataFrame(columns=base_cols + ["S"])

    out = df.copy()
    texts = out[text_col].astype(str).fillna("").tolist()
    if fb is None or len(texts) == 0:
        out["S"] = 0.0
        return out

    try:
        scores = _score_texts(fb, texts, batch=batch)
        out["S"] = pd.to_numeric(pd.Series(scores), errors="coerce").fillna(0.0)
    except Exception:
        # Keep the rows but make sentiment neutral instead of dropping everything
        out["S"] = 0.0
    return out


def _best_effort_company(ticker: str) -> Optional[str]:
    """
    Try to grab a long/short name for better RSS queries.
    Safe: failures simply return None.
    """
    try:
        import yfinance as yf  # local import to avoid import cost if not needed
        t = yf.Ticker(ticker)
        # longName often missing in fast_info; fall back to shortName
        info = getattr(t, "info", {}) or {}
        name = info.get("longName") or info.get("shortName")
        if name and isinstance(name, str) and len(name.strip()) > 0:
            return name.strip()
    except Exception:
        pass
    return None


def _fallback_daily_from_rows(news_rows: pd.DataFrame) -> pd.DataFrame:
    """
    If daily_sentiment_from_rows returns empty (or downstream join drops rows),
    explicitly build daily S = mean(S) by (date,ticker) from the raw rows.
    """
    if news_rows is None or news_rows.empty:
        return pd.DataFrame(columns=["date", "ticker", "S"])
    tmp = news_rows.copy()
    tmp["ts"] = pd.to_datetime(tmp["ts"], utc=True, errors="coerce")
    tmp = tmp.dropna(subset=["ts", "ticker"])
    if tmp.empty:
        return pd.DataFrame(columns=["date", "ticker", "S"])
    tmp["date"] = tmp["ts"].dt.tz_convert("UTC").dt.date.astype(str)
    tmp["S"] = pd.to_numeric(tmp.get("S", 0.0), errors="coerce").fillna(0.0)
    daily = (
        tmp.groupby(["date", "ticker"], as_index=False)["S"]
        .mean(numeric_only=True)
        .rename(columns={"date": "date", "ticker": "ticker", "S": "S"})
    )
    # Align types with prices (prices uses pandas datetime for merge)
    daily["date"] = pd.to_datetime(daily["date"], utc=True, errors="coerce")
    return daily


def main():
    p = argparse.ArgumentParser("Build JSON artifacts for the web app")
    p.add_argument("--universe", required=True, help="CSV with ticker column (or first column)")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--out", required=True, help="Output dir: apps/web/public/data")
    p.add_argument("--batch", type=int, default=16)
    p.add_argument("--cutoff-minutes", type=int, default=5)
    p.add_argument("--max-tickers", type=int, default=0)
    p.add_argument("--max-workers", type=int, default=8)
    a = p.parse_args()

    # Read universe; tolerate first column not named 'ticker'
    uni = pd.read_csv(a.universe)
    if "ticker" not in uni.columns:
        first = uni.columns[0]
        uni = uni.rename(columns={first: "ticker"})
    tickers: List[str] = sorted(
        [str(x).strip().upper() for x in uni["ticker"].dropna().unique().tolist()]
    )
    if a.max_tickers and a.max_tickers > 0:
        tickers = tickers[: a.max_tickers]

    mode = f"{len(tickers)} tickers"
    print(
        f"Build JSON for {mode} | batch={a.batch} cutoff_min={a.cutoff_minutes} max_workers={a.max_workers}"
    )

    # ---------------- Prices ----------------
    print("Prices:")
    prices = _fetch_all_prices(tickers, a.start, a.end, max_workers=a.max_workers)
    if prices is None or prices.empty:
        raise RuntimeError("No prices downloaded.")
    print(
        f"  ✓ Downloaded prices for {prices['ticker'].nunique()} tickers, {len(prices)} rows."
    )

    # ---------------- News ----------------
    print("News+Earnings:")
    try:
        fb = FinBERT()
    except Exception:
        fb = None  # keep going; graphs will render with neutral S if scoring fails

    news_all: List[pd.DataFrame] = []
    for t in tickers:
        comp = _best_effort_company(t)
        n = fetch_news(t, a.start, a.end, company=comp, max_per_provider=300)
        # Always keep rows; if FinBERT fails we still emit S=0 so charts don't disappear
        n = _score_rows_inplace(fb, n, text_col="text", batch=a.batch)
        news_all.append(n)

    news_rows = (
        pd.concat(news_all, ignore_index=True)
        if news_all
        else pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])
    )

    # ---------------- Daily aggregation (robust) ----------------
    # 1) Try the library helper (respects cutoff etc.)
    d_news = daily_sentiment_from_rows(
        news_rows, "news", cutoff_minutes=a.cutoff_minutes
    )

    # 2) If nothing made it through (common cause of "0/1 tickers with any news"),
    #    fall back to a plain per-day mean so the site has data.
    if d_news is None or d_news.empty:
        d_news = _fallback_daily_from_rows(news_rows)

    # No earnings yet; keep a schema-compatible empty frame
    d_earn = pd.DataFrame(columns=["date", "ticker", "S"])
    earn_rows = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])

    # ---------------- Build panel ----------------
    daily = join_and_fill_daily(d_news, d_earn)
    for c in ("S", "S_NEWS", "S_EARN"):
        if c not in daily.columns:
            daily[c] = 0.0
        daily[c] = pd.to_numeric(daily[c], errors="coerce").fillna(0.0)

    panel = prices.merge(daily[["date", "ticker", "S"]], on=["date", "ticker"], how="left")
    panel["S"] = pd.to_numeric(panel["S"], errors="coerce").fillna(0.0)

    # ---------------- Write outputs ----------------
    write_outputs(panel, news_rows, earn_rows, a.out)

    # ---------------- Summary (make the problem visible in logs) ------------
    listed = len(tickers)
    with_news = 0
    try:
        with_news = (
            news_rows[["ticker"]].dropna().nunique().iloc[0] if not news_rows.empty else 0
        )
    except Exception:
        with_news = 0

    nz = panel.groupby("ticker")["S"].apply(lambda s: (s.abs() > 1e-12).any()).sum()

    print("Summary:")
    print(f"  Tickers listed: {listed}")
    print(f"  Ticker JSON files: {listed}")
    print(f"  Tickers with any news: {with_news}/{listed}")
    print(f"  Tickers with non-zero daily S: {int(nz)}/{listed}")
    if not news_rows.empty:
        try:
            days = news_rows["ts"].pipe(pd.to_datetime, utc=True, errors="coerce").dt.date.nunique()
        except Exception:
            days = 0
        print(f"  News rows total: {len(news_rows)} | unique days covered: {days}")


if __name__ == "__main__":
    main()
