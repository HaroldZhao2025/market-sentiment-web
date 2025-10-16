# src/market_sentiment/cli/build_json.py
from __future__ import annotations

import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
from tqdm import tqdm

from market_sentiment.prices import fetch_prices_yf
from market_sentiment.news import fetch_news
from market_sentiment.edgar import fetch_earnings_docs
from market_sentiment.finbert import FinBERT
from market_sentiment.aggregate import (
    daily_sentiment_from_rows,
    join_and_fill_daily,
    add_forward_returns,
)
from market_sentiment.writers import build_ticker_json, write_outputs


def _to_naive_midnight(s: pd.Series) -> pd.Series:
    """Make a date-like Series tz-naive and normalized to midnight."""
    s2 = pd.to_datetime(s, errors="coerce", utc=True)
    return s2.dt.tz_localize(None).dt.normalize()


def _fetch_all_prices(tickers, start, end, max_workers: int = 8) -> pd.DataFrame:
    rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(fetch_prices_yf, t, start, end): t for t in tickers}
        for f in tqdm(as_completed(futs), total=len(futs), desc="Prices"):
            rows.append(f.result())
    out = (
        pd.concat(rows, ignore_index=True)
        if rows
        else pd.DataFrame(columns=["date", "ticker", "open", "close"])
    )
    return out


def _collapse_scores_to_scalar(scores) -> np.ndarray:
    """
    Convert various FinBERT score shapes into a 1-D scalar sentiment S per text.
    Preferred definition: S = P(pos) - P(neg).
      - np.array (N,3): assume [neg, neu, pos]
      - list of dicts: keys like 'positive'/'pos', 'negative'/'neg'
      - list of tuples/lists len>=3: [neg, neu, pos]
      - list of string labels: 'positive'/'negative'/'neutral'
      - 1-D scores: already scalar
    Fallback: zeros.
    """
    # NumPy array or array-like first
    try:
        arr = np.asarray(scores)
        if arr.ndim == 2 and arr.shape[1] >= 3:
            # assume [neg, neu, pos]
            neg = arr[:, 0].astype(float)
            pos = arr[:, 2].astype(float)
            return (pos - neg).astype(float)
        if arr.ndim == 1:
            # already scalar
            return arr.astype(float)
    except Exception:
        pass

    # list of dicts
    if isinstance(scores, (list, tuple)) and scores and isinstance(scores[0], dict):
        def g(d, *names):
            for n in names:
                if n in d:
                    return float(d[n])
            return 0.0
        out = [g(d, "positive", "pos") - g(d, "negative", "neg") for d in scores]
        return np.array(out, dtype=float)

    # list of tuples/lists length>=3
    if (
        isinstance(scores, (list, tuple))
        and scores
        and isinstance(scores[0], (list, tuple))
        and len(scores[0]) >= 3
    ):
        out = [float(s[-1]) - float(s[0]) for s in scores]  # pos - neg
        return np.array(out, dtype=float)

    # list of string labels
    if isinstance(scores, (list, tuple)) and scores and isinstance(scores[0], str):
        m = {"positive": 1.0, "pos": 1.0, "negative": -1.0, "neg": -1.0, "neutral": 0.0, "neu": 0.0}
        out = [m.get(s.lower(), 0.0) for s in scores]
        return np.array(out, dtype=float)

    # fallback
    try:
        n = len(scores)
    except Exception:
        n = 0
    return np.zeros(n, dtype=float)


def _score_rows_inplace(fb: FinBERT, df: pd.DataFrame, text_col: str, batch: int = 16) -> pd.DataFrame:
    """Score df[text_col] with FinBERT and write a scalar column df['S']."""
    if df.empty:
        df["S"] = []
        return df

    texts = (df[text_col].fillna("").astype(str)).tolist()
    # Be tolerant to either score(texts, batch=...) or score(texts)
    try:
        scores = fb.score(texts, batch=batch)
    except TypeError:
        scores = fb.score(texts)

    S = _collapse_scores_to_scalar(scores)
    if S.shape[0] != len(df):
        # Defensive: align lengths if a scorer returns fewer/extra
        S = np.resize(S, len(df))
    df["S"] = pd.to_numeric(pd.Series(S), errors="coerce").fillna(0.0)
    return df


def main():
    ap = argparse.ArgumentParser("Build JSON artifacts for the web app")
    ap.add_argument("--universe", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--batch", type=int, default=16)
    ap.add_argument("--cutoff-minutes", type=int, default=5)
    ap.add_argument("--max-tickers", type=int, default=0)
    ap.add_argument("--max-workers", type=int, default=8)
    a = ap.parse_args()

    out_dir = Path(a.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Universe
    u = pd.read_csv(a.universe)
    tick_col = "ticker" if "ticker" in u.columns else ("Symbol" if "Symbol" in u.columns else None)
    if not tick_col:
        raise ValueError("Universe CSV must have 'ticker' or 'Symbol' column.")
    tickers = u[tick_col].astype(str).str.upper().dropna().unique().tolist()
    if a.max_tickers and a.max_tickers > 0:
        tickers = tickers[: a.max_tickers]

    print(f"\nBuild JSON for {len(tickers)} tickers | batch={a.batch} cutoff_min={a.cutoff_minutes} max_workers={a.max_workers}")

    # Prices
    prices = _fetch_all_prices(tickers, a.start, a.end, max_workers=a.max_workers).copy()
    prices["date"] = _to_naive_midnight(prices["date"])

    # News + Earnings + FinBERT
    fb = FinBERT()
    news_rows = []
    earn_rows = []
    for t in tqdm(tickers, desc="News+Earnings"):
        n = fetch_news(t, a.start, a.end)
        e = fetch_earnings_docs(t, a.start, a.end)
        if not n.empty:
            n = _score_rows_inplace(fb, n, text_col="text", batch=a.batch)
            news_rows.append(n[["ticker", "ts", "title", "url", "text", "S"]])
        if not e.empty:
            e = _score_rows_inplace(fb, e, text_col="text", batch=a.batch)
            earn_rows.append(e[["ticker", "ts", "title", "url", "text", "S"]])

    news_rows = (
        pd.concat(news_rows, ignore_index=True)
        if news_rows
        else pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])
    )
    earn_rows = (
        pd.concat(earn_rows, ignore_index=True)
        if earn_rows
        else pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])
    )

    d_news = daily_sentiment_from_rows(news_rows, kind="news", cutoff_minutes=a.cutoff_minutes)
    d_earn = daily_sentiment_from_rows(earn_rows, kind="earn", cutoff_minutes=a.cutoff_minutes)
    daily = join_and_fill_daily(d_news, d_earn)
    daily["date"] = _to_naive_midnight(daily["date"])

    # Merge & compute returns
    panel = prices.merge(daily, on=["date", "ticker"], how="left").fillna({"S": 0.0})
    panel = add_forward_returns(panel)

    # Write outputs for the web app
    write_outputs(panel, news_rows, earn_rows, out_dir)

    # Summary for logs
    by_t = panel.groupby("ticker")["S"].apply(lambda s: (s.abs() > 0).sum())
    nz = int((by_t > 0).sum())
    print("\nSummary:")
    print(f"  Tickers listed: {len(tickers)}")
    print(f"  Tickers with non-zero daily S: {nz}/{len(tickers)}")


if __name__ == "__main__":
    main()
