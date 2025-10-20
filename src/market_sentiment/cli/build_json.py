# src/market_sentiment/cli/build_json.py
from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

import pandas as pd

from market_sentiment.aggregate import (
    add_forward_returns,
    daily_sentiment_from_rows,
    join_and_fill_daily,
    _ensure_date_dtype,
)
from market_sentiment.finbert import FinBERT  # optional – handled gracefully below
from market_sentiment.news import fetch_news
from market_sentiment.prices import fetch_prices_yf
from market_sentiment.writers import write_outputs


# -------------------------------
# FinBERT helpers (backward compat)
# -------------------------------

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


def _score_rows_inplace(fb: Optional[FinBERT], df: pd.DataFrame, text_col: str, batch: int) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", text_col, "S"])
    df = df.copy()
    if fb is None:
        # No FinBERT available -> leave S=0.0; a daily fallback will kick in later
        df["S"] = 0.0
        return df

    texts = df[text_col].astype(str).fillna("").tolist()
    if not texts:
        df["S"] = 0.0
        return df

    try:
        scores = _score_texts(fb, texts, batch=batch)
        df["S"] = pd.to_numeric(pd.Series(scores), errors="coerce").fillna(0.0)
    except Exception:
        # Any runtime error -> degrade gracefully
        df["S"] = 0.0
    return df


# -------------------------------
# Prices (with gentle throttling)
# -------------------------------

def _fetch_all_prices(tickers: List[str], start: str, end: str, max_workers: int) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    # Submit jobs with slight staggering to avoid burst rate-limits
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = []
        for t in tickers:
            futs.append(ex.submit(fetch_prices_yf, t, start, end))
            time.sleep(0.15)  # 150ms between submissions reduces YF throttling

        for f in as_completed(futs):
            try:
                df = f.result()
                if df is not None and len(df) > 0:
                    rows.append(df)
            except Exception:
                # Ignore single-ticker failures; continue building the panel
                pass

    if not rows:
        return pd.DataFrame(columns=["date", "ticker", "open", "close"])
    prices = pd.concat(rows, ignore_index=True)
    prices = _ensure_date_dtype(prices, "date")
    prices = add_forward_returns(prices)
    return prices


# -------------------------------
# Fallback daily sentiment (no FinBERT or all-zero S)
# -------------------------------

def _fallback_daily_from_counts(news_rows: pd.DataFrame) -> pd.DataFrame:
    """
    If FinBERT is unavailable or S scores are (nearly) all zero, build a
    per-day sentiment proxy from news frequency deviations by ticker.

    Returns: DataFrame with columns ["date", "ticker", "S_NEWS"].
    """
    if news_rows is None or news_rows.empty:
        return pd.DataFrame(columns=["date", "ticker", "S_NEWS"])

    df = news_rows.copy()
    # Normalize timestamps -> UTC day
    df["date"] = pd.to_datetime(df["ts"], utc=True, errors="coerce").dt.normalize()
    df = df.dropna(subset=["date", "ticker"])
    if df.empty:
        return pd.DataFrame(columns=["date", "ticker", "S_NEWS"])

    # Count news per (ticker, date)
    cnt = (
        df.groupby(["ticker", "date"], as_index=False)
          .size()
          .rename(columns={"size": "n"})
    )

    out_rows = []
    for t, g in cnt.groupby("ticker"):
        g = g.sort_values("date").reset_index(drop=True)
        n = g["n"].astype(float)
        mean = float(n.mean())
        std = float(n.std(ddof=0))
        if std <= 1e-12:
            z = (n - mean)  # all equal -> z ~ 0
        else:
            z = (n - mean) / std
        # clamp and scale to [-1, 1] (not to dominate price):
        s = (z.clip(-3, 3) / 3.0).astype(float)
        # small smoothing across days to reduce spikes
        s_ma3 = s.rolling(3, min_periods=1).mean()
        out = pd.DataFrame({"ticker": t, "date": g["date"], "S_NEWS": s_ma3})
        out_rows.append(out)

    if not out_rows:
        return pd.DataFrame(columns=["date", "ticker", "S_NEWS"])
    out = pd.concat(out_rows, ignore_index=True)
    return out[["date", "ticker", "S_NEWS"]]


def _is_all_zero_or_missing(d_news: pd.DataFrame) -> bool:
    if d_news is None or d_news.empty:
        return True
    col = "S_NEWS" if "S_NEWS" in d_news.columns else ("S" if "S" in d_news.columns else None)
    if col is None:
        return True
    s = pd.to_numeric(d_news[col], errors="coerce").fillna(0.0)
    return float(s.abs().max()) <= 1e-12


# -------------------------------
# Summary based on written files
# -------------------------------

def _summarize_from_files(out_dir: str):
    """
    Read /_tickers.json and per-ticker JSON files and summarize the
    *actual* exported content (truth), not the pre-writer panel.
    """
    out_dir = str(out_dir)
    try:
        tickers = json.load(open(os.path.join(out_dir, "_tickers.json")))
    except Exception:
        tickers = []
    have_files = 0
    with_news = 0
    with_nonzero_s = 0
    for t in tickers:
        f = os.path.join(out_dir, "ticker", f"{t}.json")
        if not os.path.exists(f):
            continue
        have_files += 1
        try:
            obj = json.load(open(f))
        except Exception:
            continue
        news = obj.get("news") or []
        if news:
            with_news += 1
        S = obj.get("S") or obj.get("sentiment") or []
        try:
            if any(abs(float(x or 0)) > 1e-12 for x in S):
                with_nonzero_s += 1
        except Exception:
            pass
    return tickers, have_files, with_news, with_nonzero_s


# -------------------------------
# Main
# -------------------------------

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

    # Universe; tolerate unnamed first column
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
    print(f"Build JSON for {mode} | batch={a.batch} cutoff_min={a.cutoff_minutes} max_workers={a.max_workers}")

    # ---------------- Prices ----------------
    print("Prices:")
    prices = _fetch_all_prices(tickers, a.start, a.end, max_workers=a.max_workers)
    if prices is None or prices.empty:
        raise RuntimeError("No prices downloaded.")
    print(f"  ✓ Downloaded prices for {prices['ticker'].nunique()} tickers, {len(prices)} rows.")

    # ---------------- News + (optional) earnings ----------------
    print("News+Earnings:")
    try:
        fb = FinBERT()
    except Exception:
        fb = None

    news_all: List[pd.DataFrame] = []
    for t in tickers:
        n = fetch_news(t, a.start, a.end)
        # Keep a sane schema no matter what
        if n is None or n.empty:
            n = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
        # Score (or set S=0 if no FinBERT); fallback will be applied later if needed
        n = _score_rows_inplace(fb, n, text_col="text", batch=a.batch)
        news_all.append(n)

    news_rows = (
        pd.concat(news_all, ignore_index=True)
        if news_all
        else pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])
    )

    # Earnings placeholder (schema-compatible)
    earn_rows = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])

    # ---------------- Aggregate daily sentiment ----------------
    d_news = daily_sentiment_from_rows(news_rows, "news", cutoff_minutes=a.cutoff_minutes)
    d_earn = (
        daily_sentiment_from_rows(earn_rows, "earn", cutoff_minutes=a.cutoff_minutes)
        if not earn_rows.empty else
        pd.DataFrame(columns=["date", "ticker", "S_EARN"])
    )

    # If FinBERT missing or all-zero S_NEWS, create a fallback from news counts
    if _is_all_zero_or_missing(d_news):
        d_news = _fallback_daily_from_counts(news_rows)

    # Normalize date types
    if not d_news.empty:
        d_news["date"] = pd.to_datetime(d_news["date"], utc=True, errors="coerce")
    if not d_earn.empty:
        d_earn["date"] = pd.to_datetime(d_earn["date"], utc=True, errors="coerce")

    daily = join_and_fill_daily(d_news, d_earn)

    # Ensure a composite S exists
    if "S" not in daily.columns:
        daily["S_NEWS"] = pd.to_numeric(daily.get("S_NEWS", 0.0), errors="coerce").fillna(0.0)
        daily["S_EARN"] = pd.to_numeric(daily.get("S_EARN", 0.0), errors="coerce").fillna(0.0)
        daily["S"] = daily["S_NEWS"] + daily["S_EARN"]

    # ---------------- Build panel ----------------
    panel = prices.merge(daily, on=["date", "ticker"], how="left")
    for c in ("S", "S_NEWS", "S_EARN"):
        if c not in panel.columns:
            panel[c] = 0.0
        panel[c] = pd.to_numeric(panel[c], errors="coerce").fillna(0.0)

    # ---------------- Write outputs ----------------
    # The writers wrapper accepts (panel, news_rows, out_dir) or (panel, news_rows, earn_rows, out_dir)
    write_outputs(panel, news_rows, earn_rows, a.out)

    # ---------------- Summary (from files) ----------------
    tickers_list, have_files, with_news, with_nonzero_s = _summarize_from_files(a.out)
    print("Summary:")
    print(f"  Tickers listed: {len(tickers_list)}")
    print(f"  Ticker JSON files: {have_files}")
    print(f"  Tickers with any news: {with_news}/{len(tickers_list)}")
    print(f"  Tickers with non-zero daily S: {with_nonzero_s}/{len(tickers_list)}")


if __name__ == "__main__":
    main()
