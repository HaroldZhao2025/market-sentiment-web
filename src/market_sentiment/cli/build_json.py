# src/market_sentiment/cli/build_json.py
from __future__ import annotations

import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

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


# ---------- FinBERT helpers (backward compatible) ----------
def _score_texts(fb: FinBERT, texts: List[str], batch: int) -> List[float]:
    try:
        return fb.score(texts, batch=batch)
    except TypeError:
        try:
            return fb.score(texts, batch_size=batch)
        except TypeError:
            return fb.score(texts)

def _score_rows_inplace(fb: Optional[FinBERT], df: pd.DataFrame, text_col: str, batch: int) -> pd.DataFrame:
    cols = ["ticker", "ts", "title", "url", text_col, "S"]
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)
    df = df.copy()
    # normalize ts to UTC
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])
    if fb is None:
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
        df["S"] = 0.0
    return df


# ---------- Prices (gentle throttle to avoid YF bursts) ----------
def _fetch_all_prices(tickers: List[str], start: str, end: str, max_workers: int) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = []
        for t in tickers:
            futs.append(ex.submit(fetch_prices_yf, t, start, end))
            time.sleep(0.1)  # small gap reduces 429s in CI
        for f in as_completed(futs):
            try:
                df = f.result()
                if df is not None and len(df) > 0:
                    frames.append(df)
            except Exception:
                pass
    if not frames:
        return pd.DataFrame(columns=["date", "ticker", "open", "close"])
    prices = pd.concat(frames, ignore_index=True)
    prices = _ensure_date_dtype(prices, "date")
    prices = add_forward_returns(prices)
    return prices


# ---------- Company lookup for better RSS recall ----------
_FALLBACK_COMPANY: Dict[str, str] = {
    "AAPL": "Apple Inc.",
    "MSFT": "Microsoft Corporation",
    "AMZN": "Amazon.com, Inc.",
    "GOOGL": "Alphabet Inc.",
    "GOOG": "Alphabet Inc.",
    "META": "Meta Platforms, Inc.",
    "NVDA": "NVIDIA Corporation",
    "TSLA": "Tesla, Inc.",
    "AMD": "Advanced Micro Devices, Inc.",
}

def _company_name(ticker: str) -> Optional[str]:
    t = ticker.upper()
    if t in _FALLBACK_COMPANY:
        return _FALLBACK_COMPANY[t]
    try:
        import yfinance as yf
        y = yf.Ticker(t)
        info = {}
        try:
            info = y.get_info() if hasattr(y, "get_info") else (y.info or {})
        except Exception:
            try:
                info = y.info or {}
            except Exception:
                info = {}
        nm = info.get("longName") or info.get("shortName") or None
        return nm
    except Exception:
        return None


# ---------- Fallback daily sentiment from headline counts ----------
def _fallback_daily_from_counts(news_rows: pd.DataFrame) -> pd.DataFrame:
    if news_rows is None or news_rows.empty:
        return pd.DataFrame(columns=["date", "ticker", "S_NEWS"])
    df = news_rows.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])
    df["date"] = df["ts"].dt.normalize()
    cnt = df.groupby(["ticker", "date"], as_index=False).size().rename(columns={"size": "n"})
    # z-score per ticker, then clip and scale to [-1, 1]
    out = []
    for t, g in cnt.groupby("ticker"):
        n = g["n"].astype(float)
        mean = float(n.mean())
        std = float(n.std(ddof=0))
        z = (n - mean) if std <= 1e-12 else (n - mean) / std
        s = (z.clip(-3, 3) / 3.0).astype(float)
        # light smoothing
        s_ma3 = s.rolling(3, min_periods=1).mean()
        out.append(pd.DataFrame({"ticker": t, "date": g["date"], "S_NEWS": s_ma3}))
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=["date", "ticker", "S_NEWS"])

def _is_all_zero_or_missing(d_news: pd.DataFrame) -> bool:
    if d_news is None or d_news.empty:
        return True
    col = "S_NEWS" if "S_NEWS" in d_news.columns else ("S" if "S" in d_news.columns else None)
    if col is None:
        return True
    s = pd.to_numeric(d_news[col], errors="coerce").fillna(0.0)
    return float(s.abs().max()) <= 1e-12


# ---------- Main ----------
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

    # Universe
    uni = pd.read_csv(a.universe)
    if "ticker" not in uni.columns:
        first = uni.columns[0]
        uni = uni.rename(columns={first: "ticker"})
    tickers: List[str] = sorted([str(x).strip().upper() for x in uni["ticker"].dropna().unique().tolist()])
    if a.max_tickers and a.max_tickers > 0:
        tickers = tickers[: a.max_tickers]

    mode = f"{len(tickers)} tickers"
    print(f"Build JSON for {mode} | batch={a.batch} cutoff_min={a.cutoff_minutes} max_workers={a.max_workers}")

    # Prices
    print("Prices:")
    prices = _fetch_all_prices(tickers, a.start, a.end, max_workers=a.max_workers)
    if prices is None or prices.empty:
        raise RuntimeError("No prices downloaded.")
    print(f"  ✓ Downloaded prices for {prices['ticker'].nunique()} tickers, {len(prices)} rows.")

    # News (+ scoring)
    print("News+Earnings:")
    try:
        fb = FinBERT()
    except Exception:
        fb = None

    news_frames: List[pd.DataFrame] = []
    for t in tickers:
        comp = _company_name(t)
        try:
            df = fetch_news(t, a.start, a.end, company=comp, max_per_provider=300)
        except Exception:
            df = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
        df = _score_rows_inplace(fb, df, text_col="text", batch=a.batch)
        news_frames.append(df)

    news_rows = pd.concat(news_frames, ignore_index=True) if news_frames else pd.DataFrame(
        columns=["ticker", "ts", "title", "url", "text", "S"]
    )

    # Earnings: keep schema-compatible empty frame for now
    earn_rows = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])

    # Daily aggregation
    d_news = daily_sentiment_from_rows(news_rows, "news", cutoff_minutes=a.cutoff_minutes)
    d_earn = daily_sentiment_from_rows(earn_rows, "earn", cutoff_minutes=a.cutoff_minutes) \
        if not earn_rows.empty else pd.DataFrame(columns=["date", "ticker", "S_EARN"])

    # Fallback if FinBERT/S aggregation yields empty or all-zero
    if _is_all_zero_or_missing(d_news):
        d_news = _fallback_daily_from_counts(news_rows)

    # Join and safety S
    if not d_news.empty:
        d_news["date"] = pd.to_datetime(d_news["date"], utc=True, errors="coerce")
    if not d_earn.empty:
        d_earn["date"] = pd.to_datetime(d_earn["date"], utc=True, errors="coerce")

    daily = join_and_fill_daily(d_news, d_earn)
    if "S" not in daily.columns:
        daily["S_NEWS"] = pd.to_numeric(daily.get("S_NEWS", 0.0), errors="coerce").fillna(0.0)
        daily["S_EARN"] = pd.to_numeric(daily.get("S_EARN", 0.0), errors="coerce").fillna(0.0)
        daily["S"] = daily["S_NEWS"] + daily["S_EARN"]

    # Panel (merge with prices)
    panel = prices.merge(daily, on=["date", "ticker"], how="left")
    for c in ("S", "S_NEWS", "S_EARN"):
        if c not in panel.columns:
            panel[c] = 0.0
        panel[c] = pd.to_numeric(panel[c], errors="coerce").fillna(0.0)

    # Write JSONs
    write_outputs(panel, news_rows, earn_rows, a.out)

    # Summary (truth from raw news)
    with_news = news_rows["ticker"].nunique() if not news_rows.empty else 0
    nz = panel.groupby("ticker")["S"].apply(lambda s: (pd.to_numeric(s, errors="coerce").fillna(0.0).abs() > 1e-12).any()).sum()

    print("Summary:")
    print(f"  Tickers listed: {len(tickers)}")
    print(f"  Ticker JSON files: {len(tickers)}")
    print(f"  Tickers with any news: {with_news}/{len(tickers)}")
    print(f"  Tickers with non-zero daily S: {nz}/{len(tickers)}")


if __name__ == "__main__":
    main()
