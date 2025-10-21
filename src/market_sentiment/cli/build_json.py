from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple

import numpy as np
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


# ---------------- helpers ----------------

def _score_texts(fb: FinBERT, texts: List[str], batch: int) -> List[float]:
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


def _fetch_all_prices(tickers: List[str], start: str, end: str, max_workers: int) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = []
        for t in tickers:
            futs.append(ex.submit(fetch_prices_yf, t, start, end))
            time.sleep(0.12)  # gentle on YF
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
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).get_info() or {}
        nm = info.get("longName") or info.get("shortName")
        if nm and 2 <= len(nm) <= 80:
            return str(nm).strip()
    except Exception:
        pass
    return None


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


# ---------- make a dense fallback curve per (ticker, day) based on news counts ----------

def _gaussian_kernel(days: int = 7, sigma: float = 2.0) -> np.ndarray:
    L = max(3, int(days) | 1)  # odd
    r = (L - 1) // 2
    x = np.arange(-r, r + 1, dtype=float)
    k = np.exp(-(x**2) / (2.0 * float(sigma) ** 2))
    k /= k.sum()
    return k


def _dense_fallback_from_news(prices: pd.DataFrame, news_rows: pd.DataFrame) -> pd.DataFrame:
    """
    For every trading day in `prices` and each ticker, produce S_NEWS_FALLBACK in [-1,1],
    using (day-level) news counts -> gaussian smoothing -> zscore -> tanh.
    """
    if prices is None or prices.empty:
        return pd.DataFrame(columns=["date", "ticker", "S_NEWS_FALLBACK"])
    out = []
    for t, g in prices.groupby("ticker"):
        g = g.sort_values("date")
        days = pd.DatetimeIndex(pd.to_datetime(g["date"], utc=True, errors="coerce").dt.floor("D"))
        if days.empty:
            continue

        sub = news_rows[news_rows["ticker"].astype(str).str.upper() == t] if news_rows is not None else pd.DataFrame()
        if sub is None or sub.empty:
            svals = np.zeros(len(days), dtype=float)
        else:
            nr = sub.copy()
            nr["ts"] = pd.to_datetime(nr["ts"], utc=True, errors="coerce")
            nr = nr.dropna(subset=["ts"])
            cnt = (
                nr.assign(day=nr["ts"].dt.floor("D"))
                  .groupby("day", as_index=False)
                  .size()
                  .rename(columns={"size": "cnt"})
                  .set_index("day")["cnt"]
            )
            aligned = cnt.reindex(days, fill_value=0).astype(float)
            k = _gaussian_kernel(7, 2.0)
            sm = np.convolve(aligned.values, k, mode="same")
            mu, sd = float(np.mean(sm)), float(np.std(sm))
            if sd <= 1e-12:
                z = np.zeros_like(sm)
            else:
                z = (sm - mu) / sd
            svals = np.tanh(z / 2.0).clip(-1.0, 1.0)
        out.append(pd.DataFrame({"date": g["date"], "ticker": t, "S_NEWS_FALLBACK": svals}))
    if not out:
        return pd.DataFrame(columns=["date", "ticker", "S_NEWS_FALLBACK"])
    return pd.concat(out, ignore_index=True)


# ---------------- main ----------------

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
    a = p.parse_args()

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
            # ask many items across providers (headlines list still capped to 10 in writers)
            n = fetch_news(t, a.start, a.end, company=comp, max_per_provider=500)
            if n is None or n.empty:
                n = fetch_news(t, a.start, a.end, company=None, max_per_provider=500)
        except Exception:
            n = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])

        if n is None or n.empty:
            print(f"  {t}: news rows=0 | days=0 | company={'-' if not comp else comp}")
        else:
            dcount = n["ts"].dt.date.nunique() if "ts" in n.columns else 0
            print(f"  {t}: news rows={len(n)} | days={dcount} | company={'-' if not comp else comp}")

        n = _score_rows_inplace(fb, n, text_col="text", batch=a.batch)
        news_all.append(n)

    news_rows = (
        pd.concat(news_all, ignore_index=True)
        if news_all else
        pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])
    )

    # -------- Aggregate daily sentiment (FinBERT day scores) --------
    d_news = daily_sentiment_from_rows(news_rows, "news", cutoff_minutes=a.cutoff_minutes)

    # dense fallback using full price window (per ticker/day)
    dense_fb = _dense_fallback_from_news(prices, news_rows)

    # combine: prefer FinBERT day S if non-zero else use fallback
    if d_news is None or d_news.empty:
        combined = dense_fb.rename(columns={"S_NEWS_FALLBACK": "S"})  # only fallback
    else:
        d_news = d_news.copy()
        d_news["date"] = pd.to_datetime(d_news["date"], utc=True, errors="coerce").dt.tz_localize(None)
        dense_fb["date"] = pd.to_datetime(dense_fb["date"], utc=True, errors="coerce").dt.tz_localize(None)
        combined = prices[["date", "ticker"]].merge(d_news, on=["date", "ticker"], how="left")
        combined = combined.merge(dense_fb, on=["date", "ticker"], how="left")
        s = pd.to_numeric(combined.get("S_NEWS", combined.get("S", 0.0)), errors="coerce").fillna(0.0)
        fb = pd.to_numeric(combined.get("S_NEWS_FALLBACK", 0.0), errors="coerce").fillna(0.0)
        # if abs(FinBERT S) <= 1e-12 -> use fallback; else keep FinBERT
        combined["S"] = np.where(s.abs() > 1e-12, s, fb)
        combined = combined[["date", "ticker", "S"]]

    # -------- Build panel + write outputs --------
    panel = prices.merge(combined, on=["date", "ticker"], how="left")
    for c in ("S",):
        if c not in panel.columns:
            panel[c] = 0.0
        panel[c] = pd.to_numeric(panel[c], errors="coerce").fillna(0.0)

    # earnings placeholder to satisfy writer signature
    earn_rows = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])

    write_outputs(panel, news_rows, earn_rows, a.out)

    tickers_list, have_files, with_news, with_nonzero_s = _summarize_from_files(a.out)
    print("Summary:")
    print(f"  Tickers listed: {len(tickers_list)}")
    print(f"  Ticker JSON files: {have_files}")
    print(f"  Tickers with any news: {with_news}/{len(tickers_list)}")
    print(f"  Tickers with non-zero daily S: {with_nonzero_s}/{len(tickers_list)}")


if __name__ == "__main__":
    main()
