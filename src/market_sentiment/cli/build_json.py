# src/market_sentiment/cli/build_json.py
from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import pandas as pd

from market_sentiment.aggregate import (
    add_forward_returns,
    daily_sentiment_from_rows,
    join_and_fill_daily,
    _ensure_date_dtype,
)
from market_sentiment.finbert import FinBERT  # optional — handled defensively
from market_sentiment.news import fetch_news
from market_sentiment.news_finnhub_daily import fetch_finnhub_daily
from market_sentiment.news_yfinance import fetch_yfinance_recent
from market_sentiment.prices import fetch_prices_yf
from market_sentiment.writers import write_outputs


# ------------------------------- FinBERT helpers ------------------------------

def _score_texts(fb: FinBERT, texts: List[str], batch: int) -> List[float]:
    """Handle FinBERT.score signature differences across versions."""
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
    return out


# ------------------------------- Prices ---------------------------------------

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
    prices = _ensure_date_dtype(prices, "date")  # -> naive date
    prices = add_forward_returns(prices)
    return prices


# ------------------------------- Diagnostics ----------------------------------

def _diag_emit_ticker(diag: Dict, ticker: str, fh_rows: int, fh_days: int, yf_rows: int, yf_days: int, merged_rows: int, merged_days: int):
    diag["tickers"][ticker] = {
        "finnhub_rows": fh_rows,
        "finnhub_days": fh_days,
        "yfinance_rows": yf_rows,
        "yfinance_days": yf_days,
        "merged_rows": merged_rows,
        "merged_days": merged_days,
    }

def _save_json(obj, path: str) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2, ensure_ascii=False, default=str)
    except Exception:
        pass


# ------------------------------- Main -----------------------------------------

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

    # NEW flags (replace old --news-per-provider)
    p.add_argument("--yfinance-count", type=int, default=240, help="How many Yahoo Finance items to request per ticker.")
    p.add_argument("--finnhub-rps", type=int, default=10, help="Finnhub requests per second (≤30).")
    p.add_argument("--diagnostics", action="store_true", help="Write diagnostics.json with provider coverage by ticker.")

    a = p.parse_args()

    # Universe
    uni = pd.read_csv(a.universe)
    if "ticker" not in uni.columns:
        uni = uni.rename(columns={uni.columns[0]: "ticker"})
    tickers = sorted(str(x).strip().upper() for x in uni["ticker"].dropna().unique().tolist())
    if a.max_tickers and a.max_tickers > 0:
        tickers = tickers[: a.max_tickers]

    print(
        f"Build JSON for {len(tickers)} tickers"
    )

    # -------- Prices --------
    prices = _fetch_all_prices(tickers, a.start, a.end, max_workers=a.max_workers)
    if prices is None or prices.empty:
        raise RuntimeError("No prices downloaded.")
    print(f"  ✓ Prices for {prices['ticker'].nunique()} tickers, rows={len(prices)}")

    # -------- News (Finnhub daily + yfinance recent) --------
    try:
        fb = FinBERT()
    except Exception:
        fb = None

    diag = {"tickers": {}, "start": a.start, "end": a.end}
    news_all: List[pd.DataFrame] = []

    for t in tickers:
        # Providers (exact requirements)
        df_fh = fetch_finnhub_daily(t, a.start, a.end, rps=int(a.finnhub_rps))
        df_yf = fetch_yfinance_recent(t, a.start, a.end, count=int(a.yfinance_count))

        fh_days = 0 if df_fh.empty else df_fh["ts"].dt.date.nunique()
        yf_days = 0 if df_yf.empty else df_yf["ts"].dt.date.nunique()

        merged = pd.concat([d for d in (df_fh, df_yf) if not d.empty], ignore_index=True) if (not df_fh.empty or not df_yf.empty) else pd.DataFrame(columns=["ticker","ts","title","url","text"])
        merged["url"] = merged["url"].fillna("")
        merged = merged.drop_duplicates(["title", "url"]).sort_values("ts").reset_index(drop=True)
        m_days = 0 if merged.empty else merged["ts"].dt.date.nunique()

        print(f"  {t}: finnhub rows={len(df_fh)} days={fh_days} | yfinance rows={len(df_yf)} days={yf_days} | merged rows={len(merged)} days={m_days}")
        _diag_emit_ticker(diag, t, len(df_fh), fh_days, len(df_yf), yf_days, len(merged), m_days)

        # FinBERT scoring (4-decimal in logs)
        scored = _score_rows_inplace(fb, merged, text_col="text", batch=a.batch)
        if not scored.empty:
            s = pd.to_numeric(scored["S"], errors="coerce").fillna(0.0)
            print(f"    FinBERT: n={len(s)} mean={s.mean():.4f} min={s.min():.4f} max={s.max():.4f}")
        news_all.append(scored)

    news_rows = (
        pd.concat(news_all, ignore_index=True)
        if news_all else
        pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])
    )

    # Placeholder earnings
    earn_rows = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])

    # -------- Aggregate daily sentiment --------
    d_news = daily_sentiment_from_rows(news_rows, "news", cutoff_minutes=a.cutoff_minutes)
    d_earn = (
        daily_sentiment_from_rows(earn_rows, "earn", cutoff_minutes=a.cutoff_minutes)
        if not earn_rows.empty else
        pd.DataFrame(columns=["date", "ticker", "S_EARN"])
    )

    if not d_news.empty:
        d_news["date"] = pd.to_datetime(d_news["date"], utc=True, errors="coerce").dt.tz_localize(None)
    if not d_earn.empty:
        d_earn["date"] = pd.to_datetime(d_earn["date"], utc=True, errors="coerce").dt.tz_localize(None)

    daily = join_and_fill_daily(d_news, d_earn)
    if "S" not in daily.columns:
        s_news = pd.to_numeric(daily.get("S_NEWS", pd.Series(0.0, index=daily.index)), errors="coerce").fillna(0.0)
        s_earn = pd.to_numeric(daily.get("S_EARN", pd.Series(0.0, index=daily.index)), errors="coerce").fillna(0.0)
        daily["S"] = s_news + s_earn

    # -------- Panel + outputs --------
    panel = prices.merge(daily, on=["date", "ticker"], how="left")
    for c in ("S", "S_NEWS", "S_EARN"):
        if c not in panel.columns:
            panel[c] = 0.0
        panel[c] = pd.to_numeric(panel[c], errors="coerce").fillna(0.0)

    write_outputs(panel, news_rows, earn_rows, a.out)

    if a.diagnostics:
        _save_json(diag, f"{a.out}/diagnostics.json")

    # -------- Summary from outputs --------
    try:
        tickers_list = json.load(open(f"{a.out}/_tickers.json", "r", encoding="utf-8"))
    except Exception:
        tickers_list = []
    have_files = with_news = with_nonzero_s = 0
    for t in tickers_list:
        f = f"{a.out}/ticker/{t}.json"
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

    print("Summary:")
    print(f"  Tickers listed: {len(tickers_list)}")
    print(f"  Ticker JSON files: {have_files}")
    print(f"  Tickers with any news: {with_news}/{len(tickers_list)}")
    print(f"  Tickers with non-zero daily S: {with_nonzero_s}/{len(tickers_list)}")


if __name__ == "__main__":
    main()
