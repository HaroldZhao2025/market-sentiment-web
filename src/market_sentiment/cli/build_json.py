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
from market_sentiment.finbert import FinBERT
from market_sentiment.news import (
    fetch_news,
    _prov_finnhub_daily,
    _prov_yfinance_all,
)
from market_sentiment.prices import fetch_prices_yf
from market_sentiment.writers import write_outputs


# -------------------------------
# FinBERT helpers
# -------------------------------

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


# -------------------------------
# Prices
# -------------------------------

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


# -------------------------------
# Diagnostics writers
# -------------------------------

def _write_diag_per_ticker(
    out_dir: str,
    ticker: str,
    start: str,
    end: str,
    df_fh: pd.DataFrame,
    df_yf: pd.DataFrame,
    merged_scored: pd.DataFrame,
) -> Dict:
    """Write <out>/_diag/<TICKER>.json for inspection."""
    diag_dir = f"{out_dir}/_diag"
    os = __import__("os")
    os.makedirs(diag_dir, exist_ok=True)

    def _count_days(df: pd.DataFrame) -> int:
        return 0 if df is None or df.empty else df["ts"].dt.date.nunique()

    def _by_day_counts(df: pd.DataFrame) -> Dict[str, int]:
        if df is None or df.empty:
            return {}
        s = df.copy()
        s["d"] = s["ts"].dt.date.astype(str)
        return s.groupby("d")["title"].size().to_dict()

    def _first_last_ts(df: pd.DataFrame) -> Tuple[str, str]:
        if df is None or df.empty:
            return "", ""
        return (
            str(pd.to_datetime(df["ts"].min()).tz_convert("UTC")),
            str(pd.to_datetime(df["ts"].max()).tz_convert("UTC")),
        )

    fh_first, fh_last = _first_last_ts(df_fh)
    yf_first, yf_last = _first_last_ts(df_yf)
    mg_first, mg_last = _first_last_ts(merged_scored)

    doc = {
        "ticker": ticker,
        "period": {"start": start, "end": end},
        "counts": {
            "finnhub_rows": 0 if df_fh is None or df_fh.empty else int(len(df_fh)),
            "finnhub_days": _count_days(df_fh),
            "yfinance_rows": 0 if df_yf is None or df_yf.empty else int(len(df_yf)),
            "yfinance_days": _count_days(df_yf),
            "merged_rows": 0 if merged_scored is None or merged_scored.empty else int(len(merged_scored)),
            "merged_days": _count_days(merged_scored),
        },
        "ranges": {
            "finnhub_first_ts": fh_first,
            "finnhub_last_ts": fh_last,
            "yfinance_first_ts": yf_first,
            "yfinance_last_ts": yf_last,
            "merged_first_ts": mg_first,
            "merged_last_ts": mg_last,
        },
        "by_day": {
            "finnhub": _by_day_counts(df_fh),
            "yfinance": _by_day_counts(df_yf),
            "merged": _by_day_counts(merged_scored),
        },
        # Light preview of the last few merged headlines:
        "merged_tail": [] if merged_scored is None or merged_scored.empty
        else merged_scored.sort_values("ts").tail(5)[["ts", "title", "url"]].assign(
            ts=lambda d: d["ts"].dt.strftime("%Y-%m-%d %H:%M:%S%z")
        ).to_dict(orient="records"),
    }

    path = f"{diag_dir}/{ticker}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)

    return doc


# -------------------------------
# Main
# -------------------------------

def main():
    import os
    p = argparse.ArgumentParser("Build JSON artifacts for the web app")
    p.add_argument("--universe", required=True, help="CSV with 'ticker' column (first col accepted)")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--out", required=True, help="Output dir: apps/web/public/data")
    p.add_argument("--batch", type=int, default=16)
    p.add_argument("--cutoff-minutes", type=int, default=5)
    p.add_argument("--max-tickers", type=int, default=0)
    p.add_argument("--max-workers", type=int, default=8)
    p.add_argument("--yfinance-count", type=int, default=240,
                   help="Number of yahoo finance items to request (max 240).")
    p.add_argument("--finnhub-rps", type=int, default=10,
                   help="Requests per second for Finnhub day-by-day fetch (<=30).")
    a = p.parse_args()

    # Make Finnhub RPS available to provider
    os.environ["FINNHUB_RPS"] = str(max(1, min(a.finnhub_rps, 30)))

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

    # FinBERT (graceful if load fails)
    try:
        fb = FinBERT()
    except Exception:
        fb = None

    all_diag = []
    news_all: List[pd.DataFrame] = []

    for t in tickers:
        # 1) Pull providers explicitly to diagnose
        try:
            df_fh = _prov_finnhub_daily(t, a.start, a.end, keep_source=True)
        except Exception:
            df_fh = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "src"])

        try:
            df_yf = _prov_yfinance_all(t, a.start, a.end, count=a.yfinance_count, keep_source=True)
        except Exception:
            df_yf = pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "src"])

        # 2) Merge (Finnhub + yfinance) and de-dup
        if len(df_fh) or len(df_yf):
            merged = pd.concat([df_fh, df_yf], ignore_index=True)
            merged["url"] = merged["url"].fillna("")
            merged = (
                merged
                .drop_duplicates(["title", "url"])
                .sort_values("ts")
                .reset_index(drop=True)
            )
            merged = merged[
                (merged["ts"] >= pd.to_datetime(a.start, utc=True)) &
                (merged["ts"] <= pd.to_datetime(a.end, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1))
            ]
        else:
            # Shouldn't happen, but keep a single-API convenience path:
            merged = fetch_news(t, a.start, a.end, keep_source=True)

        days = 0 if merged.empty else merged["ts"].dt.date.nunique()
        print(
            f"  {t}: finnhub_rows={len(df_fh):4d}, "
            f"yfinance_rows={len(df_yf):4d} | merged_rows={len(merged):4d} | days={days}"
        )

        # 3) Score with FinBERT (4 decimals)
        scored = merged.drop(columns=["src"], errors="ignore").copy()
        scored = _score_rows_inplace(fb, scored, text_col="text", batch=a.batch)
        news_all.append(scored)

        # 4) Diagnostics
        doc = _write_diag_per_ticker(
            a.out, t, a.start, a.end, df_fh=df_fh, df_yf=df_yf, merged_scored=scored
        )
        all_diag.append(doc)

    # Merge all news
    news_rows = (
        pd.concat(news_all, ignore_index=True)
        if news_all else
        pd.DataFrame(columns=["ticker", "ts", "title", "url", "text", "S"])
    )

    # Daily aggregation (4 decimals)
    d_news = daily_sentiment_from_rows(news_rows, "news", cutoff_minutes=a.cutoff_minutes)
    if not d_news.empty:
        d_news["date"] = pd.to_datetime(d_news["date"], utc=True, errors="coerce").dt.tz_localize(None)
        for c in ("S_NEWS", "S"):
            if c in d_news.columns:
                d_news[c] = pd.to_numeric(d_news[c], errors="coerce").fillna(0.0).round(4)

    # Earnings placeholder
    d_earn = pd.DataFrame(columns=["date", "ticker", "S_EARN"])

    daily = join_and_fill_daily(d_news, d_earn)
    if "S" not in daily.columns:
        daily["S_NEWS"] = pd.to_numeric(daily.get("S_NEWS", 0.0), errors="coerce").fillna(0.0)
        daily["S_EARN"] = pd.to_numeric(daily.get("S_EARN", 0.0), errors="coerce").fillna(0.0)
        daily["S"] = (daily["S_NEWS"] + daily["S_EARN"]).round(4)

    panel = prices.merge(daily, on=["date", "ticker"], how="left")
    for c in ("S", "S_NEWS", "S_EARN"):
        if c not in panel.columns:
            panel[c] = 0.0
        panel[c] = pd.to_numeric(panel[c], errors="coerce").fillna(0.0).round(4)

    # Write site data (writers keeps only 10 headlines in UI JSON; that's OK)
    write_outputs(panel, news_rows, None, a.out)

    # Also write a compact run summary diagnostics file
    diag_summary_path = f"{a.out}/_diag/_summary.json"
    os = __import__("os")
    os.makedirs(f"{a.out}/_diag", exist_ok=True)
    with open(diag_summary_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "period": {"start": a.start, "end": a.end},
                "tickers": [d["ticker"] for d in all_diag],
                "by_ticker_counts": {
                    d["ticker"]: d["counts"] for d in all_diag
                },
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    tickers_list, have_files, with_news, with_nonzero_s = _summarize_from_files(a.out)
    print("Summary:")
    print(f"  Tickers listed: {len(tickers_list)}")
    print(f"  Ticker JSON files: {have_files}")
    print(f"  Tickers with any news: {with_news}/{len(tickers_list)}")
    print(f"  Tickers with non-zero daily S: {with_nonzero_s}/{len(tickers_list)}")


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


if __name__ == "__main__":
    main()
