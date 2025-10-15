# src/market_sentiment/cli/build_json.py
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List, Dict, Any

import numpy as np
import pandas as pd
from tqdm import tqdm

from market_sentiment.finbert import FinBERT  # must expose .score(texts, batch=...)
from market_sentiment.news import fetch_news_yf
from market_sentiment.edgar import fetch_earnings_docs
from market_sentiment.prices import fetch_prices_yf
from market_sentiment.aggregate import (
    daily_sentiment_from_rows,
    combine_daily,
    add_forward_returns,
    safe_merge_prices_daily,
    _pick_close_column,  # <-- fix: import the helper we call below
)
from market_sentiment.portfolio import daily_long_short

# ---------- helpers ----------

def _ensure_prices_frame(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Fetch prices and normalize to columns: ['date','ticker','close', ...], with UTC datetimes.
    """
    p = fetch_prices_yf(ticker, start, end)
    if p is None or len(p) == 0:
        return pd.DataFrame(columns=["date", "ticker", "close"])

    df = p.copy()

    # If yfinance returns a DateTimeIndex, surface it
    if "date" not in df.columns:
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index().rename(columns={"index": "date", "Date": "date"})
        else:
            # last resort
            df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)

    # Standardize column names to lowercase; keep 'close' present
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    if "ticker" not in df.columns:
        df["ticker"] = ticker

    # Ensure TZ-aware UTC datetimes
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)

    # Make sure there's a close-like column
    try:
        close_col = _pick_close_column(df)
    except KeyError:
        # If only 'adj_close' exists, synthesize 'close' from it so downstream never breaks
        if "adj_close" in df.columns:
            df["close"] = pd.to_numeric(df["adj_close"], errors="coerce")
        else:
            # empty
            return pd.DataFrame(columns=["date", "ticker", "close"])

    # keep a consistent set
    if "close" not in df.columns:
        df["close"] = pd.to_numeric(df[_pick_close_column(df)], errors="coerce")
    df = df.dropna(subset=["close"])

    # keep minimal core + anything else the app might use
    core_cols = ["date", "ticker", "close"]
    extras = [c for c in df.columns if c not in core_cols]
    return df[core_cols + extras]


def _score_rows_inplace(fb: FinBERT, df: pd.DataFrame, text_col: str, batch: int) -> pd.DataFrame:
    """
    Score df[text_col] with FinBERT, write a float column 'S' in [-1,1].
    Expects fb.score(texts, batch=...) to return floats or a list of dicts with 'S'.
    """
    if df is None or len(df) == 0:
        return df
    texts = df[text_col].fillna("").astype(str).tolist()
    if not texts:
        df["S"] = np.nan
        return df

    scores = fb.score(texts, batch=batch)  # your finbert.py API
    # Normalize to a plain list of floats
    if isinstance(scores, dict) and "S" in scores:
        vals = [float(scores["S"])]
    elif isinstance(scores, list):
        # elements may be floats or dicts
        tmp = []
        for s in scores:
            if isinstance(s, dict) and "S" in s:
                tmp.append(float(s["S"]))
            else:
                tmp.append(float(s))
        vals = tmp
    else:
        # single float
        vals = [float(scores)]
    # Length guard
    if len(vals) != len(df):
        # pad/truncate just in case (shouldn't happen)
        if len(vals) < len(df):
            vals = vals + [np.nan] * (len(df) - len(vals))
        else:
            vals = vals[: len(df)]
    df = df.copy()
    df["S"] = vals
    return df


def _top_news_json(df: pd.DataFrame, k: int = 12) -> List[Dict[str, Any]]:
    """
    Select top-k absolute sentiment news items with fields the UI expects.
    """
    if df is None or len(df) == 0:
        return []
    d = df.copy()
    d["absS"] = d["S"].abs()
    d = d.sort_values("absS", ascending=False).head(k)
    out = []
    for _, r in d.iterrows():
        ts = pd.to_datetime(r["ts"], errors="coerce", utc=True)
        out.append(
            {
                "ts": (ts.tz_convert("America/New_York").strftime("%Y-%m-%d %H:%M:%S %Z") if pd.notna(ts) else None),
                "title": str(r.get("title", ""))[:500],
                "url": str(r.get("url", "")),
                "S": float(r.get("S", 0.0)),
                "source": str(r.get("source", "")) if "source" in d.columns else "news",
            }
        )
    return out


def _series_json(prices: pd.DataFrame, daily: pd.DataFrame) -> Dict[str, Any]:
    """
    Build the time-series block for a ticker page.
    """
    # merge price & S on date
    ser = (
        prices[["date", "close"]]
        .merge(daily[["date", "S"]], on="date", how="left")
        .sort_values("date")
        .reset_index(drop=True)
    )
    ser["S"] = pd.to_numeric(ser["S"], errors="coerce").fillna(0.0)
    ser["ma7"] = ser["S"].rolling(7, min_periods=1).mean()

    dates = pd.to_datetime(ser["date"], utc=True).dt.tz_convert("America/New_York").dt.strftime("%Y-%m-%d").tolist()
    return {
        "date": dates,
        "price": ser["close"].astype(float).round(6).tolist(),
        "sentiment": ser["S"].astype(float).round(6).tolist(),
        "sentiment_ma7": ser["ma7"].astype(float).round(6).tolist(),
    }


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)


# ---------- CLI main ----------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser("Build JSON artifacts for the web app")
    p.add_argument("--universe", required=True, type=Path)
    p.add_argument("--start", required=True, type=str)
    p.add_argument("--end", required=True, type=str)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--batch", type=int, default=16)
    p.add_argument("--cutoff-minutes", type=int, default=5, help="Minutes before 16:00 ET that roll to T+1")
    p.add_argument("--max-tickers", type=int, default=0, help="0 = all tickers; otherwise first N")
    return p.parse_args()


def _read_universe(path: Path) -> List[str]:
    u = pd.read_csv(path)
    if "ticker" in u.columns:
        tickers = u["ticker"].astype(str).str.upper().tolist()
    elif "Symbol" in u.columns:
        tickers = u["Symbol"].astype(str).str.upper().tolist()
    else:
        # fallback: first column
        c0 = u.columns[0]
        tickers = u[c0].astype(str).str.upper().tolist()
    return tickers


def main():
    a = parse_args()
    out_dir = Path(a.out)

    tickers = _read_universe(a.universe)
    if a.max_tickers and a.max_tickers > 0:
        tickers = tickers[: a.max_tickers]

    print(f"Build JSON: 0/{len(tickers)} tickers")

    fb = FinBERT()

    all_prices: List[pd.DataFrame] = []
    all_daily: List[pd.DataFrame] = []
    panel_rows: List[pd.DataFrame] = []

    news_stats = {"has_news": 0, "has_nonzero_S": 0}

    for t in tqdm(tickers, desc="Build JSON"):
        # 1) prices
        p = _ensure_prices_frame(t, a.start, a.end)
        if p.empty:
            # still create an empty file so the web doesn't 404
            _write_json(out_dir / "ticker" / f"{t}.json", {"symbol": t, "series": {"date": [], "price": [], "sentiment": [], "sentiment_ma7": []}, "top_news": []})
            continue

        # 2) news
        news_rows = fetch_news_yf(t, a.start, a.end)
        if not news_rows.empty:
            news_rows = _score_rows_inplace(fb, news_rows, text_col="text", batch=a.batch)
            # per-day aggregation
            d_news = daily_sentiment_from_rows(news_rows, kind="news", cutoff_minutes=a.cutoff_minutes)
            # stats
            news_stats["has_news"] += 1
            if (news_rows["S"].abs() > 0).any():
                news_stats["has_nonzero_S"] += 1
        else:
            d_news = pd.DataFrame(columns=["date", "ticker", "S_news", "news_count"])

        # 3) earnings (SEC)
        earn_rows = fetch_earnings_docs(t, a.start, a.end)
        if not earn_rows.empty:
            earn_rows = _score_rows_inplace(fb, earn_rows, text_col="text", batch=a.batch)
            d_earn = daily_sentiment_from_rows(earn_rows, kind="earn", cutoff_minutes=a.cutoff_minutes)
        else:
            d_earn = pd.DataFrame(columns=["date", "ticker", "S_earn", "earn_count"])

        # 4) combine daily + attach prices + forward returns (for portfolio later)
        d = combine_daily(d_news, d_earn)
        prices_with_ret = add_forward_returns(p)

        merged = safe_merge_prices_daily(prices_with_ret, d)
        # Keep for portfolio (date/ticker/S/ret_cc_1d only)
        panel_rows.append(merged[["date", "ticker", "S", "ret_cc_1d"]])

        # 5) write per-ticker JSON that the site reads
        series = _series_json(prices_with_ret, d)
        top_news = _top_news_json(pd.concat([news_rows, earn_rows], ignore_index=True, sort=False) if not news_rows.empty or not earn_rows.empty else pd.DataFrame())

        ticker_obj = {
            "symbol": t,
            "series": series,
            "top_news": top_news,
        }
        _write_json(out_dir / "ticker" / f"{t}.json", ticker_obj)

        # collect for summary stats
        all_prices.append(prices_with_ret[["date", "ticker", "close"]])
        all_daily.append(d)

    # 6) write tickers list
    _write_json(out_dir / "_tickers.json", tickers)

    # 7) build portfolio from panel
    panel = pd.concat(panel_rows, ignore_index=True) if panel_rows else pd.DataFrame(columns=["date", "ticker", "S", "ret_cc_1d"])
    port = daily_long_short(panel, long_quantile=0.9, short_quantile=0.1)
    _write_json(out_dir / "portfolio.json", port)

    # 8) summary for CI logs
    daily = pd.concat(all_daily, ignore_index=True) if all_daily else pd.DataFrame(columns=["date", "ticker", "S", "S_news", "S_earn", "news_count", "earn_count"])
    n_tickers = len(tickers)
    n_has_news = news_stats["has_news"]
    n_nonzero = news_stats["has_nonzero_S"]

    print(f"Tickers listed: {n_tickers}")
    print(f"Tickers with any news: {n_has_news}/{n_tickers}")
    print(f"Tickers with non-zero sentiment S: {n_nonzero}/{n_tickers}")

    if not daily.empty:
        summary = (
            daily.groupby("ticker", as_index=False)["S"].apply(lambda s: float(np.nanmean(np.abs(s))))
                  .rename(columns={"S": "mean_abs_S"})
                  .sort_values("mean_abs_S", ascending=False)
        )
        top = summary.head(5)
        print("Sample tickers with mean|S| > 0:")
        for _, r in top.iterrows():
            tk = r["ticker"]
            nz = int((daily[daily["ticker"] == tk]["S"].abs() > 0).sum())
            print(f"  {tk}: mean|S|={r['mean_abs_S']:.4f} (nz_points={nz})")
    else:
        print("No daily sentiment was generated.")

if __name__ == "__main__":
    main()
