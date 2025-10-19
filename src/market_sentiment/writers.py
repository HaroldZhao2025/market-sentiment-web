# src/market_sentiment/writers.py
from __future__ import annotations

import json
import math
import os
from typing import Iterable, List, Dict, Any, Tuple

import pandas as pd


def _to_datestr(x) -> str:
    try:
        ts = pd.to_datetime(x, utc=True, errors="coerce")
        if pd.isna(ts):
            return ""
        # we only need the date string (App charts use YYYY-MM-DD)
        return ts.tz_convert("UTC").strftime("%Y-%m-%d") if ts.tzinfo else ts.strftime("%Y-%m-%d")
    except Exception:
        return ""


def _safe_num(x) -> float:
    try:
        f = float(x)
        if math.isfinite(f):
            return f
        return 0.0
    except Exception:
        return 0.0


def _roll_ma(arr: List[float], n: int = 7) -> List[float]:
    out: List[float] = []
    run = 0.0
    q: List[float] = []
    for v in arr:
        v = 0.0 if v is None or (isinstance(v, float) and math.isnan(v)) else float(v)
        q.append(v)
        run += v
        if len(q) > n:
            run -= q.pop(0)
        out.append(run / n if len(q) >= n else float("nan"))
    return out


def _ensure_cols(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    """Create missing columns with NA to keep downstream code simple."""
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df


def _coerce_panel(panel: pd.DataFrame) -> pd.DataFrame:
    """
    Expect at minimum: date, ticker, close.
    Optional: open, S (daily sentiment).
    """
    if panel is None or panel.empty:
        return pd.DataFrame(columns=["date", "ticker", "open", "close", "S"])

    df = panel.copy()

    # Standardize column names a bit (lowercase aliases)
    lower = {str(c).lower(): c for c in df.columns}
    date_col = lower.get("date") or "date"
    tick_col = lower.get("ticker") or "ticker"
    close_col = lower.get("close") or lower.get("adj close") or lower.get("adj_close") or "close"
    open_col = lower.get("open") or "open"

    # Keep a narrow set of columns; create missing
    df = df.rename(columns={
        date_col: "date",
        tick_col: "ticker",
        close_col: "close",
        open_col: "open",
    })
    _ensure_cols(df, ["date", "ticker", "open", "close"])

    # Ensure dtypes
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["open"] = pd.to_numeric(df["open"], errors="coerce")

    # If panel already carries daily S (awesome), keep it; otherwise create 0s (neutral)
    if "S" in df.columns:
        df["S"] = pd.to_numeric(df["S"], errors="coerce").fillna(0.0)
    else:
        df["S"] = 0.0

    # Drop rows with no date or no ticker or no close
    df = df.dropna(subset=["date", "ticker", "close"])

    # Sort
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df[["date", "ticker", "open", "close", "S"]]


def _coerce_news(news: pd.DataFrame) -> pd.DataFrame:
    if news is None or news.empty:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    df = news.copy()
    # standardize columns (accept minor aliasing)
    lower = {str(c).lower(): c for c in df.columns}
    df = df.rename(columns={
        lower.get("ticker") or "ticker": "ticker",
        lower.get("ts") or lower.get("time") or lower.get("date"): "ts",
        lower.get("title") or "title": "title",
        lower.get("url") or "link": "url",
        lower.get("text") or lower.get("summary") or "content": "text",
    })
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df["title"] = df["title"].astype(str)
    df["url"] = df["url"].astype(str)
    df["text"] = df.get("text", pd.Series([""] * len(df))).astype(str)
    df = df.dropna(subset=["ticker", "ts", "title"]).sort_values(["ticker", "ts"])
    return df[["ticker", "ts", "title", "url", "text"]]


def _union_tickers(panel: pd.DataFrame, news: pd.DataFrame) -> List[str]:
    a = set((panel["ticker"].dropna().unique().tolist()) if not panel.empty else [])
    b = set((news["ticker"].dropna().unique().tolist()) if not news.empty else [])
    return sorted(t for t in (a | b) if isinstance(t, str) and t)


def _one_ticker_obj(df_t: pd.DataFrame, news_t: pd.DataFrame) -> Dict[str, Any]:
    """
    Build the per-ticker JSON object consumed by the web app.
    """
    df_t = df_t.sort_values("date").reset_index(drop=True)

    # Basic arrays
    dates = [_to_datestr(x) for x in df_t["date"].tolist()]
    price = [_safe_num(x) for x in df_t["close"].tolist()]
    s = [_safe_num(x) for x in df_t["S"].tolist()]

    # 7-day MA on S
    s_ma7 = _roll_ma(s, n=7)

    # News: latest 300 (enough for UI)
    news_rows = []
    if news_t is not None and not news_t.empty:
        for _, r in news_t.tail(300).iterrows():
            news_rows.append({
                "ts": pd.to_datetime(r["ts"], utc=True, errors="coerce").isoformat(),
                "title": str(r["title"]),
                "url": str(r["url"]),
                "text": str(r.get("text", "")),
            })

    return {
        "date": dates,
        "close": price,
        "S": s,
        "S_MA7": s_ma7,
        "news": news_rows,
    }


def _write_json(obj: Any, path_out: str) -> None:
    os.makedirs(os.path.dirname(path_out), exist_ok=True)
    with open(path_out, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)


def _portfolio_from_panel(panel: pd.DataFrame, tickers: List[str]) -> Dict[str, Any]:
    """
    Equal-weight daily portfolio from S across all tickers.
    If a ticker has no S on a day, treat as 0 (neutral).
    """
    if panel.empty or not tickers:
        return {"dates": [], "S": [], "S_MA7": [], "count": []}

    # Build a pivot: rows=date, cols=ticker, values=S (fillna=0)
    pvt = panel.pivot_table(index="date", columns="ticker", values="S", aggfunc="mean")
    pvt = pvt.reindex(columns=tickers)  # consistent order
    pvt = pvt.fillna(0.0)

    dates = [_to_datestr(d) for d in pvt.index]
    s = _safe_list(pvt.mean(axis=1).tolist())
    count = (pvt.notna()).sum(axis=1).astype(int).tolist()
    s_ma7 = _roll_ma(s, n=7)

    return {"dates": dates, "S": s, "S_MA7": s_ma7, "count": count}


def _safe_list(xs: Iterable[Any]) -> List[float]:
    return [_safe_num(x) for x in xs]


def write_outputs(
    panel: pd.DataFrame,
    news_rows: pd.DataFrame,
    out_dir: str,
) -> Tuple[List[str], int]:
    """
    Minimal, robust writer.

    - Always writes: public/data/_tickers.json
    - Writes one JSON per ticker: public/data/ticker/<TICKER>.json
    - Writes: public/data/portfolio.json (equal-weight from S)

    Returns:
        (tickers_written, n_files)
    """
    out_dir = os.fspath(out_dir)
    os.makedirs(os.path.join(out_dir, "ticker"), exist_ok=True)

    panel = _coerce_panel(panel)
    news_rows = _coerce_news(news_rows)

    # union of tickers present in *either* prices panel or news feeds
    tickers = _union_tickers(panel, news_rows)

    # per-ticker JSON
    n_files = 0
    for t in tickers:
        df_t = panel.loc[panel["ticker"] == t].copy()
        nt = news_rows.loc[news_rows["ticker"] == t].copy()

        if df_t.empty and nt.empty:
            continue  # nothing to write

        # If we have only news (no prices), synthesize a minimal date series
        if df_t.empty and not nt.empty:
            # Use unique news dates; price is empty list
            dates = sorted({_to_datestr(x) for x in nt["ts"] if pd.notna(x)})
            obj = {
                "date": dates,
                "close": [],
                "S": [],
                "S_MA7": [],
                "news": [
                    {
                        "ts": pd.to_datetime(r["ts"], utc=True, errors="coerce").isoformat(),
                        "title": str(r["title"]),
                        "url": str(r["url"]),
                        "text": str(r.get("text", "")),
                    }
                    for _, r in nt.tail(300).iterrows()
                ],
            }
        else:
            obj = _one_ticker_obj(df_t, nt)

        _write_json(obj, os.path.join(out_dir, "ticker", f"{t}.json"))
        n_files += 1

    # tickers list
    _write_json(tickers, os.path.join(out_dir, "_tickers.json"))

    # portfolio (from panel only; if empty, write an empty portfolio)
    portfolio = _portfolio_from_panel(panel, tickers)
    _write_json(portfolio, os.path.join(out_dir, "portfolio.json"))

    return tickers, n_files
