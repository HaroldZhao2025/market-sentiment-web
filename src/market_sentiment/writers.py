# src/market_sentiment/writers.py
from __future__ import annotations

import json
import math
import os
from typing import Iterable, List, Dict, Any, Tuple, Optional

import pandas as pd


# ---------------------------
# Small helpers
# ---------------------------

def _to_datestr(x) -> str:
    try:
        ts = pd.to_datetime(x, utc=True, errors="coerce")
        if pd.isna(ts):
            return ""
        return ts.tz_convert("UTC").strftime("%Y-%m-%d") if ts.tzinfo else ts.strftime("%Y-%m-%d")
    except Exception:
        return ""


def _to_iso(x) -> str:
    try:
        ts = pd.to_datetime(x, utc=True, errors="coerce")
        if pd.isna(ts):
            return ""
        return ts.tz_convert("UTC").isoformat() if ts.tzinfo else ts.tz_localize("UTC").isoformat()
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


def _safe_list(xs: Iterable[Any]) -> List[float]:
    return [_safe_num(x) for x in xs]


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
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df


# ---------------------------
# Normalizers
# ---------------------------

def _coerce_panel(panel: pd.DataFrame) -> pd.DataFrame:
    """Expect: date, ticker, close. Optional: open, S."""
    if panel is None or panel.empty:
        return pd.DataFrame(columns=["date", "ticker", "open", "close", "S"])

    df = panel.copy()

    lower = {str(c).lower(): c for c in df.columns}
    date_col = lower.get("date") or "date"
    tick_col = lower.get("ticker") or "ticker"
    close_col = lower.get("close") or lower.get("adj close") or lower.get("adj_close") or "close"
    open_col = lower.get("open") or "open"

    df = df.rename(columns={
        date_col: "date",
        tick_col: "ticker",
        close_col: "close",
        open_col: "open",
    })
    _ensure_cols(df, ["date", "ticker", "open", "close"])

    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["open"] = pd.to_numeric(df["open"], errors="coerce")

    if "S" in df.columns:
        df["S"] = pd.to_numeric(df["S"], errors="coerce").fillna(0.0)
    else:
        df["S"] = 0.0

    df = df.dropna(subset=["date", "ticker", "close"])
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df[["date", "ticker", "open", "close", "S"]]


def _coerce_news(news: pd.DataFrame) -> pd.DataFrame:
    if news is None or news.empty:
        return pd.DataFrame(columns=["ticker", "ts", "title", "url", "text"])
    df = news.copy()
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


def _coerce_earnings(earn: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Normalize earnings rows if provided. Otherwise return empty frame."""
    if earn is None or isinstance(earn, (list, tuple)) or (hasattr(earn, "empty") and earn.empty):
        return pd.DataFrame(columns=["ticker", "date", "eps", "eps_estimate", "revenue", "revenue_estimate", "surprise"])

    df = earn.copy()
    lower = {str(c).lower(): c for c in df.columns}

    # Try to map common fields; everything is optional
    df = df.rename(columns={
        lower.get("ticker") or "ticker": "ticker",
        lower.get("date") or lower.get("ts") or "date": "date",
        lower.get("eps") or lower.get("eps_actual") or "eps": "eps",
        lower.get("eps_estimate") or lower.get("estimate") or "eps_estimate": "eps_estimate",
        lower.get("revenue") or lower.get("revenue_actual") or "revenue": "revenue",
        lower.get("revenue_estimate") or "revenue_estimate": "revenue_estimate",
        lower.get("surprise") or "surprise": "surprise",
    })

    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    for c in ["eps", "eps_estimate", "revenue", "revenue_estimate", "surprise"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["ticker", "date"])
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    keep = ["ticker", "date", "eps", "eps_estimate", "revenue", "revenue_estimate", "surprise"]
    for c in keep:
        if c not in df.columns:
            df[c] = pd.NA
    return df[keep]


# ---------------------------
# Assembly
# ---------------------------

def _union_tickers(panel: pd.DataFrame, news: pd.DataFrame, earn: pd.DataFrame) -> List[str]:
    a = set(panel["ticker"].dropna().unique().tolist()) if not panel.empty else set()
    b = set(news["ticker"].dropna().unique().tolist()) if not news.empty else set()
    c = set(earn["ticker"].dropna().unique().tolist()) if not earn.empty else set()
    return sorted(t for t in (a | b | c) if isinstance(t, str) and t)


def _one_ticker_obj(df_t: pd.DataFrame, news_t: pd.DataFrame) -> Dict[str, Any]:
    df_t = df_t.sort_values("date").reset_index(drop=True)

    dates = [_to_datestr(x) for x in df_t["date"].tolist()]
    price = [_safe_num(x) for x in df_t["close"].tolist()]
    s = [_safe_num(x) for x in df_t["S"].tolist()]
    s_ma7 = _roll_ma(s, n=7)

    news_rows: List[Dict[str, Any]] = []
    if news_t is not None and not news_t.empty:
        for _, r in news_t.tail(300).iterrows():
            news_rows.append({
                "ts": _to_iso(r["ts"]),
                "title": str(r["title"]),
                "url": str(r["url"]),
                "text": str(r.get("text", "")),
            })

    return {"date": dates, "close": price, "S": s, "S_MA7": s_ma7, "news": news_rows}


def _write_json(obj: Any, path_out: str) -> None:
    os.makedirs(os.path.dirname(path_out), exist_ok=True)
    with open(path_out, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)


def _portfolio_from_panel(panel: pd.DataFrame, tickers: List[str]) -> Dict[str, Any]:
    if panel.empty or not tickers:
        return {"dates": [], "S": [], "S_MA7": [], "count": []}

    pvt = panel.pivot_table(index="date", columns="ticker", values="S", aggfunc="mean")
    pvt = pvt.reindex(columns=tickers)
    pvt = pvt.fillna(0.0)

    dates = [_to_datestr(d) for d in pvt.index]
    s = _safe_list(pvt.mean(axis=1).tolist())
    count = (pvt.notna()).sum(axis=1).astype(int).tolist()
    s_ma7 = _roll_ma(s, n=7)

    return {"dates": dates, "S": s, "S_MA7": s_ma7, "count": count}


def _write_earnings_per_ticker(earn_df: pd.DataFrame, out_dir: str) -> None:
    if earn_df is None or earn_df.empty:
        return
    for t, g in earn_df.groupby("ticker"):
        rows: List[Dict[str, Any]] = []
        for _, r in g.sort_values("date").iterrows():
            rows.append({
                "date": _to_datestr(r["date"]),
                "eps": _safe_num(r.get("eps")),
                "eps_estimate": _safe_num(r.get("eps_estimate")),
                "revenue": _safe_num(r.get("revenue")),
                "revenue_estimate": _safe_num(r.get("revenue_estimate")),
                "surprise": _safe_num(r.get("surprise")),
            })
        _write_json(rows, os.path.join(out_dir, "earnings", f"{t}.json"))


# ---------------------------
# Public entry point
# ---------------------------

def write_outputs(
    panel: pd.DataFrame,
    news_rows: pd.DataFrame,
    *args,
) -> Tuple[List[str], int]:
    """
    Compatible with both call sites:

        write_outputs(panel, news_rows, out_dir)
        write_outputs(panel, news_rows, earn_rows, out_dir)

    Returns: (tickers_written, n_files)
    """
    if len(args) == 1:
        earn_rows = None
        out_dir = args[0]
    elif len(args) == 2:
        earn_rows = args[0]
        out_dir = args[1]
    else:
        raise TypeError("write_outputs(panel, news_rows, out_dir) or write_outputs(panel, news_rows, earn_rows, out_dir)")

    out_dir = os.fspath(out_dir)
    os.makedirs(os.path.join(out_dir, "ticker"), exist_ok=True)
    os.makedirs(os.path.join(out_dir, "earnings"), exist_ok=True)

    panel = _coerce_panel(panel)
    news_rows = _coerce_news(news_rows)
    earn_df = _coerce_earnings(earn_rows if isinstance(earn_rows, pd.DataFrame) else None)

    tickers = _union_tickers(panel, news_rows, earn_df)

    n_files = 0
    for t in tickers:
        df_t = panel.loc[panel["ticker"] == t].copy()
        nt = news_rows.loc[news_rows["ticker"] == t].copy()

        if df_t.empty and nt.empty:
            continue

        if df_t.empty and not nt.empty:
            # minimal object when only news exists
            obj = {
                "date": sorted({_to_datestr(x) for x in nt["ts"] if pd.notna(x)}),
                "close": [],
                "S": [],
                "S_MA7": [],
                "news": [
                    {"ts": _to_iso(r["ts"]), "title": str(r["title"]), "url": str(r["url"]), "text": str(r.get("text", ""))}
                    for _, r in nt.tail(300).iterrows()
                ],
            }
        else:
            obj = _one_ticker_obj(df_t, nt)

        _write_json(obj, os.path.join(out_dir, "ticker", f"{t}.json"))
        n_files += 1

    _write_json(tickers, os.path.join(out_dir, "_tickers.json"))

    portfolio = _portfolio_from_panel(panel, tickers)
    _write_json(portfolio, os.path.join(out_dir, "portfolio.json"))

    # Earnings (optional)
    _write_earnings_per_ticker(earn_df, out_dir)

    return tickers, n_files
