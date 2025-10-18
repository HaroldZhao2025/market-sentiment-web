# src/market_sentiment/aggregate.py
from __future__ import annotations

import pandas as pd
import numpy as np

# ---------- date helpers ----------

def _ensure_date_dtype(df: pd.DataFrame, col: str = "date") -> pd.DataFrame:
    """Ensure a tz-naive daily datetime64[ns] column."""
    out = df.copy()
    if col not in out.columns:
        raise KeyError(f"Missing '{col}' in DataFrame.")

    d = pd.to_datetime(out[col], errors="coerce", utc=True)
    # normalize to midnight UTC, then drop tz (daily index)
    d = d.dt.normalize().dt.tz_localize(None)
    out[col] = d
    return out


def _effective_date(ts: pd.Series | list | pd.DatetimeIndex, cutoff_minutes: int = 5) -> pd.Series:
    """Shift timestamps by cutoff (NY time) then take the trading day (NY)."""
    s = pd.to_datetime(ts, errors="coerce", utc=True)
    if not isinstance(s, pd.Series):
        s = pd.Series(s, copy=False)
    et = s.dt.tz_convert("America/New_York")
    eff = (et - pd.Timedelta(minutes=cutoff_minutes)).dt.normalize().dt.tz_localize(None)
    return eff

# ---------- sentiment aggregation ----------

def daily_sentiment_from_rows(rows: pd.DataFrame, kind: str, cutoff_minutes: int = 5) -> pd.DataFrame:
    """
    Aggregate raw rows (ticker, ts, S) into daily NY dates.
    kind in {'news','earn'} -> output col 'S_NEWS' or 'S_EARN'
    """
    if rows is None or rows.empty:
        return pd.DataFrame(columns=["date", "ticker", "S_NEWS" if kind == "news" else "S_EARN"])

    req = {"ticker", "ts", "S"}
    if not req.issubset(rows.columns):
        raise KeyError(f"{kind} rows must have columns: ticker, ts, S.")

    df = rows.copy()
    df["date"] = _effective_date(df["ts"], cutoff_minutes=cutoff_minutes)
    df = df.dropna(subset=["date", "ticker"])

    out_col = "S_NEWS" if kind == "news" else "S_EARN"
    daily = (
        df.groupby(["date", "ticker"], as_index=False)["S"]
        .mean(numeric_only=True)
        .rename(columns={"S": out_col})
    )
    return _ensure_date_dtype(daily, "date")


def join_and_fill_daily(d_news: pd.DataFrame | None, d_earn: pd.DataFrame | None) -> pd.DataFrame:
    """Outer-join daily sources; fill NaNs; compute total S."""
    cols = ["date", "ticker"]
    dn = d_news.copy() if d_news is not None and not d_news.empty else pd.DataFrame(columns=cols + ["S_NEWS"])
    de = d_earn.copy() if d_earn is not None and not d_earn.empty else pd.DataFrame(columns=cols + ["S_EARN"])

    if "date" in dn.columns:
        dn = _ensure_date_dtype(dn, "date")
    if "date" in de.columns:
        de = _ensure_date_dtype(de, "date")

    df = pd.merge(dn, de, on=cols, how="outer")
    df["S_NEWS"] = pd.to_numeric(df.get("S_NEWS"), errors="coerce").fillna(0.0)
    df["S_EARN"] = pd.to_numeric(df.get("S_EARN"), errors="coerce").fillna(0.0)
    df["S"] = df["S_NEWS"] + df["S_EARN"]
    return df.sort_values(cols).reset_index(drop=True)

# ---------- prices / returns ----------

def add_forward_returns(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Require columns: date, ticker, open, close
    Adds: ret_oc_1d (open->close same day), ret_cc_1d (close->next close)
    """
    req = {"date", "ticker", "open", "close"}
    if not req.issubset(prices.columns):
        raise KeyError("prices must have columns: date, ticker, open, close")

    df = _ensure_date_dtype(prices, "date").sort_values(["ticker", "date"]).reset_index(drop=True)

    def _one(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("date").copy()
        g["ret_oc_1d"] = (g["close"] - g["open"]) / g["open"]
        g["ret_cc_1d"] = g["close"].shift(-1) / g["close"] - 1.0
        return g

    out = df.groupby("ticker", group_keys=False).apply(_one).reset_index(drop=True)
    return out
