# src/market_sentiment/prices.py
from __future__ import annotations

from typing import Optional, Sequence
import pandas as pd
import numpy as np
import yfinance as yf


def _ensure_datetime_utc(idx_or_series) -> pd.DatetimeIndex:
    """
    Make a tz-aware UTC DatetimeIndex from a DatetimeIndex or datetime-like Series/Index.
    """
    if isinstance(idx_or_series, pd.DatetimeIndex):
        dt = idx_or_series
    else:
        dt = pd.to_datetime(idx_or_series, errors="coerce")
    if getattr(dt, "tz", None) is None:
        try:
            dt = dt.tz_localize("UTC", ambiguous="NaT", nonexistent="shift_forward")
        except Exception:
            dt = dt.tz_localize("UTC")
    else:
        dt = dt.tz_convert("UTC")
    return dt


def _pick_col(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    """
    Return the first matching column name from `candidates` ignoring case/spaces/underscores.
    """
    norm = {str(c).strip().lower().replace(" ", "").replace("_", ""): c for c in df.columns}
    for want in candidates:
        key = want.strip().lower().replace(" ", "").replace("_", "")
        if key in norm:
            return norm[key]
    return None


def _from_multiindex_columns(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Flatten yfinance MultiIndex columns and extract open/close for a single ticker.
    Handles both (ticker, field) and (field, ticker) orders.
    """
    t = str(ticker).upper()

    if not isinstance(df.columns, pd.MultiIndex):
        return _from_singlelevel_columns(df)

    # Orientation A: (ticker, field)
    try:
        level0 = df.columns.get_level_values(0).astype(str).str.upper()
        level1 = df.columns.get_level_values(1).astype(str)
        mask = level0 == t
        if mask.any():
            sub = df.loc[:, mask]
            sub.columns = level1[mask]  # single level: Open, Close, Adj Close, ...
            open_col = _pick_col(sub, ["Open", "open"])
            close_col = _pick_col(sub, ["Close", "close", "Adj Close", "adjclose"])
            if open_col or close_col:
                out = {}
                if open_col:
                    out["open"] = pd.to_numeric(sub[open_col], errors="coerce")
                if close_col:
                    out["close"] = pd.to_numeric(sub[close_col], errors="coerce")
                return pd.DataFrame(out, index=df.index)
    except Exception:
        pass

    # Orientation B: (field, ticker)
    try:
        level0 = df.columns.get_level_values(0).astype(str)        # fields
        level1 = df.columns.get_level_values(1).astype(str).str.upper()  # tickers
        mask_open = (level0.str.lower().isin(["open"])) & (level1 == t)
        mask_close = (level0.str.lower().isin(["close", "adj close"])) & (level1 == t)
        out = {}
        if mask_open.any():
            out["open"] = pd.to_numeric(df.loc[:, df.columns[mask_open][0]], errors="coerce")
        if mask_close.any():
            out["close"] = pd.to_numeric(df.loc[:, df.columns[mask_close][0]], errors="coerce")
        if out:
            return pd.DataFrame(out, index=df.index)
    except Exception:
        pass

    # Fallback
    return _from_singlelevel_columns(df)


def _from_singlelevel_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract open/close from a single-level columns DataFrame.
    Prefers Close over Adj Close for consistency; uses Adj Close if Close missing.
    """
    open_col = _pick_col(df, ["Open", "open"])
    close_col = _pick_col(df, ["Close", "close", "Adj Close", "adjclose"])

    out = pd.DataFrame(index=df.index)
    out["open"] = pd.to_numeric(df[open_col], errors="coerce") if open_col else np.nan
    out["close"] = pd.to_numeric(df[close_col], errors="coerce") if close_col else np.nan
    return out


def _normalize_price_frame(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Normalize a raw yfinance frame into: ['date','ticker','open','close'].
    Handles MultiIndex/single-level columns and varying index names.
    """
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=["date", "ticker", "open", "close"])

    # Build a datetime index (from index or 'Date'/'date' columns)
    if isinstance(df.index, pd.DatetimeIndex):
        idx = df.index
    elif "Date" in df.columns:
        idx = pd.to_datetime(df["Date"], errors="coerce")
        df = df.drop(columns=["Date"])
    elif "date" in df.columns:
        idx = pd.to_datetime(df["date"], errors="coerce")
        df = df.drop(columns=["date"])
    else:
        idx = pd.to_datetime(df.index, errors="coerce")

    # Extract open/close robustly
    oc = _from_multiindex_columns(df, ticker) if isinstance(df.columns, pd.MultiIndex) else _from_singlelevel_columns(df)

    # Assemble output and ensure UTC datetimes
    out = pd.DataFrame(index=idx)
    out[["open", "close"]] = oc.reindex(out.index)
    out.index = _ensure_datetime_utc(out.index)

    # Reset index and robustly rename the index column to 'date'
    out = out.reset_index()
    # Whatever the first column is called (e.g., 'index', 'Date', a named index, etc.), rename it to 'date'
    first_col = out.columns[0]
    if first_col != "date":
        out = out.rename(columns={first_col: "date"})

    # Finalize schema
    out["ticker"] = str(ticker).upper()
    out["date"] = pd.to_datetime(out["date"], utc=True, errors="coerce")
    out = out[["date", "ticker", "open", "close"]].sort_values("date").reset_index(drop=True)
    return out


def fetch_prices_yf(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Fetch daily OHLC for a single ticker from Yahoo Finance (free) and normalize.

    Returns:
      DataFrame with ['date','ticker','open','close'] (UTC date, sorted).
    """
    t = str(ticker).upper()

    raw = None
    # Preferred: Ticker().history()
    try:
        raw = yf.Ticker(t).history(start=start, end=end, auto_adjust=False)
    except Exception:
        raw = None

    # Fallback: download()
    if raw is None or len(raw) == 0:
        try:
            raw = yf.download(
                t,
                start=start,
                end=end,
                auto_adjust=False,
                progress=False,
                group_by="column",  # avoid MultiIndex when possible
                threads=False,
            )
        except Exception:
            raw = None

    if raw is None or len(raw) == 0:
        return pd.DataFrame(columns=["date", "ticker", "open", "close"])

    return _normalize_price_frame(raw, t)
