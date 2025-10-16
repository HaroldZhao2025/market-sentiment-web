from __future__ import annotations

from typing import Optional, Sequence
import pandas as pd
import numpy as np
import yfinance as yf


def _ensure_datetime_utc(idx_or_series) -> pd.DatetimeIndex:
    """
    Take a DatetimeIndex or a datetime-like Series and return a tz-aware UTC index.
    """
    if isinstance(idx_or_series, pd.DatetimeIndex):
        dt = idx_or_series
    else:
        dt = pd.to_datetime(idx_or_series, errors="coerce")
    # localize or convert to UTC safely
    if getattr(dt, "tz", None) is None:
        # handle ambiguous/nonexistent times generously
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
    yfinance may return columns in either order:
      ('AAPL','Open') or ('Open','AAPL').
    """
    t = str(ticker).upper()

    # Try both orientations
    open_col = close_col = None

    if isinstance(df.columns, pd.MultiIndex):
        # Orientation A: (ticker, field)
        try:
            level0 = df.columns.get_level_values(0).astype(str).str.upper()
            level1 = df.columns.get_level_values(1).astype(str)
            # columns where level0==ticker
            mask = level0 == t
            if mask.any():
                sub = df.loc[:, mask]
                sub.columns = level1[mask]  # now single level: Open, Close, ...
                open_col = _pick_col(sub, ["Open", "open"])
                close_col = _pick_col(sub, ["Close", "Adj Close", "close", "adjclose"])
                if open_col or close_col:
                    return pd.DataFrame(
                        {
                            "open": pd.to_numeric(sub[open_col], errors="coerce") if open_col else np.nan,
                            "close": pd.to_numeric(sub[close_col], errors="coerce") if close_col else np.nan,
                        },
                        index=df.index,
                    )
        except Exception:
            pass

        # Orientation B: (field, ticker)
        try:
            level0 = df.columns.get_level_values(0).astype(str)
            level1 = df.columns.get_level_values(1).astype(str).str.upper()
            mask_open = (level0.str.lower().isin(["open"])) & (level1 == t)
            mask_close = (level0.str.lower().isin(["close", "adj close"])) & (level1 == t)
            col_open = df.columns[mask_open]
            col_close = df.columns[mask_close]
            out = {}
            if len(col_open) > 0:
                out["open"] = pd.to_numeric(df[col_open[0]], errors="coerce")
            if len(col_close) > 0:
                out["close"] = pd.to_numeric(df[col_close[0]], errors="coerce")
            if out:
                return pd.DataFrame(out, index=df.index)
        except Exception:
            pass

    # Not a MultiIndex (or failed to parse) — fall back to single-level logic
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
    Handles MultiIndex columns, single-level columns, and different shapes.
    """
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=["date", "ticker", "open", "close"])

    # Some yfinance shapes put the timestamp in the index; sometimes it's a 'Date' column.
    if isinstance(df.index, pd.DatetimeIndex):
        idx = df.index
    elif "Date" in df.columns:
        idx = pd.to_datetime(df["Date"], errors="coerce")
        df = df.drop(columns=["Date"])
    elif "date" in df.columns:
        idx = pd.to_datetime(df["date"], errors="coerce")
        df = df.drop(columns=["date"])
    else:
        # last resort: try to parse the index
        idx = pd.to_datetime(df.index, errors="coerce")

    # Extract open/close robustly
    if isinstance(df.columns, pd.MultiIndex):
        oc = _from_multiindex_columns(df, ticker)
    else:
        oc = _from_singlelevel_columns(df)

    # Build final frame
    out = pd.DataFrame(index=idx)
    out[["open", "close"]] = oc.reindex(out.index)
    out.index = _ensure_datetime_utc(out.index)  # tz-aware UTC
    out = out.reset_index().rename(columns={"index": "date"})
    out["ticker"] = str(ticker).upper()
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
    # Try the most stable API first: single-ticker history()
    try:
        raw = yf.Ticker(t).history(start=start, end=end, auto_adjust=False)
    except Exception:
        raw = None

    # Fallback to download with group_by='column' to avoid MultiIndex if possible
    if raw is None or len(raw) == 0:
        try:
            raw = yf.download(
                t,
                start=start,
                end=end,
                auto_adjust=False,
                progress=False,
                group_by="column",  # keep single-level columns when possible
                threads=False,
            )
        except Exception:
            raw = None

    # If still empty, return empty frame with correct schema
    if raw is None or len(raw) == 0:
        return pd.DataFrame(columns=["date", "ticker", "open", "close"])

    return _normalize_price_frame(raw, t)
