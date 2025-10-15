# src/market_sentiment/prices.py
from __future__ import annotations

from typing import Optional
import pandas as pd
import yfinance as yf


def _select_open_close_columns(df: pd.DataFrame) -> tuple[Optional[str], Optional[str]]:
    """
    Return column names for open and close in df, preferring:
      - 'Open' for open
      - 'Close' for close; fallback to 'Adj Close' if missing
    If none found, try fuzzy contains for 'open'/'close'.
    """
    cols = list(df.columns)
    # Prefer exact
    open_col = "Open" if "Open" in cols else None
    close_col = "Close" if "Close" in cols else None

    # Fallback for Close: use 'Adj Close' if present
    if close_col is None and "Adj Close" in cols:
        close_col = "Adj Close"

    # Fuzzy fallback
    if open_col is None:
        cands = [c for c in cols if "open" in c.lower()]
        open_col = cands[0] if cands else None
    if close_col is None:
        cands = [c for c in cols if "close" in c.lower()]
        # avoid picking 'adj close' if a generic 'close' exists
        cands_sorted = sorted(cands, key=lambda c: (c.lower() != "close", len(c)))
        close_col = cands_sorted[0] if cands_sorted else None

    return open_col, close_col


def _extract_date_column(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """
    Ensure there is a concrete date column in the frame.
    Strategy:
      1) reset_index() to bring index out
      2) prefer first datetime-like column; else look for common names
      3) if still not found, attempt to parse the 'index' column as datetime
    Returns (df_with_date_col, date_col_name)
    """
    # Bring index out as a column (named by index name or 'index')
    df = df.reset_index(drop=False)

    # Pick datetime-like column if present
    datetime_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
    if datetime_cols:
        return df, datetime_cols[0]

    # Common names that may hold date strings
    common_names = ["Date", "Datetime", "date", "INDEX", "index"]
    for name in common_names:
        if name in df.columns:
            # Try to coerce to datetime
            try:
                as_dt = pd.to_datetime(df[name], errors="coerce", utc=True)
                if as_dt.notna().any():
                    df[name] = as_dt
                    return df, name
            except Exception:
                pass

    # Last resort: try to datetime-convert any column with 'date' in the name
    for c in df.columns:
        if "date" in c.lower():
            try:
                as_dt = pd.to_datetime(df[c], errors="coerce", utc=True)
                if as_dt.notna().any():
                    df[c] = as_dt
                    return df, c
            except Exception:
                pass

    # Could not infer
    # Make a synthetic empty result; caller will handle empties.
    return df, ""


def _normalize_price_frame(raw: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Normalize any yfinance output into:
      columns: ["date", "ticker", "open", "close"]
      date: tz-naive, America/New_York midnight
    """
    cols = ["date", "ticker", "open", "close"]
    if raw is None or raw.empty:
        return pd.DataFrame(columns=cols)

    df = raw.copy()

    # If MultiIndex columns, collapse to a 2-col frame with Open/Close
    if isinstance(df.columns, pd.MultiIndex):
        # Two main common layouts:
        #   (Field, Ticker)  OR  (Ticker, Field)
        lv0, lv1 = df.columns.get_level_values(0), df.columns.get_level_values(1)
        if "Open" in lv0 and "Close" in lv0:
            # (Field, Ticker)
            try:
                open_series = df[("Open", ticker)]
                close_series = df[("Close", ticker)]
            except Exception:
                # fallback: first ticker column for each field
                open_series = df["Open"].iloc[:, 0]
                close_series = df["Close"].iloc[:, 0]
            df = pd.DataFrame({"Open": open_series, "Close": close_series})
        elif "Open" in lv1 and "Close" in lv1:
            # (Ticker, Field)
            try:
                open_series = df[(ticker, "Open")]
                close_series = df[(ticker, "Close")]
            except Exception:
                first_ticker = df.columns.get_level_values(0)[0]
                open_series = df[(first_ticker, "Open")]
                close_series = df[(first_ticker, "Close")]
            df = pd.DataFrame({"Open": open_series, "Close": close_series})
        else:
            # Generic flatten
            df.columns = ["_".join([str(x) for x in tup if str(x) != ""]).strip("_") for tup in df.columns]

    # Ensure we have a concrete date column name
    df, date_col = _extract_date_column(df)
    if not date_col:
        return pd.DataFrame(columns=cols)

    # Identify open/close columns
    open_col, close_col = _select_open_close_columns(df)
    if open_col is None or close_col is None:
        return pd.DataFrame(columns=cols)

    # Coerce date to NY tz-naive midnight
    d = pd.to_datetime(df[date_col], errors="coerce", utc=True)
    # If all NaT, bail out
    if d.isna().all():
        return pd.DataFrame(columns=cols)

    d = d.dt.tz_convert("America/New_York").dt.normalize().dt.tz_localize(None)

    out = pd.DataFrame(
        {
            "date": d,
            "ticker": ticker,
            "open": pd.to_numeric(df[open_col], errors="coerce"),
            "close": pd.to_numeric(df[close_col], errors="coerce"),
        }
    )
    out = out.dropna(subset=["date", "open", "close"])
    if out.empty:
        return pd.DataFrame(columns=cols)

    # Deduplicate on (ticker, date)
    out = out.sort_values("date").drop_duplicates(["ticker", "date"], keep="last").reset_index(drop=True)
    return out


def _try_download(ticker: str, start: str, end: str, *, use_download: bool = True) -> pd.DataFrame:
    """
    Try yf.download first (single ticker, single-level columns), then fallback to Ticker.history.
    """
    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end) + pd.Timedelta(days=1)  # yfinance end is exclusive

    if use_download:
        try:
            df = yf.download(
                ticker,
                start=start_dt,
                end=end_dt,
                interval="1d",
                progress=False,
                auto_adjust=False,
                actions=False,
                group_by="column",  # single-level columns for single ticker
                threads=False,
            )
            if df is not None and not df.empty:
                return df
        except Exception:
            pass

    try:
        t = yf.Ticker(ticker)
        df = t.history(start=start_dt, end=end_dt, interval="1d", actions=False, auto_adjust=False)
        return df
    except Exception:
        return pd.DataFrame()


def fetch_prices_yf(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Public entry used by the CLI. Always returns columns: ["date","ticker","open","close"].
    """
    raw = _try_download(ticker, start, end, use_download=True)
    norm = _normalize_price_frame(raw, ticker)
    return norm
