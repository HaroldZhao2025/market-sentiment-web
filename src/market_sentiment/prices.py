# src/market_sentiment/prices.py
from __future__ import annotations

from typing import Optional
import pandas as pd
import yfinance as yf


def _normalize_price_frame(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Take any yfinance price frame and return a standardized DataFrame with:
        columns: ["date", "ticker", "open", "close"]
        date: tz-naive, normalized to midnight (YYYY-MM-DD 00:00:00)
    If df is empty, returns an empty DataFrame with the same columns.
    """
    cols = ["date", "ticker", "open", "close"]
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)

    # If we got multi-index columns (common when group_by is not 'column'),
    # collapse them. We only need the Open/Close for the (single) ticker.
    if isinstance(df.columns, pd.MultiIndex):
        # Try to pick the first level as field name if df has a single ticker level
        # Typical shapes: columns like ('Open', 'AAPL'), ('Close','AAPL') or ('AAPL','Open')
        # We'll try both possible layouts:
        if "Open" in df.columns.get_level_values(0) and "Close" in df.columns.get_level_values(0):
            # Layout: level0 = field, level1 = ticker
            # Select the column for this ticker if present; else take the first column across tickers
            try:
                open_series = df[("Open", ticker)]
                close_series = df[("Close", ticker)]
            except Exception:
                # fallback: take the first ticker column if the specific one is missing
                open_series = df["Open"].iloc[:, 0]
                close_series = df["Close"].iloc[:, 0]
            df = pd.DataFrame({"Open": open_series, "Close": close_series})
        elif "Open" in df.columns.get_level_values(1) and "Close" in df.columns.get_level_values(1):
            # Layout: level0 = ticker, level1 = field
            try:
                open_series = df[(ticker, "Open")]
                close_series = df[(ticker, "Close")]
            except Exception:
                # fallback: take the first ticker level available
                first_ticker = df.columns.get_level_values(0)[0]
                open_series = df[(first_ticker, "Open")]
                close_series = df[(first_ticker, "Close")]
            df = pd.DataFrame({"Open": open_series, "Close": close_series})
        else:
            # Unknown multiindex layout; best effort attempt to flatten
            df.columns = ["_".join([str(x) for x in c if str(x) != ""]) for c in df.columns]
            # look for any variant of Open/Close
            cand_open = [c for c in df.columns if c.lower().endswith("open")]
            cand_close = [c for c in df.columns if c.lower().endswith("close")]
            if not cand_open or not cand_close:
                return pd.DataFrame(columns=cols)
            df = pd.DataFrame({"Open": df[cand_open[0]], "Close": df[cand_close[0]]})

    # Ensure index -> date column
    if "Date" in df.columns:
        df = df.rename(columns={"Date": "date"})
    else:
        df = df.reset_index().rename(columns={"index": "date"})

    # Normalize column names to lower-case
    df = df.rename(columns={"Open": "open", "Close": "close"})

    # If open/close are missing, attempt common variants or bail to empty
    if "open" not in df.columns or "close" not in df.columns:
        lc = {c.lower(): c for c in df.columns}
        open_col = lc.get("open")
        close_col = lc.get("close")
        if open_col and close_col:
            df = df.rename(columns={open_col: "open", close_col: "close"})
        else:
            return pd.DataFrame(columns=cols)

    # Clean datetimes: make tz-naive midnight (normalize)
    d = pd.to_datetime(df["date"], errors="coerce", utc=True)
    # Convert to NY then normalize, then drop tz
    d = d.tz_convert("America/New_York").normalize().tz_localize(None)
    df["date"] = d

    # Add ticker and keep only required columns
    df["ticker"] = ticker
    out = df[["date", "ticker", "open", "close"]].dropna().reset_index(drop=True)

    # Drop duplicated dates if any (keep last)
    if not out.empty:
        out = out.sort_values("date").drop_duplicates(["ticker", "date"], keep="last")
    return out


def _try_download(
    ticker: str,
    start: str,
    end: str,
    *,
    use_download: bool = True
) -> pd.DataFrame:
    """
    Try yf.download first (single ticker to avoid MultiIndex), then fallback
    to Ticker.history if needed.
    """
    start_dt = pd.to_datetime(start)
    # yfinance end is exclusive for download; add a day to include 'end'
    end_dt = pd.to_datetime(end) + pd.Timedelta(days=1)

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

    # fallback to Ticker.history
    try:
        t = yf.Ticker(ticker)
        df = t.history(
            start=start_dt,
            end=end_dt,
            interval="1d",
            actions=False,
            auto_adjust=False,
        )
        return df
    except Exception:
        return pd.DataFrame()


def fetch_prices_yf(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Public function used by the CLI.
    Always returns a DataFrame with columns: ["date","ticker","open","close"].
    """
    raw = _try_download(ticker, start, end, use_download=True)
    norm = _normalize_price_frame(raw, ticker)
    return norm
