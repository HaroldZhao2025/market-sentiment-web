from __future__ import annotations
import warnings
from typing import List
import pandas as pd
import yfinance as yf

REQUIRED_COLS: List[str] = ["date", "ticker", "open", "close"]

def _normalize_price_df(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Normalize a yfinance OHLCV frame to:
      ['date','ticker','open','high','low','close','adj_close','volume']
    with 'date' as pandas datetime (naive), sorted ascending, duplicates dropped.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=[
            "date","ticker","open","high","low","close","adj_close","volume"
        ])

    # yfinance.download returns Date index; history() returns Date index too.
    if "Date" in df.columns:
        # sometimes appears as a column after reset_index; we’ll use it
        date = pd.to_datetime(df["Date"], errors="coerce", utc=True)
    else:
        # most common: index is DatetimeIndex
        date = pd.to_datetime(df.index, errors="coerce", utc=True)

    out = df.copy()
    out = out.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    )

    # Ensure all expected columns exist even if yfinance omitted some
    for col in ["open", "high", "low", "close", "adj_close", "volume"]:
        if col not in out.columns:
            out[col] = pd.NA

    out = out.assign(
        date=date.dt.tz_localize(None),  # naive timestamp; we convert to NY trading date later
        ticker=ticker,
    )

    # Clean & order
    out = out[["date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]]
    out = out.dropna(subset=["date"]).drop_duplicates(subset=["date"]).sort_values("date")

    # Cast numerics where possible
    for c in ["open", "high", "low", "close", "adj_close"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce").astype("Int64")

    return out


def fetch_prices_yf(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Fetch daily OHLCV from yfinance for [start, end).
    Returns a DataFrame with columns:
      ['date','ticker','open','high','low','close','adj_close','volume']
    If no data is returned, an empty frame with those columns is returned.
    """
    # 1) Try the vectorized downloader
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = yf.download(
            tickers=ticker,
            start=start,
            end=end,
            interval="1d",
            auto_adjust=False,
            actions=False,
            progress=False,
            threads=False,
        )

    norm = _normalize_price_df(df, ticker)
    if not norm.empty:
        return norm

    # 2) Fallback: Ticker().history() (some tickers work better here)
    try:
        t = yf.Ticker(ticker)
        df2 = t.history(start=start, end=end, interval="1d", auto_adjust=False, actions=False)
        norm2 = _normalize_price_df(df2, ticker)
        if not norm2.empty:
            return norm2
    except Exception:
        pass

    # 3) Still empty -> return empty schema
    return _normalize_price_df(pd.DataFrame(), ticker)
