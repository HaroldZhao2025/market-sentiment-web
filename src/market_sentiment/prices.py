from __future__ import annotations
import pandas as pd
import yfinance as yf


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure single-level, normalized column names (lowercase).
    Handles yfinance's occasional MultiIndex columns even for single tickers.
    """
    if isinstance(df.columns, pd.MultiIndex):
        # Keep only the price field name (Open/High/Low/Close/Adj Close/Volume)
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    return df


def fetch_prices(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Robust single-ticker OHLCV fetcher. Always returns a DataFrame with:
    ['date','open','high','low','close','volume'] and tz-aware 'date'.
    """
    df = yf.download(
        ticker,
        start=start,
        end=end,
        progress=False,
        auto_adjust=False,
        group_by="column",  # avoid wide multi-ticker style
    )

    if df is None or len(df) == 0:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    df = _flatten_columns(df)

    # Reset index to have a 'date' column regardless of the index name
    df = df.reset_index()
    # Normalize the datetime column name to 'date'
    if "date" not in df.columns:
        # yfinance typically uses 'Date' after reset_index()
        for c in df.columns:
            if str(c).lower().startswith("date"):
                df = df.rename(columns={c: "date"})
                break

    # Map possible columns to our canonical names
    # (sometimes only 'adj_close' is present)
    out = pd.DataFrame(index=df.index)
    out["date"] = pd.to_datetime(df["date"], errors="coerce")
    # Localize to America/New_York (treat values as naive local trading dates)
    out["date"] = out["date"].dt.tz_localize("America/New_York")

    def pick(*names: str) -> pd.Series:
        for n in names:
            if n in df.columns:
                return pd.to_numeric(df[n], errors="coerce")
        return pd.Series(index=df.index, dtype="float64")

    out["open"] = pick("open")
    out["high"] = pick("high")
    out["low"] = pick("low")
    out["close"] = pick("close", "adj_close")
    out["volume"] = pick("volume")

    # Keep only the required columns
    out = out[["date", "open", "high", "low", "close", "volume"]]
    return out
