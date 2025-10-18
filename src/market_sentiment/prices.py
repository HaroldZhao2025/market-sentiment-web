# src/market_sentiment/prices.py
from __future__ import annotations
from typing import List
import pandas as pd

def _normalize_price_frame(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["date","ticker","open","close"])

    # Flatten any MultiIndex columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join([str(x) for x in tup if str(x) != ""]) for tup in df.columns]

    cols = {c.lower(): c for c in df.columns}
    open_col  = cols.get("open") or cols.get("open_0")
    close_col = cols.get("close") or cols.get("adj close") or cols.get("adj_close")

    if open_col is None and "Open" in df.columns: open_col = "Open"
    if close_col is None:
        if "Close" in df.columns: close_col = "Close"
        elif "Adj Close" in df.columns: close_col = "Adj Close"

    # Index → date
    idx = df.index
    if "date" in df.columns:
        d = pd.to_datetime(df["date"], utc=True, errors="coerce").dt.tz_localize(None).dt.normalize()
    else:
        d = pd.to_datetime(idx, utc=True, errors="coerce").tz_convert(None).normalize() if getattr(idx, "tz", None) else pd.to_datetime(idx, errors="coerce").tz_localize(None).normalize()

    out = pd.DataFrame({"date": d})
    out["ticker"] = ticker
    if open_col is not None:
        out["open"] = pd.to_numeric(df[open_col], errors="coerce")
    else:
        out["open"] = pd.NA
    if close_col is not None:
        out["close"] = pd.to_numeric(df[close_col], errors="coerce")
    else:
        out["close"] = pd.NA

    out = out.dropna(subset=["close"]).drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
    return out[["date","ticker","open","close"]]

def _yf_download(ticker: str, start: str, end: str) -> pd.DataFrame:
    import yfinance as yf
    return yf.download(
        ticker,
        start=start,
        end=end,
        interval="1d",
        auto_adjust=False,
        actions=False,
        progress=False,
        threads=False,
    )

def _yf_history(ticker: str, start: str, end: str) -> pd.DataFrame:
    import yfinance as yf
    return yf.Ticker(ticker).history(
        start=start, end=end, interval="1d", auto_adjust=False
    )

def _stooq(ticker: str, start: str, end: str) -> pd.DataFrame:
    try:
        from pandas_datareader import data as pdr
    except Exception:
        return pd.DataFrame()
    # Stooq symbols often need ".US"
    symbols = [ticker, f"{ticker}.US"]
    for sym in symbols:
        try:
            df = pdr.DataReader(sym, "stooq")
            if not df.empty:
                # stooq comes desc; flip and window
                df = df.sort_index()
                m = (df.index >= pd.to_datetime(start)) & (df.index <= pd.to_datetime(end))
                return df.loc[m]
        except Exception:
            continue
    return pd.DataFrame()

def fetch_prices_yf(ticker: str, start: str, end: str) -> pd.DataFrame:
    # Try yfinance download
    try:
        raw = _yf_download(ticker, start, end)
        norm = _normalize_price_frame(raw, ticker)
        if not norm.empty:
            return norm
    except Exception:
        pass

    # Try yfinance history
    try:
        raw = _yf_history(ticker, start, end)
        norm = _normalize_price_frame(raw, ticker)
        if not norm.empty:
            return norm
    except Exception:
        pass

    # Try Stooq fallback
    try:
        raw = _stooq(ticker, start, end)
        norm = _normalize_price_frame(raw, ticker)
        if not norm.empty:
            return norm
    except Exception:
        pass

    # Give up silently (caller aggregates/warns)
    return pd.DataFrame(columns=["date","ticker","open","close"])
