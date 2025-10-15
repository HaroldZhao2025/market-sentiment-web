from __future__ import annotations
import pandas as pd
import yfinance as yf

def fetch_prices_yf(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Returns columns: ['date','ticker','open','close'] (date naive midnight)
    """
    df = yf.download(
        ticker, start=start, end=end, interval="1d",
        auto_adjust=False, progress=False, group_by="column"
    )
    if df is None or df.empty:
        return pd.DataFrame(columns=["date","ticker","open","close"])
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[1] if isinstance(c, tuple) else c for c in df.columns]
    df = df.rename(columns={"Open":"open","Close":"close"})
    df["date"] = pd.to_datetime(df.index).tz_localize(None).normalize()
    df["ticker"] = ticker
    return df[["date","ticker","open","close"]].reset_index(drop=True)
