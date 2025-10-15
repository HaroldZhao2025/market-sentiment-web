from __future__ import annotations
import warnings
import pandas as pd
import yfinance as yf

def _normalize(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["date","ticker","open","high","low","close","adj_close","volume"])
    if "Date" in df.columns:
        date = pd.to_datetime(df["Date"], errors="coerce", utc=True)
    else:
        date = pd.to_datetime(df.index, errors="coerce", utc=True)

    out = df.copy().rename(columns={
        "Open":"open", "High":"high", "Low":"low",
        "Close":"close", "Adj Close":"adj_close", "Volume":"volume"
    })
    for c in ["open","high","low","close","adj_close","volume"]:
        if c not in out.columns: out[c] = pd.NA
    out = out.assign(date=date.dt.tz_localize(None), ticker=ticker)
    out = out[["date","ticker","open","high","low","close","adj_close","volume"]]
    out = out.dropna(subset=["date"]).drop_duplicates(subset=["date"]).sort_values("date")
    for c in ["open","high","low","close","adj_close"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce").astype("Int64")
    return out

def fetch_prices_yf(ticker: str, start: str, end: str) -> pd.DataFrame:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = yf.download(ticker, start=start, end=end, interval="1d",
                         auto_adjust=False, actions=False, progress=False, threads=False)
    norm = _normalize(df, ticker)
    if not norm.empty: return norm
    try:
        df2 = yf.Ticker(ticker).history(start=start, end=end, interval="1d",
                                        auto_adjust=False, actions=False)
        return _normalize(df2, ticker)
    except Exception:
        return _normalize(pd.DataFrame(), ticker)
