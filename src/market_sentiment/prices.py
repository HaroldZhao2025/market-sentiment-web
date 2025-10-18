# src/market_sentiment/prices.py
from __future__ import annotations
from typing import List
import io
import pandas as pd
import requests

_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/118.0 Safari/537.36"
)

def _normalize_price_frame(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["date","ticker","open","close"])

    # Flatten columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join([str(x) for x in t if str(x) != ""]) for t in df.columns]

    cols = {str(c).lower(): c for c in df.columns}
    open_col  = cols.get("open") or cols.get("open_0") or cols.get("o") or cols.get("1. open")
    close_col = cols.get("close") or cols.get("adj close") or cols.get("adj_close") or cols.get("c") or cols.get("4. close")

    # build date
    if "date" in df.columns:
        d = pd.to_datetime(df["date"], utc=True, errors="coerce").dt.tz_localize(None).dt.normalize()
    else:
        idx = df.index
        if getattr(idx, "tz", None) is not None:
            d = pd.to_datetime(idx, utc=True, errors="coerce").tz_convert(None).normalize()
        else:
            d = pd.to_datetime(idx, errors="coerce").tz_localize(None).normalize()

    out = pd.DataFrame({"date": d})
    out["ticker"] = ticker
    out["open"]  = pd.to_numeric(df[open_col], errors="coerce")  if open_col  in df.columns else pd.NA
    out["close"] = pd.to_numeric(df[close_col], errors="coerce") if close_col in df.columns else pd.NA

    out = out.dropna(subset=["close"]).drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
    return out[["date","ticker","open","close"]]

def _stooq_http_csv(ticker: str) -> pd.DataFrame:
    # robust CSV fallback: https://stooq.com/q/d/l/?s=aapl&i=d
    variants = [
        ticker,
        ticker.lower(),
        f"{ticker}.US",
        f"{ticker.lower()}.us",
    ]
    for sym in variants:
        url = f"https://stooq.com/q/d/l/?s={sym}&i=d"
        try:
            r = requests.get(url, headers={"User-Agent": _UA}, timeout=(8, 20))
            if r.status_code == 200 and "Date,Open,High,Low,Close,Volume" in r.text:
                df = pd.read_csv(io.StringIO(r.text))
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
                df = df.rename(columns={"Date":"date","Open":"open","Close":"close"})
                df = df.dropna(subset=["date","close"]).sort_values("date").reset_index(drop=True)
                df["ticker"] = ticker
                return df[["date","ticker","open","close"]]
        except Exception:
            continue
    return pd.DataFrame()

def _yf_download(ticker: str, start: str, end: str) -> pd.DataFrame:
    import yfinance as yf
    return yf.download(
        ticker, start=start, end=end,
        interval="1d", auto_adjust=False, actions=False,
        progress=False, threads=False
    )

def _yf_history(ticker: str, start: str, end: str) -> pd.DataFrame:
    import yfinance as yf
    return yf.Ticker(ticker).history(start=start, end=end, interval="1d", auto_adjust=False)

def fetch_prices_yf(ticker: str, start: str, end: str) -> pd.DataFrame:
    # try stooq first (fast, robust in CI)
    try:
        stq = _stooq_http_csv(ticker)
        if not stq.empty:
            # constrain to window
            m = (stq["date"] >= pd.to_datetime(start)) & (stq["date"] <= pd.to_datetime(end))
            stq = stq.loc[m].reset_index(drop=True)
            if not stq.empty:
                return stq
    except Exception:
        pass

    # then yfinance download
    try:
        raw = _yf_download(ticker, start, end)
        norm = _normalize_price_frame(raw, ticker)
        if not norm.empty:
            return norm
    except Exception:
        pass

    # then yfinance history
    try:
        raw = _yf_history(ticker, start, end)
        norm = _normalize_price_frame(raw, ticker)
        if not norm.empty:
            return norm
    except Exception:
        pass

    return pd.DataFrame(columns=["date","ticker","open","close"])
