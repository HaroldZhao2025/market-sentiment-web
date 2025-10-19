# src/market_sentiment/prices.py
from __future__ import annotations
from typing import List
import io
import os
import time
import random
import pandas as pd
import requests

_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/118.0 Safari/537.36"
)

def _get_env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default

def _get_env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

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

    out = (
        out.dropna(subset=["close"])
           .drop_duplicates(subset=["date"])
           .sort_values("date")
           .reset_index(drop=True)
    )
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

def _throttle_sleep(base: float, attempt: int, backoff: float) -> None:
    """
    Sleep with exponential backoff + small jitter.
    Controlled by env:
      YF_THROTTLE_S   (base, default 0.8)
      YF_BACKOFF_BASE (multiplier, default 1.8)
    """
    base = max(base, 0.0)
    backoff = max(backoff, 1.0)
    delay = base * (backoff ** attempt) + random.uniform(0, 0.35)
    time.sleep(delay)

def fetch_prices_yf(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Keep the original behavior and schema, but add throttling + retries
    around the yfinance calls. Environment knobs (optional):

      YF_THROTTLE_S   default 0.8   (seconds)
      YF_MAX_RETRIES  default 6     (integer)
      YF_BACKOFF_BASE default 1.8   (multiplier)

    Returns DataFrame with columns: date, ticker, open, close
    """
    throttle_s   = _get_env_float("YF_THROTTLE_S", 0.8)
    max_retries  = _get_env_int("YF_MAX_RETRIES", 6)
    backoff_base = _get_env_float("YF_BACKOFF_BASE", 1.8)

    # 1) Try stooq once (fast & CI-friendly)
    try:
        stq = _stooq_http_csv(ticker)
        if not stq.empty:
            m = (stq["date"] >= pd.to_datetime(start)) & (stq["date"] <= pd.to_datetime(end))
            stq = stq.loc[m].reset_index(drop=True)
            if not stq.empty:
                # tiny sleep to be nice even when stooq hits
                time.sleep(min(throttle_s, 0.2))
                return stq
    except Exception:
        pass

    # 2) yfinance.download with retries + throttle
    for attempt in range(max_retries):
        try:
            raw = _yf_download(ticker, start, end)
            norm = _normalize_price_frame(raw, ticker)
            if not norm.empty:
                _throttle_sleep(throttle_s, attempt=0, backoff=backoff_base)  # post-success small sleep
                return norm
            # empty is treated as transient
            raise RuntimeError("yfinance.download returned empty frame")
        except Exception:
            _throttle_sleep(throttle_s, attempt=attempt, backoff=backoff_base)

    # 3) yfinance.Ticker(...).history with retries + throttle
    for attempt in range(max_retries):
        try:
            raw = _yf_history(ticker, start, end)
            norm = _normalize_price_frame(raw, ticker)
            if not norm.empty:
                _throttle_sleep(throttle_s, attempt=0, backoff=backoff_base)
                return norm
            raise RuntimeError("yfinance.history returned empty frame")
        except Exception:
            _throttle_sleep(throttle_s, attempt=attempt, backoff=backoff_base)

    # 4) Give up, but preserve schema (prevents KeyError downstream)
    return pd.DataFrame(columns=["date","ticker","open","close"])
