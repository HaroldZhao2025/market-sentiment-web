# src/market_sentiment/prices.py
from __future__ import annotations

import os
import time
import random
from typing import Iterable, List, Optional

import pandas as pd
import yfinance as yf


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _normalize_df(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Normalize yfinance output to:
      columns: date, ticker, open, close, high, low, volume
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "ticker", "open", "close", "high", "low", "volume"])

    # yfinance returns Date index; ensure naive datetime
    df = df.copy()
    if isinstance(df.index, pd.DatetimeIndex):
        # Make timezone-naive for stable JSON
        try:
            # if already tz-aware, remove tz
            if df.index.tz is not None:
                df.index = df.index.tz_convert(None)
        except Exception:
            # if tz_convert fails due to naive, ensure to_datetime
            df.index = pd.to_datetime(df.index)

    df = df.rename(
        columns={
            "Open": "open",
            "Close": "close",
            "High": "high",
            "Low": "low",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    )

    # Some providers may return lowercase already; ensure keys exist
    for col in ["open", "close", "high", "low", "volume"]:
        if col not in df.columns:
            df[col] = pd.NA

    out = (
        df.reset_index()
        .rename(columns={"Date": "date"})
        .loc[:, ["date", "open", "close", "high", "low", "volume"]]
    )
    out["ticker"] = ticker
    # Ensure column order
    out = out[["date", "ticker", "open", "close", "high", "low", "volume"]]
    return out


def fetch_prices_yf(
    ticker: str,
    start: str,
    end: str,
    *,
    throttle_s: Optional[float] = None,
    max_retries: Optional[int] = None,
    backoff_base: Optional[float] = None,
) -> pd.DataFrame:
    """
    Download daily prices for a single ticker with built-in throttling and retries.

    - Respects env vars:
        YF_THROTTLE_S   (default 0.8)   : base delay after *each* call
        YF_MAX_RETRIES  (default 6)     : max attempts when rate-limited/empty
        YF_BACKOFF_BASE (default 1.75)  : exponential backoff multiplier
    - Never raises on rate-limit; returns empty DataFrame on final failure.
    - Keeps the original function name and signature used across the project.
    """
    throttle = _env_float("YF_THROTTLE_S", 0.8) if throttle_s is None else float(throttle_s)
    retries = _env_int("YF_MAX_RETRIES", 6) if max_retries is None else int(max_retries)
    backoff = _env_float("YF_BACKOFF_BASE", 1.75) if backoff_base is None else float(backoff_base)

    last_err: Optional[Exception] = None

    for attempt in range(retries):
        try:
            # progress=False avoids flooding logs; threads=False keeps calls serialized
            df = yf.download(
                tickers=ticker,
                start=start,
                end=end,
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
            )
            # yfinance sometimes returns empty on transient rate-limit; treat as retryable
            if df is None or df.empty:
                raise RuntimeError("Empty prices frame (transient).")

            out = _normalize_df(df, ticker)
            # Base throttle + small jitter to desynchronize
            time.sleep(throttle + random.uniform(0, max(throttle * 0.5, 0.05)))
            return out
        except Exception as e:
            last_err = e
            # Exponential backoff with jitter
            sleep_s = (throttle if throttle > 0 else 0.5) * (backoff ** attempt) + random.uniform(0, 0.35)
            time.sleep(sleep_s)

    # Final failure: return empty (do not crash entire pipeline)
    # Caller can count successes and continue.
    return pd.DataFrame(columns=["date", "ticker", "open", "close", "high", "low", "volume"])


# Optional: batch helper if you later decide to switch build_json to group downloads
def fetch_prices_many_yf(
    tickers: Iterable[str],
    start: str,
    end: str,
    *,
    batch_size: int = 20,
    throttle_s: float = None,
    max_retries: int = None,
    backoff_base: float = None,
) -> pd.DataFrame:
    """
    Convenience wrapper that fetches multiple tickers sequentially (throttled).
    Uses fetch_prices_yf internally so it inherits the same retry/backoff logic.
    """
    throttle = _env_float("YF_THROTTLE_S", 0.8) if throttle_s is None else float(throttle_s)
    retries = _env_int("YF_MAX_RETRIES", 6) if max_retries is None else int(max_retries)
    backoff = _env_float("YF_BACKOFF_BASE", 1.75) if backoff_base is None else float(backoff_base)

    frames: List[pd.DataFrame] = []
    tickers = [str(t).strip().upper() for t in tickers if str(t).strip()]

    # Sequential by design to be friendlier to rate limits
    for t in tickers:
        df = fetch_prices_yf(t, start, end, throttle_s=throttle, max_retries=retries, backoff_base=backoff)
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["date", "ticker", "open", "close", "high", "low", "volume"])
    return pd.concat(frames, ignore_index=True)
