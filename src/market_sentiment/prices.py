# src/market_sentiment/prices.py
from __future__ import annotations

import time
from typing import List, Iterable
import pandas as pd
import yfinance as yf


def _normalize_single(raw: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Normalize to columns: date (tz-naive YYYY-MM-DD), ticker, open, close
    Accepts either a single-ticker DataFrame (standard yfinance) or a sub-DataFrame.
    """
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["date", "ticker", "open", "close"])

    df = raw.copy()

    # Flatten any MultiIndex columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["-".join([str(c) for c in cols if c not in (None, "")]) for cols in df.columns.to_list()]

    # Try to find open/close columns (case-insensitive)
    colmap = {c.lower(): c for c in df.columns}
    open_col = colmap.get("open")
    close_col = colmap.get("close")
    if open_col is None or close_col is None:
        # Sometimes sub-frames look like columns ['Open','High','Low','Close',...]
        # Try exact title-case
        open_col = "Open" if "Open" in df.columns else open_col
        close_col = "Close" if "Close" in df.columns else close_col
        if open_col not in df.columns or close_col not in df.columns:
            return pd.DataFrame(columns=["date", "ticker", "open", "close"])

    out = df[[open_col, close_col]].rename(columns={open_col: "open", close_col: "close"}).copy()

    # Turn index into date column (normalize to NY date, tz-naive)
    idx = pd.to_datetime(df.index, utc=True, errors="coerce")
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    idx = idx.tz_convert("America/New_York").tz_localize(None).normalize()

    out.insert(0, "date", idx)
    out["ticker"] = ticker
    return out[["date", "ticker", "open", "close"]].dropna().reset_index(drop=True)


def _normalize_from_multi(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    df is the full multi-ticker frame from yf.download(group_by='ticker').
    We extract the subframe for `ticker` regardless of column order (ticker-first or field-first).
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "ticker", "open", "close"])

    if not isinstance(df.columns, pd.MultiIndex):
        # Single-ticker case sneaks in sometimes
        return _normalize_single(df, ticker)

    # Case A: columns like (ticker, 'Open'/'Close')
    if ticker in df.columns.get_level_values(0):
        sub = df[ticker]
        return _normalize_single(sub, ticker)

    # Case B: columns like ('Open'/'Close', ticker)
    if ticker in df.columns.get_level_values(1):
        try:
            sub = df.xs(ticker, level=1, axis=1)
            return _normalize_single(sub, ticker)
        except Exception:
            pass

    # Nothing found
    return pd.DataFrame(columns=["date", "ticker", "open", "close"])


def _chunks(seq: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i:i+n]


def fetch_prices_yf_many(
    tickers: List[str],
    start: str,
    end: str,
    chunk_size: int = 50,
    tries: int = 3,
    pause: float = 2.0,
) -> pd.DataFrame:
    """
    Robust, chunked multi-ticker download with retries. Returns normalized rows:
    ['date','ticker','open','close'] for all tickers that resolved.
    """
    all_rows = []

    for chunk in _chunks(tickers, chunk_size):
        last_df = None
        # Try start/end; if it fails repeatedly, fall back to 'period'
        for attempt in range(1, tries + 1):
            try:
                last_df = yf.download(
                    tickers=" ".join(chunk),
                    start=start,
                    end=end,
                    group_by="ticker",
                    auto_adjust=False,
                    progress=False,
                    threads=False,   # keep Yahoo happy
                    interval="1d",
                )
            except Exception:
                last_df = None

            if last_df is not None and not last_df.empty:
                break
            time.sleep(pause)

        # If still empty, try a period fallback
        if last_df is None or last_df.empty:
            for attempt in range(1, tries + 1):
                try:
                    last_df = yf.download(
                        tickers=" ".join(chunk),
                        period="1y",
                        group_by="ticker",
                        auto_adjust=False,
                        progress=False,
                        threads=False,
                        interval="1d",
                    )
                except Exception:
                    last_df = None
                if last_df is not None and not last_df.empty:
                    break
                time.sleep(pause)

        # Normalize per ticker in the chunk
        for t in chunk:
            sub = _normalize_from_multi(last_df, t) if last_df is not None else pd.DataFrame()
            if sub is None or sub.empty:
                # Final fallback: single ticker
                try:
                    single = yf.download(
                        tickers=t, start=start, end=end,
                        auto_adjust=False, progress=False, threads=False, interval="1d",
                    )
                    sub = _normalize_single(single, t)
                except Exception:
                    sub = pd.DataFrame(columns=["date", "ticker", "open", "close"])
            if not sub.empty:
                all_rows.append(sub)

    if not all_rows:
        return pd.DataFrame(columns=["date", "ticker", "open", "close"])
    out = pd.concat(all_rows, ignore_index=True).dropna()
    # Deduplicate (rare Yahoo duplicates)
    out = out.drop_duplicates(["date", "ticker"]).sort_values(["ticker", "date"]).reset_index(drop=True)
    return out


def fetch_prices_yf(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Keep a single-ticker API for any legacy use.
    """
    try:
        raw = yf.download(
            tickers=ticker,
            start=start,
            end=end,
            auto_adjust=False,
            progress=False,
            threads=False,
            interval="1d",
        )
    except Exception:
        raw = None

    df = _normalize_single(raw, ticker)
    if df.empty:
        # Fallback to period
        try:
            raw = yf.download(
                tickers=ticker,
                period="1y",
                auto_adjust=False,
                progress=False,
                threads=False,
                interval="1d",
            )
        except Exception:
            raw = None
        df = _normalize_single(raw, ticker)
    return df
