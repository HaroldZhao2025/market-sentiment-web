# src/market_sentiment/aggregate.py
from __future__ import annotations

import pandas as pd


def _effective_date(ts_utc: pd.Series, cutoff_minutes: int = 5) -> pd.Series:
    """
    Convert UTC timestamps to *calendar date in New York time*,
    applying an early-morning cutoff so late-evening headlines roll to next day.
    """
    s = pd.to_datetime(ts_utc, utc=True, errors="coerce")
    et = s.dt.tz_convert("America/New_York")
    eff = et - pd.to_timedelta(cutoff_minutes, unit="m")
    # normalize to midnight local, then drop tz for safe merges
    return eff.dt.normalize().dt.tz_localize(None)


def daily_sentiment_from_rows(rows: pd.DataFrame, kind: str, cutoff_minutes: int = 5) -> pd.DataFrame:
    """
    rows: columns must contain ['ticker','ts','S'].
    Returns daily sums by (date,ticker) with column S_{kind} and counts.
    """
    req = {"ticker", "ts", "S"}
    if not req.issubset(rows.columns):
        raise KeyError(f"{kind} rows must have columns: ticker, ts, S.")

    d = rows.copy()
    d["date"] = _effective_date(d["ts"], cutoff_minutes=cutoff_minutes)

    val_col = f"S_{kind}"
    cnt_col = f"{kind}_count"
    d[val_col] = pd.to_numeric(d["S"], errors="coerce").fillna(0.0)

    def one_day(df):
        return pd.DataFrame({
            "ticker": df["ticker"].iloc[:1].tolist() * len(df.groupby("ticker")),
            val_col: [df[val_col].sum()],
            cnt_col: [df[val_col].astype(bool).sum()],
            "date": [df["date"].iloc[0]],
        })

    # group_keys=False removes the deprecation warning about including group columns
    daily = d.groupby(["date", "ticker"], group_keys=False).apply(
        lambda g: pd.DataFrame({
            "date": [g["date"].iloc[0]],
            "ticker": [g["ticker"].iloc[0]],
            val_col: [g[val_col].sum()],
            cnt_col: [g[val_col].astype(bool).sum()],
        })
    ).reset_index(drop=True)

    return daily[["date", "ticker", val_col, cnt_col]]


def join_and_fill_daily(d_news: pd.DataFrame | None, d_earn: pd.DataFrame | None) -> pd.DataFrame:
    cols = ["date", "ticker"]
    dn = d_news.copy() if (isinstance(d_news, pd.DataFrame) and not d_news.empty) else pd.DataFrame(columns=cols + ["S_news", "news_count"])
    de = d_earn.copy() if (isinstance(d_earn, pd.DataFrame) and not d_earn.empty) else pd.DataFrame(columns=cols + ["S_earn", "earn_count"])

    # ensure consistent dtypes for safe merge
    for df in (dn, de):
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)

    df = pd.merge(dn, de, on=cols, how="outer")
    for c, default in [("S_news", 0.0), ("S_earn", 0.0), ("news_count", 0), ("earn_count", 0)]:
        if c not in df.columns:
            df[c] = default
        df[c] = df[c].fillna(default).infer_objects(copy=False)
    df["S"] = df["S_news"] + df["S_earn"]
    return df[["date", "ticker", "S", "S_news", "S_earn", "news_count", "earn_count"]]
