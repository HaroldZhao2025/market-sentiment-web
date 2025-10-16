from __future__ import annotations

import pandas as pd


def _effective_date(ts, cutoff_minutes: int = 5) -> pd.Series:
    """
    Convert timestamps to effective NY trading date, moving anything within
    'cutoff_minutes' after midnight back to previous date.
    """
    t = pd.to_datetime(ts, utc=True, errors="coerce")
    ny = t.tz_convert("America/New_York")
    # normalize to date
    d = ny.dt.normalize()
    # handle tiny-after-midnight edge case
    minute = (ny - d).dt.total_seconds() / 60.0
    d = d.where(minute > cutoff_minutes, d - pd.Timedelta(days=1))
    return d.dt.tz_localize(None)


def daily_sentiment_from_rows(rows: pd.DataFrame, kind: str, cutoff_minutes: int = 5) -> pd.DataFrame:
    """
    rows: columns ['ticker','ts','S'] (+ anything else). Returns per-day per-ticker rollup.
    Output: ['date','ticker','S_*','*_count'] depending on kind in {'news','earn'}
    """
    if rows is None or rows.empty:
        cols = ["date", "ticker"]
        if kind == "news":
            cols += ["S_news", "news_count"]
        else:
            cols += ["S_earn", "earn_count"]
        return pd.DataFrame(columns=cols)

    d = rows.copy()
    for c in ["ticker", "ts"]:
        if c not in d.columns:
            raise KeyError(f"{kind} rows must have columns: ticker, ts, S.")
    if "S" not in d.columns:
        d["S"] = 0.0

    d["date"] = _effective_date(d["ts"], cutoff_minutes=cutoff_minutes)

    if kind == "news":
        g = (
            d.groupby(["date", "ticker"], as_index=False)
            .agg(S_news=("S", "mean"), news_count=("S", "size"))
        )
        return g
    else:
        g = (
            d.groupby(["date", "ticker"], as_index=False)
            .agg(S_earn=("S", "mean"), earn_count=("S", "size"))
        )
        return g


def join_and_fill_daily(d_news: pd.DataFrame, d_earn: pd.DataFrame) -> pd.DataFrame:
    """
    Outer join news and earnings daily frames, fill NaNs with zeros, compute S = avg(news,earn)
    """
    cols = ["date", "ticker"]
    if d_news is None or d_news.empty:
        d_news = pd.DataFrame(columns=cols + ["S_news", "news_count"])
    if d_earn is None or d_earn.empty:
        d_earn = pd.DataFrame(columns=cols + ["S_earn", "earn_count"])

    df = pd.merge(d_news, d_earn, on=cols, how="outer", suffixes=("", ""))
    for c, default in [("S_news", 0.0), ("S_earn", 0.0), ("news_count", 0), ("earn_count", 0)]:
        if c not in df.columns:
            df[c] = default

    df["S_news"] = pd.to_numeric(df["S_news"], errors="coerce").fillna(0.0)
    df["S_earn"] = pd.to_numeric(df["S_earn"], errors="coerce").fillna(0.0)
    df["news_count"] = pd.to_numeric(df["news_count"], errors="coerce").fillna(0).astype(int)
    df["earn_count"] = pd.to_numeric(df["earn_count"], errors="coerce").fillna(0).astype(int)
    df["S"] = (df["S_news"] + df["S_earn"]) / 2.0

    # enforce dtypes / order
    out = df[["date", "ticker", "S", "S_news", "S_earn", "news_count", "earn_count"]].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    return out.sort_values(["ticker", "date"]).reset_index(drop=True)


def add_forward_returns(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Input: ['date','ticker','open','close']
    Output adds: ret_cc_1d (close->close next day), ret_oc_1d (overnight open-close)
    """
    if prices is None or prices.empty:
        return pd.DataFrame(columns=["date", "ticker", "open", "close", "ret_cc_1d", "ret_oc_1d"])

    out = prices.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    out = out.sort_values(["ticker", "date"]).reset_index(drop=True)

    # next-day close for each ticker
    out["close_next"] = out.groupby("ticker")["close"].shift(-1)
    out["ret_cc_1d"] = (pd.to_numeric(out["close_next"], errors="coerce") -
                        pd.to_numeric(out["close"], errors="coerce")) / pd.to_numeric(out["close"], errors="coerce")
    # same-day open->close
    out["ret_oc_1d"] = (pd.to_numeric(out["close"], errors="coerce") -
                        pd.to_numeric(out["open"], errors="coerce")) / pd.to_numeric(out["open"], errors="coerce")

    out = out.drop(columns=["close_next"])
    for c in ["ret_cc_1d", "ret_oc_1d"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)

    return out
