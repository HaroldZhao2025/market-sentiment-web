# src/market_sentiment/aggregate.py
from __future__ import annotations

import pandas as pd


# --------------------------
# Internal helpers
# --------------------------

def _as_series(x) -> pd.Series:
    if isinstance(x, pd.Series):
        return x
    if isinstance(x, pd.DatetimeIndex):
        return pd.Series(x)
    return pd.Series(x)


def _effective_date(ts, cutoff_minutes: int = 5) -> pd.Series:
    """
    Map UTC timestamps to an *effective* New York date with a small cutoff.
      1) Coerce to tz-aware UTC
      2) Convert to America/New_York
      3) Subtract cutoff minutes
      4) Normalize to date, drop tz
    Returns tz-naive datetime64[ns].
    """
    s = pd.to_datetime(_as_series(ts), utc=True, errors="coerce")
    if getattr(s.dt, "tz", None) is None:
        s = s.dt.tz_localize("UTC")

    local = s.dt.tz_convert("America/New_York")
    eff_local = (local - pd.to_timedelta(cutoff_minutes, unit="m")).dt.normalize()
    return eff_local.dt.tz_localize(None)


def _ensure_date_dtype(df: pd.DataFrame, col: str = "date") -> pd.DataFrame:
    """Force df[col] to tz-naive datetime64[ns] normalized to midnight."""
    if df is None or df.empty or col not in df.columns:
        return df
    d = pd.to_datetime(df[col], utc=False, errors="coerce")
    try:
        d = d.dt.tz_localize(None)
    except Exception:
        pass
    df[col] = d.dt.normalize()
    return df


# --------------------------
# Public helpers used by CLI
# --------------------------

def daily_sentiment_from_rows(
    rows: pd.DataFrame,
    kind: str,
    cutoff_minutes: int = 5,
) -> pd.DataFrame:
    """
    Aggregate raw rows -> daily per-ticker sentiment.

    Input columns expected:
      - ticker (str)
      - ts (UTC timestamps or parseable)
      - S (float sentiment per item)

    Output columns:
      - date (tz-naive datetime64[ns])
      - ticker
      - S   (daily mean sentiment for the kind)
    """
    if rows is None or rows.empty:
        return pd.DataFrame(columns=["date", "ticker", "S"])

    required = {"ticker", "ts", "S"}
    missing = required - set(rows.columns)
    if missing:
        raise KeyError(f"{kind} rows must have columns: ticker, ts, S. Missing: {sorted(missing)}")

    df = rows.copy()
    df["S"] = pd.to_numeric(df["S"], errors="coerce")
    df = df.dropna(subset=["S"])

    df["date"] = _effective_date(df["ts"], cutoff_minutes=cutoff_minutes)
    df = df.dropna(subset=["date"])[["date", "ticker", "S"]]

    def one_day(g: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({
            "date": [g["date"].iloc[0]],
            "ticker": [g["ticker"].iloc[0]],
            "S": [float(g["S"].mean())],
        })

    daily = (
        df.groupby(["date", "ticker"], as_index=False)
          .apply(one_day)
          .reset_index(drop=True)
    )
    daily = _ensure_date_dtype(daily, "date")
    return daily[["date", "ticker", "S"]]


def join_and_fill_daily(d_news: pd.DataFrame, d_earn: pd.DataFrame) -> pd.DataFrame:
    """
    Outer-join daily news and earnings sentiment on (date, ticker),
    coerce dates to a consistent dtype, and fill missing with 0.0.

    Returns columns:
      - date, ticker, S_news, S_earn, S  (where S = S_news + S_earn)
    """
    if d_news is not None and not d_news.empty:
        dn = d_news.copy()
    else:
        dn = pd.DataFrame(columns=["date", "ticker", "S"])

    if d_earn is not None and not d_earn.empty:
        de = d_earn.copy()
    else:
        de = pd.DataFrame(columns=["date", "ticker", "S"])

    dn = _ensure_date_dtype(dn, "date")
    de = _ensure_date_dtype(de, "date")

    if "S" in dn.columns:
        dn = dn.rename(columns={"S": "S_news"})
    else:
        dn["S_news"] = 0.0

    if "S" in de.columns:
        de = de.rename(columns={"S": "S_earn"})
    else:
        de["S_earn"] = 0.0

    cols = ["date", "ticker"]
    for df in (dn, de):
        for c in cols:
            if c not in df.columns:
                df[c] = pd.NaT if c == "date" else ""

    out = pd.merge(dn[cols + ["S_news"]], de[cols + ["S_earn"]], on=cols, how="outer")

    for c in ("S_news", "S_earn"):
        if c not in out.columns:
            out[c] = 0.0
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)

    out = _ensure_date_dtype(out, "date")
    out["S"] = out["S_news"] + out["S_earn"]
    out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
    return out[["date", "ticker", "S_news", "S_earn", "S"]]


def add_forward_returns(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Given daily prices [date, ticker, open, close], compute forward returns:
      - ret_oc_1d: (close/open) - 1 for same day
      - ret_cc_1d: next_day_close / close - 1

    Returns the same frame with added columns.
    """
    if prices is None or prices.empty:
        return pd.DataFrame(columns=["date", "ticker", "open", "close", "ret_oc_1d", "ret_cc_1d"])

    df = prices.copy()
    df = _ensure_date_dtype(df, "date")

    df["ret_oc_1d"] = pd.to_numeric(df["close"], errors="coerce") / pd.to_numeric(df["open"], errors="coerce") - 1.0
    df = df.sort_values(["ticker", "date"])
    df["next_close"] = df.groupby("ticker")["close"].shift(-1)
    df["ret_cc_1d"] = (pd.to_numeric(df["next_close"], errors="coerce") / pd.to_numeric(df["close"], errors="coerce")) - 1.0
    df = df.drop(columns=["next_close"])
    return df
