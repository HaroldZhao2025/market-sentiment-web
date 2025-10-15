from __future__ import annotations
import pandas as pd
import yfinance as yf

def fetch_prices(ticker: str, start: str, end: str, interval: str = "1d") -> pd.DataFrame:
    """
    Download OHLCV from Yahoo via yfinance and normalize columns.
    Guarantees columns: ['date','ticker','open','close','volume'].
    'date' is tz-aware UTC (for consistent downstream conversions).
    """
    try:
        df = yf.download(
            tickers=ticker,
            start=start,
            end=end,
            interval=interval,
            progress=False,
            auto_adjust=False,
            actions=False,
            threads=False,
        )
    except Exception:
        df = pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame(columns=["date","ticker","open","close","volume"])

    # Reset index to have a 'Date' column, then rename to our schema
    df = df.reset_index()
    date_col = "Date" if "Date" in df.columns else "date"
    out = df.rename(
        columns={
            date_col: "date",
            "Open": "open",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    )
    # Coerce to UTC (yfinance often returns tz-naive in local or UTC)
    out["date"] = pd.to_datetime(out["date"], errors="coerce", utc=True)
    out["ticker"] = ticker
    # Keep minimal set needed downstream
    cols = ["date", "ticker", "open", "close", "volume"]
    out = out[[c for c in cols if c in out.columns]].sort_values("date").reset_index(drop=True)
    return out
