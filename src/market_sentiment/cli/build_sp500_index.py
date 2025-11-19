from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List

import pandas as pd
import yfinance as yf


# -------------------- constants --------------------

INDEX_YF_SYMBOL = "^GSPC"         # yfinance symbol
INDEX_SYMBOL = "SPX"              # index symbol used on your site
INDEX_NAME = "S&P 500 Index"


# -------------------- date helpers --------------------

def _parse_date(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _default_end_today_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _default_start_one_year(end_str: str) -> str:
    end_dt = datetime.fromisoformat(end_str)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)
    start_dt = end_dt - timedelta(days=365)
    return start_dt.date().isoformat()


# -------------------- SPX price & news --------------------

def download_spx_prices(start: str, end: str) -> pd.DataFrame:
    """
    Daily SPX prices from yfinance (^GSPC).
    Returns columns: ['date', 'close'].
    """
    print(f"[SPX] Downloading prices {start} → {end} ...")
    df = yf.download(
        INDEX_YF_SYMBOL,
        start=start,
        end=end,
        auto_adjust=True,
        progress=False,
        interval="1d",
    )
    if df.empty:
        raise RuntimeError("[SPX] No price data returned for ^GSPC")

    df = df.reset_index()

    if "Close" in df.columns:
        close_col = "Close"
    elif "Adj Close" in df.columns:
        close_col = "Adj Close"
    else:
        raise RuntimeError("[SPX] Close/Adj Close missing for ^GSPC")

    df = df[["Date", close_col]].rename(columns={"Date": "date", close_col: "close"})
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    df = df.sort_values("date").reset_index(drop=True)
    print(f"[SPX] Got {len(df)} price rows")
    return df


def download_spx_news(start: str, end: str, max_items: int = 500) -> pd.DataFrame:
    """
    yfinance news for ^GSPC.
    Returns columns: ['date', 'title', 'publisher', 'link'].
    """
    print(f"[SPX] Fetching news for {INDEX_YF_SYMBOL} ...")
    t = yf.Ticker(INDEX_YF_SYMBOL)
    raw = t.news or []
    if not raw:
        print("[SPX] No news returned from yfinance")
        return pd.DataFrame(columns=["date", "title", "publisher", "link"])

    start_dt = _parse_date(start)
    end_dt = _parse_date(end) + timedelta(days=1)

    rows: List[Dict[str, Any]] = []
    for item in raw[:max_items]:
        ts = item.get("providerPublishTime")
        if ts is None:
            continue
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        if not (start_dt <= dt < end_dt):
            continue
        rows.append(
            {
                "date": dt.date().isoformat(),
                "title": item.get("title", ""),
                "publisher": item.get("publisher", ""),
                "link": item.get("link") or item.get("url") or "",
            }
        )

    if not rows:
        print("[SPX] No news in requested date range")
        return pd.DataFrame(columns=["date", "title", "publisher", "link"])

    news_df = (
        pd.DataFrame(rows)
        .sort_values(["date", "publisher", "title"])
        .reset_index(drop=True)
    )
    print(f"[SPX] Got {len(news_df)} news articles in range")
    return news_df


# -------------------- cap-weighted sentiment --------------------

def load_universe_with_weights(
    universe_path: Path,
    symbol_col: str = "symbol",
    mktcap_col: str = "marketCap",
) -> pd.DataFrame:
    """
    Load S&P 500 universe and compute static cap weights.
    Supports several fallback column names to match your CSV.
    """
    uni = pd.read_csv(universe_path)

    # symbol column
    if symbol_col not in uni.columns:
        for cand in ["Symbol", "ticker", "Ticker"]:
            if cand in uni.columns:
                symbol_col = cand
                break
        else:
            raise KeyError(f"Could not find symbol column in universe: {uni.columns.tolist()}")

    # market cap column
    if mktcap_col not in uni.columns:
        for cand in ["market_cap", "MarketCap", "marketcap"]:
            if cand in uni.columns:
                mktcap_col = cand
                break
        else:
            raise KeyError(f"Could not find market cap column in universe: {uni.columns.tolist()}")

    uni = uni[[symbol_col, mktcap_col]].rename(
        columns={symbol_col: "symbol", mktcap_col: "market_cap"}
    )
    uni = uni.dropna(subset=["market_cap"])
    uni = uni[uni["market_cap"] > 0]

    total_cap = float(uni["market_cap"].sum())
    if total_cap <= 0:
        raise ValueError("Total market cap is non-positive")

    uni["weight"] = uni["market_cap"] / total_cap
    print(f"[SPX] Loaded {len(uni)} tickers with positive market cap")
    return uni


def load_ticker_daily_sentiment(
    data_root: Path,
    symbol: str,
    daily_key: str = "daily",
    sentiment_key: str = "sentiment",
) -> pd.DataFrame:
    """
    Load per-ticker JSON and return ['date', 'sentiment'].

    Assumptions (matches your existing build_json output):
      - File: {data_root}/ticker/{symbol}.json
      - Has a top-level list under key `daily` (or `series` / `timeline` fallback)
      - Inside each row: a date field and a sentiment field.

    If structure differs, adjust `daily_key` / `sentiment_key` or the fallback logic.
    """
    path = data_root / "ticker" / f"{symbol}.json"
    if not path.exists():
        return pd.DataFrame(columns=["date", "sentiment"])

    with path.open("r") as f:
        obj = json.load(f)

    # find the time-series key
    if daily_key not in obj:
        for cand in ["series", "timeline"]:
            if cand in obj:
                daily_key = cand
                break
        else:
            return pd.DataFrame(columns=["date", "sentiment"])

    rows = obj.get(daily_key) or []
    if not isinstance(rows, list) or not rows:
        return pd.DataFrame(columns=["date", "sentiment"])

    df = pd.DataFrame(rows)

    # choose date column
    date_col = None
    for cand in ["date", "Date", "day"]:
        if cand in df.columns:
            date_col = cand
            break
    if date_col is None or sentiment_key not in df.columns:
        return pd.DataFrame(columns=["date", "sentiment"])

    df = df[[date_col, sentiment_key]].rename(
        columns={date_col: "date", sentiment_key: "sentiment"}
    )
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    return df


def compute_cap_weighted_sentiment(
    universe: pd.DataFrame,
    data_root: Path,
    start: str,
    end: str,
) -> pd.DataFrame:
    """
    For each day, compute cap-weighted sentiment:
      Sent_index_t = sum_i w_i,t * s_i,t
    where w_i,t is the (re-normalised) market-cap weight among tickers
    that actually have sentiment on day t.
    """
    start_dt = datetime.fromisoformat(start).date()
    end_dt = datetime.fromisoformat(end).date()

    frames: List[pd.DataFrame] = []

    for _, row in universe.iterrows():
        symbol = row["symbol"]
        weight = row["weight"]
        df = load_ticker_daily_sentiment(data_root, symbol)
        if df.empty:
            continue
        df["symbol"] = symbol
        df["weight"] = weight
        frames.append(df)

    if not frames:
        print("[SPX] No per-ticker sentiment found")
        return pd.DataFrame(columns=["date", "sentiment_cap_weighted"])

    panel = pd.concat(frames, ignore_index=True)
    panel["date"] = pd.to_datetime(panel["date"]).dt.date
    panel = panel[(panel["date"] >= start_dt) & (panel["date"] <= end_dt)]

    if panel.empty:
        print("[SPX] Panel empty after date filter")
        return pd.DataFrame(columns=["date", "sentiment_cap_weighted"])

    def _agg(group: pd.DataFrame) -> float:
        w = group["weight"].astype(float)
        s = group["sentiment"].astype(float)
        mask = s.notna()
        w = w[mask]
        s = s[mask]
        if w.empty:
            return float("nan")
        w = w / w.sum()  # renormalise among tickers with sentiment that day
        return float((w * s).sum())

    out = (
        panel.groupby("date", as_index=False)
        .apply(lambda g: pd.Series({"sentiment_cap_weighted": _agg(g)}))
    )
    out["date"] = out["date"].dt.date.astype(str)
    out = out.sort_values("date").reset_index(drop=True)

    print(f"[SPX] Built cap-weighted sentiment for {len(out)} days")
    return out


# -------------------- payload --------------------

def build_sp500_index_payload(
    prices: pd.DataFrame,
    sentiment: pd.DataFrame,
    news: pd.DataFrame,
) -> Dict[str, Any]:
    """
    Merge price + sentiment + news into one JSON payload.
    """
    daily = prices.merge(sentiment, on="date", how="left")
    daily = daily.sort_values("date").reset_index(drop=True)

    payload: Dict[str, Any] = {
        "symbol": INDEX_SYMBOL,
        "name": INDEX_NAME,
        "yf_symbol": INDEX_YF_SYMBOL,
        "daily": daily.to_dict(orient="records"),
        "news": news.sort_values("date").to_dict(orient="records"),
    }
    return payload


# -------------------- CLI --------------------

def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Build S&P 500 index JSON (price + news from yfinance, cap-weighted sentiment from per-ticker JSON)."
    )
    parser.add_argument(
        "--universe",
        type=str,
        required=True,
        help="Path to universe CSV (e.g. data/sp500.csv).",
    )
    parser.add_argument(
        "--data-root",
        type=str,
        required=True,
        help="Root data dir where per-ticker JSON lives (e.g. apps/web/public/data).",
    )
    parser.add_argument(
        "--out",
        type=str,
        required=True,
        help="Output directory (usually the same as --data-root).",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start date YYYY-MM-DD (default: end-365d).",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date YYYY-MM-DD (default: today UTC).",
    )
    parser.add_argument(
        "--max-news",
        type=int,
        default=500,
        help="Max yfinance news items to pull for SPX.",
    )

    args = parser.parse_args(argv)

    end_str = args.end or _default_end_today_utc()
    start_str = args.start or _default_start_one_year(end_str)

    print(f"[SPX] Date range: {start_str} → {end_str}")

    universe = load_universe_with_weights(Path(args.universe))
    data_root = Path(args.data_root)

    prices = download_spx_prices(start_str, end_str)
    sentiment = compute_cap_weighted_sentiment(universe, data_root, start_str, end_str)
    news = download_spx_news(start_str, end_str, max_items=args.max_news)

    payload = build_sp500_index_payload(prices, sentiment, news)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "sp500_index.json"
    with out_path.open("w") as f:
        json.dump(payload, f, indent=2)

    print(f"[SPX] Wrote {out_path}")


if __name__ == "__main__":
    main()
