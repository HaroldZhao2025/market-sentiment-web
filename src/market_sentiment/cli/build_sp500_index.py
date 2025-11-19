from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List

import pandas as pd
import yfinance as yf

# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------

INDEX_YF_SYMBOL = "^GSPC"   # yfinance symbol for S&P 500 index
INDEX_SYMBOL = "SPX"        # symbol name to expose on site
INDEX_NAME = "S&P 500 Index"


# ----------------------------------------------------------------------
# Date helpers
# ----------------------------------------------------------------------

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


# ----------------------------------------------------------------------
# SPX price & news
# ----------------------------------------------------------------------

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


# ----------------------------------------------------------------------
# Market cap handling
# ----------------------------------------------------------------------

def _find_symbol_column(uni: pd.DataFrame) -> str:
    for cand in ["symbol", "ticker", "Symbol", "Ticker"]:
        if cand in uni.columns:
            return cand
    raise KeyError(f"Could not find symbol/ticker column in universe: {uni.columns.tolist()}")


def _find_market_cap_column(uni: pd.DataFrame) -> Optional[str]:
    for cand in ["marketCap", "market_cap", "MarketCap", "marketcap"]:
        if cand in uni.columns:
            return cand
    return None


def fetch_market_caps_from_yf(symbols: List[str]) -> pd.DataFrame:
    """
    Fetch market caps for all symbols using yfinance.

    We use yf.Tickers(...) and then, for each symbol, try:
      - fast_info["market_cap"]
      - info["marketCap"]
    """
    print(f"[SPX] Fetching market caps from yfinance for {len(symbols)} tickers ...")
    # yfinance Tickers takes space-separated or comma-separated string
    multi = yf.Tickers(" ".join(symbols))

    records: List[Dict[str, Any]] = []

    for sym in symbols:
        mc = None
        try:
            t = multi.tickers.get(sym) or yf.Ticker(sym)

            # Try fast_info first
            fi = getattr(t, "fast_info", None)
            if isinstance(fi, dict):
                mc = fi.get("market_cap")
            else:
                # Some versions expose attributes instead of dict
                mc = getattr(fi, "market_cap", None)

            # Fallback: full info
            if mc is None:
                info = getattr(t, "info", {}) or {}
                if isinstance(info, dict):
                    mc = info.get("marketCap")

        except Exception as e:
            print(f"[SPX] Warning: failed to fetch market cap for {sym}: {e!r}")
            mc = None

        records.append({"symbol": sym, "market_cap": mc})

    caps = pd.DataFrame(records)
    caps = caps.dropna(subset=["market_cap"])
    caps = caps[caps["market_cap"] > 0]
    print(f"[SPX] Got positive market caps for {len(caps)} of {len(symbols)} tickers")
    return caps


def load_universe_with_weights(
    universe_path: Path,
) -> pd.DataFrame:
    """
    Load universe and ensure we have a 'symbol' column and a 'weight' column.

    If no market cap column exists, we fetch market caps from yfinance.
    """
    uni = pd.read_csv(universe_path)

    # --- symbol column ---
    sym_col = _find_symbol_column(uni)
    uni = uni.rename(columns={sym_col: "symbol"})

    # --- market cap column ---
    mktcap_col = _find_market_cap_column(uni)
    if mktcap_col is None:
        # No market cap in universe: fetch from yfinance
        symbols = sorted(uni["symbol"].astype(str).unique().tolist())
        caps = fetch_market_caps_from_yf(symbols)
        if caps.empty:
            raise ValueError("[SPX] Could not fetch any market caps from yfinance")

        uni = uni.merge(caps, on="symbol", how="inner")
    else:
        uni = uni.rename(columns={mktcap_col: "market_cap"})

    uni = uni.dropna(subset=["market_cap"])
    uni = uni[uni["market_cap"] > 0]

    total_cap = float(uni["market_cap"].sum())
    if total_cap <= 0:
        raise ValueError("[SPX] Total market cap is non-positive")

    uni["weight"] = uni["market_cap"] / total_cap
    print(f"[SPX] Loaded {len(uni)} tickers with positive market cap for weighting")
    return uni[["symbol", "market_cap", "weight"]]


# ----------------------------------------------------------------------
# Sentiment aggregation
# ----------------------------------------------------------------------

def load_ticker_daily_sentiment(
    data_root: Path,
    symbol: str,
    daily_key: str = "daily",
    sentiment_key: str = "sentiment",
) -> pd.DataFrame:
    """
    Load per-ticker JSON and return ['date', 'sentiment'].

    Assumptions (adapt to your build_json output if needed):
      - File: {data_root}/ticker/{symbol}.json
      - Has a list under key 'daily' (fallback to 'series' or 'timeline').
      - Each row has date + sentiment fields.
    """
    path = data_root / "ticker" / f"{symbol}.json"
    if not path.exists():
        return pd.DataFrame(columns=["date", "sentiment"])

    with path.open("r") as f:
        obj = json.load(f)

    # Time-series key
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

    # Date column
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
    For each date t, compute cap-weighted sentiment:

        Sent_t = sum_i w_i * s_i,t  /  sum_i w_i (over tickers with sentiment on day t)
    """
    start_date = datetime.fromisoformat(start).date()
    end_date = datetime.fromisoformat(end).date()

    frames: List[pd.DataFrame] = []

    for _, row in universe.iterrows():
        symbol = row["symbol"]
        base_weight = float(row["weight"])

        df = load_ticker_daily_sentiment(data_root, symbol)
        if df.empty:
            continue

        df["symbol"] = symbol
        df["base_weight"] = base_weight
        frames.append(df)

    if not frames:
        print("[SPX] No per-ticker sentiment found in data_root")
        return pd.DataFrame(columns=["date", "sentiment_cap_weighted"])

    panel = pd.concat(frames, ignore_index=True)
    panel["date"] = pd.to_datetime(panel["date"]).dt.date
    panel = panel[(panel["date"] >= start_date) & (panel["date"] <= end_date)]

    if panel.empty:
        print("[SPX] Panel empty after date filter")
        return pd.DataFrame(columns=["date", "sentiment_cap_weighted"])

    def _agg_one_day(g: pd.DataFrame) -> float:
        w = g["base_weight"].astype(float)
        s = g["sentiment"].astype(float)
        mask = s.notna()
        w = w[mask]
        s = s[mask]
        if w.empty:
            return float("nan")
        # Renormalise among tickers that actually have sentiment that day
        w = w / w.sum()
        return float((w * s).sum())

    out = (
        panel.groupby("date")
        .apply(_agg_one_day)
        .reset_index(name="sentiment_cap_weighted")
    )
    out["date"] = out["date"].dt.date.astype(str)
    out = out.sort_values("date").reset_index(drop=True)

    print(f"[SPX] Built cap-weighted sentiment for {len(out)} days")
    return out


# ----------------------------------------------------------------------
# Payload builder
# ----------------------------------------------------------------------

def build_sp500_index_payload(
    prices: pd.DataFrame,
    sentiment: pd.DataFrame,
    news: pd.DataFrame,
) -> Dict[str, Any]:
    """
    Merge price + sentiment + news into a single JSON payload.
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


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------

def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Build S&P 500 index JSON (price + news from yfinance, cap-weighted sentiment from per-ticker JSON)."
    )
    parser.add_argument(
        "--universe",
        type=str,
        required=True,
        help="Path to universe CSV (e.g. data/sp500.csv). Needs a ticker/symbol column.",
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
        help="Output directory (usually same as --data-root).",
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
