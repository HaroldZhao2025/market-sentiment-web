from __future__ import annotations

import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
import yfinance as yf

# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------

# For prices we try these in order
INDEX_PRICE_SYMBOL_CANDIDATES = ["^GSPC", "^SPX", "SPY"]

# For news we try ^SPX first (your manual pattern), then fallbacks
INDEX_NEWS_SYMBOL_CANDIDATES = ["^SPX", "^GSPC", "SPY"]

INDEX_SYMBOL = "SPX"        # symbol exposed on the site
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
# Utility: make DataFrame "flat" before merge
# ----------------------------------------------------------------------

def _make_flat(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure df has no MultiIndex in index or columns to make merges safe.
    """
    df = df.copy()
    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index()
    if isinstance(df.columns, pd.MultiIndex):
        new_cols = []
        for col in df.columns:
            if isinstance(col, tuple):
                new_cols.append("_".join(str(c) for c in col if c not in ("", None)))
            else:
                new_cols.append(str(col))
        df.columns = new_cols
    return df


# ----------------------------------------------------------------------
# SPX price & news
# ----------------------------------------------------------------------

def download_spx_prices(start: str, end: str) -> pd.DataFrame:
    """
    Try a few symbols for S&P 500 price: ^GSPC, ^SPX, SPY.
    Returns ['date', 'close'].
    """
    last_err: Optional[Exception] = None

    for sym in INDEX_PRICE_SYMBOL_CANDIDATES:
        print(f"[SPX] Downloading prices for {sym} {start} → {end} ...")
        try:
            df = yf.download(
                sym,
                start=start,
                end=end,
                auto_adjust=True,
                progress=False,
                interval="1d",
            )
        except Exception as e:
            print(f"[SPX] Error downloading prices for {sym}: {e!r}")
            last_err = e
            continue

        if df.empty:
            print(f"[SPX] No price data returned for {sym}, trying next candidate")
            continue

        df = df.reset_index()

        if "Close" in df.columns:
            close_col = "Close"
        elif "Adj Close" in df.columns:
            close_col = "Adj Close"
        else:
            print(f"[SPX] {sym} missing Close/Adj Close, trying next candidate")
            continue

        df = df[["Date", close_col]].rename(columns={"Date": "date", close_col: "close"})
        df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
        df = df.sort_values("date").reset_index(drop=True)
        print(f"[SPX] Using {sym} for index prices, {len(df)} rows")
        return df

    raise RuntimeError(
        f"[SPX] Could not download index prices for any of {INDEX_PRICE_SYMBOL_CANDIDATES}. "
        f"Last error: {last_err!r}"
    )


def _try_parse_datetime_str(s: str) -> Optional[datetime]:
    """
    Best-effort parse for common timestamp strings. Keep it dependency-free (no dateutil).
    """
    if not s:
        return None

    s = s.strip()
    # common RFC3339 "Z"
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    # try ISO first
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        pass

    # try a couple of common formats
    fmts = [
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%a, %d %b %Y %H:%M:%S %z",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt
        except Exception:
            continue
    return None


def _extract_publish_dt(item: Dict[str, Any]) -> Optional[datetime]:
    """
    yfinance news items are not perfectly stable across versions/endpoints.
    We accept multiple keys and auto-handle seconds vs milliseconds.
    """
    # most common
    ts = item.get("providerPublishTime")

    # alternatives seen in the wild
    if ts is None:
        ts = item.get("providerPublishTimeMs")
    if ts is None:
        ts = item.get("published_at")
    if ts is None:
        ts = item.get("pubDate")
    if ts is None:
        content = item.get("content") if isinstance(item.get("content"), dict) else {}
        ts = content.get("pubDate") or content.get("published_at") or content.get("publishedAt")

    # numeric epoch
    if isinstance(ts, (int, float)):
        v = float(ts)
        # if it's milliseconds, it's around 1e12-1e13; seconds is around 1e9
        if v > 10_000_000_000:  # 1e10 threshold is safe for the 2001-2286 epoch range
            v = v / 1000.0
        try:
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except Exception:
            return None

    # string epoch or ISO
    if isinstance(ts, str):
        # try int epoch first
        try:
            v = float(ts)
            if v > 10_000_000_000:
                v = v / 1000.0
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except Exception:
            return _try_parse_datetime_str(ts)

    return None


def _yf_get_news(t: yf.Ticker, count: int) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Prefer tab="all". Fallback to other signatures / t.news.
    """
    # Prefer: tab="all" -> you already verified it works for ^SPX locally.
    for tab in ("all", "news"):
        try:
            raw = t.get_news(count=count, tab=tab) or []
            if raw:
                return raw, tab
        except TypeError:
            # Some versions may not support tab=
            try:
                raw = t.get_news(count=count) or []
                if raw:
                    return raw, None
            except Exception:
                pass
        except AttributeError:
            # Very old version: no get_news at all
            raw = getattr(t, "news", None) or []
            if raw:
                return raw, None
        except Exception:
            continue

    # last resort
    raw = getattr(t, "news", None) or []
    return raw, None


def download_spx_news(
    start: str,
    end: str,
    max_items: int = 500,
    retries: int = 3,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Try multiple yfinance symbols for S&P 500 news.

    Returns:
      - news_df with columns ['date', 'title', 'publisher', 'link']
      - meta: {'news_source_symbol', 'news_source_tab', 'news_raw_count', 'news_kept_count'}
    """
    start_dt = _parse_date(start)
    # inclusive end date -> add 1 day (same as your original logic)
    end_dt = _parse_date(end) + timedelta(days=1)

    last_err: Optional[Exception] = None

    for sym in INDEX_NEWS_SYMBOL_CANDIDATES:
        print(f"[SPX] Fetching news for {sym} ...")

        raw: List[Dict[str, Any]] = []
        used_tab: Optional[str] = None

        # retries (CI is more likely to hit transient Yahoo issues)
        for attempt in range(retries):
            try:
                t = yf.Ticker(sym)
                raw, used_tab = _yf_get_news(t, count=max_items)
                break
            except Exception as e:
                last_err = e
                sleep_s = (2 ** attempt) + random.random()
                print(f"[SPX] News fetch attempt {attempt+1}/{retries} failed for {sym}: {e!r}. sleep={sleep_s:.2f}s")
                time.sleep(sleep_s)

        if not raw:
            print(f"[SPX] No news returned for {sym}, trying next candidate")
            continue

        rows_in_range: List[Dict[str, Any]] = []
        rows_any: List[Tuple[datetime, Dict[str, Any]]] = []

        for item in raw:
            dt = _extract_publish_dt(item)
            if dt is None:
                continue

            rec = {
                "date": dt.date().isoformat(),
                "title": item.get("title", "") or "",
                "publisher": item.get("publisher", "") or "",
                "link": item.get("link") or item.get("url") or "",
            }

            # keep for fallback sorting
            rows_any.append((dt, rec))

            # in-range filter
            if start_dt <= dt < end_dt:
                rows_in_range.append(rec)

        def _dedup_and_sort(rows: List[Dict[str, Any]]) -> pd.DataFrame:
            if not rows:
                return pd.DataFrame(columns=["date", "title", "publisher", "link"])
            df = pd.DataFrame(rows)
            # dedupe by link if possible, else by (publisher,title,date)
            if "link" in df.columns:
                df["link"] = df["link"].fillna("")
                has_link = df["link"].str.len() > 0
                df_link = df[has_link].drop_duplicates(subset=["link"], keep="first")
                df_nolink = df[~has_link].drop_duplicates(subset=["date", "publisher", "title"], keep="first")
                df = pd.concat([df_link, df_nolink], ignore_index=True)
            else:
                df = df.drop_duplicates(subset=["date", "publisher", "title"], keep="first")

            df = df.sort_values(["date", "publisher", "title"]).reset_index(drop=True)
            return df

        # primary: only take in-range items
        news_df = _dedup_and_sort(rows_in_range)

        # fallback: if in-range becomes empty but raw was non-empty,
        # keep most recent items (prevents CI/version timestamp quirks from wiping all news)
        if news_df.empty and rows_any:
            print(f"[SPX] Warning: {sym} returned news but none survived date-range filter. Falling back to most-recent items.")
            rows_any.sort(key=lambda x: x[0], reverse=True)
            fallback = [r for _, r in rows_any[: min(max_items, len(rows_any))]]
            news_df = _dedup_and_sort(fallback)

        if news_df.empty:
            print(f"[SPX] No usable news rows for {sym} after parsing, trying next candidate")
            continue

        meta = {
            "news_source_symbol": sym,
            "news_source_tab": used_tab,
            "news_raw_count": len(raw),
            "news_kept_count": int(len(news_df)),
        }
        print(f"[SPX] Using {sym} for news (tab={used_tab}), kept {meta['news_kept_count']}/{meta['news_raw_count']}")
        return news_df, meta

    print(f"[SPX] No index news found for any symbol candidate. last_err={last_err!r}")
    empty = pd.DataFrame(columns=["date", "title", "publisher", "link"])
    meta = {
        "news_source_symbol": None,
        "news_source_tab": None,
        "news_raw_count": 0,
        "news_kept_count": 0,
        "news_error": repr(last_err) if last_err else None,
    }
    return empty, meta


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
    """
    print(f"[SPX] Fetching market caps from yfinance for {len(symbols)} tickers ...")
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


def load_universe_with_weights(universe_path: Path) -> pd.DataFrame:
    """
    Load universe and ensure:
      - column 'symbol'
      - column 'weight' from market caps.
    If market caps are missing in CSV, fetch them via yfinance.
    """
    uni = pd.read_csv(universe_path)

    # symbol column
    sym_col = _find_symbol_column(uni)
    uni = uni.rename(columns={sym_col: "symbol"})

    # market cap column
    mktcap_col = _find_market_cap_column(uni)
    if mktcap_col is None:
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
# Sentiment aggregation from data/[ticker]/sentiment/YYYY-MM-DD.json
# ----------------------------------------------------------------------

def load_ticker_daily_sentiment_from_files(
    sentiment_root: Path,
    symbol: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """
    Read daily sentiment from:
        sentiment_root / symbol / "sentiment" / YYYY-MM-DD.json
    """
    folder = sentiment_root / symbol / "sentiment"
    if not folder.exists():
        return pd.DataFrame(columns=["date", "sentiment"])

    rows: List[Dict[str, Any]] = []

    for path in folder.glob("*.json"):
        try:
            with path.open("r") as f:
                obj = json.load(f)
        except Exception:
            continue

        date_str = obj.get("date") or path.stem
        try:
            d = datetime.fromisoformat(date_str).date()
        except Exception:
            continue

        if d < start_date or d > end_date:
            continue

        sentiment = obj.get("score_mean")
        if sentiment is None:
            sentiment = obj.get("sentiment")
        rows.append({"date": d, "sentiment": sentiment})

    if not rows:
        return pd.DataFrame(columns=["date", "sentiment"])

    df = pd.DataFrame(rows)
    df = df.sort_values("date").reset_index(drop=True)
    return df


def compute_cap_weighted_sentiment(
    universe: pd.DataFrame,
    sentiment_root: Path,
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

        df = load_ticker_daily_sentiment_from_files(
            sentiment_root=sentiment_root,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )
        if df.empty:
            continue

        df["symbol"] = symbol
        df["base_weight"] = base_weight
        frames.append(df)

    if not frames:
        print("[SPX] No per-ticker sentiment found in sentiment_root")
        return pd.DataFrame(columns=["date", "sentiment_cap_weighted"])

    panel = pd.concat(frames, ignore_index=True)

    panel["date"] = pd.to_datetime(panel["date"]).dt.date

    panel = panel[(panel["date"] >= start_date) & (panel["date"] <= end_date)]

    if panel.empty:
        print("[SPX] Panel empty after date filter")
        return pd.DataFrame(columns=["date", "sentiment_cap_weighted"])

    panel["sentiment"] = pd.to_numeric(panel["sentiment"], errors="coerce")
    panel = panel.dropna(subset=["sentiment"])

    if panel.empty:
        print("[SPX] All sentiment values were NaN after conversion")
        return pd.DataFrame(columns=["date", "sentiment_cap_weighted"])

    tmp = panel.assign(weighted=panel["base_weight"] * panel["sentiment"])
    grouped = (
        tmp.groupby("date", as_index=False)
        .agg(total_weight=("base_weight", "sum"),
             weighted_sum=("weighted", "sum"))
    )
    grouped["sentiment_cap_weighted"] = grouped["weighted_sum"] / grouped["total_weight"]

    out = grouped[["date", "sentiment_cap_weighted"]].copy()
    out["date"] = out["date"].astype(str)

    print(f"[SPX] Built cap-weighted sentiment for {len(out)} days")
    return out


# ----------------------------------------------------------------------
# Payload builder
# ----------------------------------------------------------------------

def build_sp500_index_payload(
    prices: pd.DataFrame,
    sentiment: pd.DataFrame,
    news: pd.DataFrame,
    news_meta: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Merge price + sentiment + news into a single JSON payload.
    """
    prices = _make_flat(prices)
    sentiment = _make_flat(sentiment)

    if not sentiment.empty and "date" not in sentiment.columns:
        raise ValueError("[SPX] Sentiment DataFrame has no 'date' column")

    daily = prices.merge(sentiment, on="date", how="left")
    daily = daily.sort_values("date").reset_index(drop=True)

    payload: Dict[str, Any] = {
        "symbol": INDEX_SYMBOL,
        "name": INDEX_NAME,
        "price_symbol_candidates": INDEX_PRICE_SYMBOL_CANDIDATES,
        "news_symbol_candidates": INDEX_NEWS_SYMBOL_CANDIDATES,
        "daily": daily.to_dict(orient="records"),
        "news": news.sort_values("date").to_dict(orient="records") if not news.empty else [],
        # debug/meta (safe to keep; small)
        **(news_meta or {}),
    }
    return payload


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------

def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build S&P 500 index JSON (price + news from yfinance, "
            "cap-weighted sentiment from data/[ticker]/sentiment/YYYY-MM-DD.json)."
        )
    )
    parser.add_argument(
        "--universe",
        type=str,
        required=True,
        help="Path to universe CSV (e.g. data/sp500.csv). Needs a ticker/symbol column.",
    )
    parser.add_argument(
        "--sentiment-root",
        type=str,
        required=True,
        help="Root dir where per-ticker sentiment folders live (e.g. data).",
    )
    parser.add_argument(
        "--out",
        type=str,
        required=True,
        help="Output directory for sp500_index.json (e.g. apps/web/public/data).",
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
    parser.add_argument(
        "--news-retries",
        type=int,
        default=3,
        help="Retry count for yfinance news fetch (CI robustness).",
    )

    args = parser.parse_args(argv)

    end_str = args.end or _default_end_today_utc()
    start_str = args.start or _default_start_one_year(end_str)

    print(f"[SPX] Date range: {start_str} → {end_str}")

    universe = load_universe_with_weights(Path(args.universe))
    sentiment_root = Path(args.sentiment_root)

    prices = download_spx_prices(start_str, end_str)
    sentiment = compute_cap_weighted_sentiment(universe, sentiment_root, start_str, end_str)
    news, news_meta = download_spx_news(
        start_str,
        end_str,
        max_items=args.max_news,
        retries=args.news_retries,
    )

    payload = build_sp500_index_payload(prices, sentiment, news, news_meta)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "sp500_index.json"
    with out_path.open("w") as f:
        json.dump(payload, f, indent=2)

    print(f"[SPX] Wrote {out_path}")


if __name__ == "__main__":
    main()
