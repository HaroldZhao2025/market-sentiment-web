from __future__ import annotations

import argparse
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
import yfinance as yf

# Optional fallback
try:
    import requests  # type: ignore
except Exception:
    requests = None  # pragma: no cover

# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------

# For prices we try these in order
INDEX_PRICE_SYMBOL_CANDIDATES = ["^GSPC", "^SPX", "SPY"]

# For news we try ^SPX first (your manual pattern), then fallbacks
INDEX_NEWS_SYMBOL_CANDIDATES = ["^SPX", "^GSPC", "SPY"]

INDEX_SYMBOL = "SPX"        # symbol exposed on the site
INDEX_NAME = "S&P 500 Index"

# yfinance tabs to try (your local test suggests "all" is critical)
YF_NEWS_TABS = ("all", "news")

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
# Robust timestamp parsing for yfinance news
# ----------------------------------------------------------------------

def _ts_to_dt_utc(ts: Any) -> Optional[datetime]:
    """
    Try to convert various timestamp shapes to UTC datetime.
    Handles seconds vs milliseconds.
    """
    if ts is None:
        return None

    # numeric epoch
    if isinstance(ts, (int, float)):
        # heuristic: ms epoch is huge
        val = float(ts)
        if val > 10_000_000_000:  # ~2286-11-20 in seconds; above this is likely ms
            val = val / 1000.0
        try:
            return datetime.fromtimestamp(val, tz=timezone.utc)
        except Exception:
            return None

    # string datetime
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    return None


def _extract_news_datetime(item: Dict[str, Any]) -> Optional[datetime]:
    """
    yfinance news item keys vary by version / endpoint.
    Try a few common keys.
    """
    for k in ("providerPublishTime", "providerPublishTimeMs", "publishTime", "published_at", "pubDate"):
        dt = _ts_to_dt_utc(item.get(k))
        if dt is not None:
            return dt
    return None


def _normalize_yf_news(items: List[Dict[str, Any]], raw_symbol: str) -> List[Dict[str, Any]]:
    """
    Normalize yfinance items to a stable schema.
    """
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()

    for it in items or []:
        link = it.get("link") or it.get("url") or ""
        title = (it.get("title") or "").strip()
        publisher = (it.get("publisher") or "").strip()

        uid = str(it.get("uuid") or it.get("id") or link or f"{publisher}|{title}")
        if not uid or uid in seen:
            continue
        seen.add(uid)

        dt = _extract_news_datetime(it)
        if dt is None:
            # If no timestamp, skip (keeps date filtering logic reliable)
            continue

        out.append(
            {
                "id": uid,
                "symbol": INDEX_SYMBOL,        # canonical symbol for the site
                "raw_symbol": raw_symbol,      # which Yahoo symbol produced this row
                "date": dt.date().isoformat(),
                "published_at": dt.isoformat(),
                "title": title,
                "publisher": publisher,
                "link": link,
                "source": "yfinance",
                "related_tickers": it.get("relatedTickers") or [],
            }
        )

    # newest first is usually best for UI
    out.sort(key=lambda x: x.get("published_at") or "", reverse=True)
    return out


# ----------------------------------------------------------------------
# SPX price & news
# ----------------------------------------------------------------------

def download_spx_prices(start: str, end: str) -> Tuple[pd.DataFrame, str]:
    """
    Try a few symbols for S&P 500 price: ^GSPC, ^SPX, SPY.
    Returns (df, source_symbol).
    df columns: ['date', 'close', f'close_{source_symbol}']
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

        # Add compatibility column: close_^GSPC / close_^SPX / close_SPY
        compat_col = f"close_{sym}"
        df[compat_col] = df["close"]

        df = df.sort_values("date").reset_index(drop=True)
        print(f"[SPX] Using {sym} for index prices, {len(df)} rows")
        return df, sym

    raise RuntimeError(
        f"[SPX] Could not download index prices for any of {INDEX_PRICE_SYMBOL_CANDIDATES}. "
        f"Last error: {last_err!r}"
    )


def _yf_fetch_news_once(sym: str, max_items: int) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    One-shot fetch from yfinance for a given symbol, trying tabs.
    Returns (raw_items, tab_used).
    """
    t = yf.Ticker(sym)

    for tab in YF_NEWS_TABS:
        try:
            raw = t.get_news(count=max_items, tab=tab) or []
            if raw:
                return raw, tab
        except TypeError:
            # Some versions may not support tab=
            try:
                raw = t.get_news(count=max_items) or []
                if raw:
                    return raw, None
            except Exception:
                pass
        except AttributeError:
            # Very old version: no get_news at all
            try:
                raw = t.news or []
                if raw:
                    return raw, None
            except Exception:
                pass
        except Exception:
            # handled by retry wrapper
            raise

    # last resort
    try:
        raw = getattr(t, "news", None) or []
        if raw:
            return raw, None
    except Exception:
        pass

    return [], None


def _yf_fetch_news_with_retries(sym: str, max_items: int, retries: int = 4) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Retry wrapper for CI robustness.
    """
    last_err: Optional[Exception] = None
    for attempt in range(retries):
        try:
            raw, tab_used = _yf_fetch_news_once(sym, max_items=max_items)
            return raw, tab_used
        except Exception as e:
            last_err = e
            sleep_s = (2 ** attempt) + random.random()
            print(f"[SPX] yfinance news error for {sym} (attempt {attempt+1}/{retries}): {e!r}; sleep {sleep_s:.2f}s")
            time.sleep(sleep_s)

    print(f"[SPX] yfinance news failed for {sym}. last_err={last_err!r}")
    return [], None


def _filter_news_by_range(news: List[Dict[str, Any]], start: str, end: str) -> List[Dict[str, Any]]:
    """
    Keep items with published_at in [start, end+1d).
    """
    start_dt = _parse_date(start)
    end_dt = _parse_date(end) + timedelta(days=1)

    out = []
    for it in news:
        dt = None
        # prefer published_at
        dt = _ts_to_dt_utc(it.get("published_at")) or _ts_to_dt_utc(it.get("providerPublishTime"))
        if dt is None:
            continue
        if start_dt <= dt < end_dt:
            out.append(it)
    return out


def _newsapi_fallback(start: str, end: str, max_items: int) -> pd.DataFrame:
    """
    Optional fallback using NewsAPI if NEWS_API_KEY is set.
    Query is broad for SP500.
    """
    api_key = os.getenv("NEWS_API_KEY", "").strip()
    if not api_key:
        return pd.DataFrame(columns=["date", "title", "publisher", "link", "published_at", "source", "symbol", "raw_symbol", "id"])

    if requests is None:
        print("[SPX] requests not available; cannot use NewsAPI fallback")
        return pd.DataFrame(columns=["date", "title", "publisher", "link", "published_at", "source", "symbol", "raw_symbol", "id"])

    # SP500 query (tweak if you want)
    q = '"S&P 500" OR SPX OR SP500 OR "S&P500" OR "S&P 500 Index"'
    url = "https://newsapi.org/v2/everything"

    collected: List[Dict[str, Any]] = []
    page = 1
    page_size = min(100, max_items)

    while len(collected) < max_items and page <= 10:  # safeguard
        params = {
            "q": q,
            "from": start,
            "to": end,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": page_size,
            "page": page,
            "apiKey": api_key,
        }
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code != 200:
                print(f"[SPX] NewsAPI status={resp.status_code}: {resp.text[:200]}")
                break
            data = resp.json()
        except Exception as e:
            print(f"[SPX] NewsAPI error: {e!r}")
            break

        articles = data.get("articles") or []
        if not articles:
            break

        for a in articles:
            published_at = a.get("publishedAt") or ""
            dt = _ts_to_dt_utc(published_at)
            if dt is None:
                continue
            link = a.get("url") or ""
            title = (a.get("title") or "").strip()
            publisher = ((a.get("source") or {}).get("name") or "").strip()
            uid = link or f"{publisher}|{title}|{published_at}"
            collected.append(
                {
                    "id": uid,
                    "symbol": INDEX_SYMBOL,
                    "raw_symbol": "NewsAPI",
                    "date": dt.date().isoformat(),
                    "published_at": dt.isoformat(),
                    "title": title,
                    "publisher": publisher,
                    "link": link,
                    "source": "newsapi",
                }
            )
            if len(collected) >= max_items:
                break

        page += 1
        time.sleep(0.2)  # be gentle

    if not collected:
        return pd.DataFrame(columns=["date", "title", "publisher", "link", "published_at", "source", "symbol", "raw_symbol", "id"])

    df = pd.DataFrame(collected)
    df = df.sort_values("published_at", ascending=False).reset_index(drop=True)
    print(f"[SPX] Using NewsAPI fallback, {len(df)} articles in range")
    return df


def download_spx_news(
    start: str,
    end: str,
    max_items: int = 500,
    news_source: str = "auto",  # "auto" | "yfinance" | "newsapi"
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Try multiple yfinance symbols for S&P 500 news, using:
        t.get_news(count=max_items, tab="all")
    with retries & robust parsing.

    If still empty and NEWS_API_KEY is provided, fallback to NewsAPI (when news_source="auto"/"newsapi").

    Returns (news_df, meta)
    news_df columns: ['date','title','publisher','link', ... optional fields]
    meta includes: news_source_symbol, news_source_tab, news_source_used
    """
    meta = {
        "news_source_symbol": None,
        "news_source_tab": None,
        "news_source_used": None,
    }

    if news_source not in ("auto", "yfinance", "newsapi"):
        raise ValueError(f"[SPX] Invalid --news-source: {news_source}")

    # ----------------------------
    # 1) yfinance path
    # ----------------------------
    if news_source in ("auto", "yfinance"):
        for sym in INDEX_NEWS_SYMBOL_CANDIDATES:
            print(f"[SPX] Fetching news for {sym} (max_items={max_items}) ...")
            raw, tab_used = _yf_fetch_news_with_retries(sym, max_items=max_items, retries=4)

            if not raw:
                print(f"[SPX] No raw news for {sym}, trying next candidate")
                continue

            normalized = _normalize_yf_news(raw, raw_symbol=sym)

            # Filter by range; if filter wipes everything, keep latest few as fallback
            in_range = [x for x in normalized if _parse_date(start) <= _parse_date(x["date"]) <= _parse_date(end)]
            if not in_range:
                # range filter too strict or timestamps missing — keep latest 50
                in_range = normalized[: min(50, len(normalized))]
                print(f"[SPX] No news in range for {sym}; keeping latest {len(in_range)} items as fallback")

            if not in_range:
                print(f"[SPX] Normalized empty for {sym}, trying next candidate")
                continue

            df = pd.DataFrame(in_range)

            # Ensure core columns exist for payload
            for col in ["date", "title", "publisher", "link"]:
                if col not in df.columns:
                    df[col] = ""

            df = df.sort_values(["published_at", "publisher", "title"], ascending=[False, True, True]).reset_index(drop=True)

            meta["news_source_symbol"] = sym
            meta["news_source_tab"] = tab_used
            meta["news_source_used"] = "yfinance"
            print(f"[SPX] Using {sym} for news, {len(df)} articles")
            return df, meta

        print("[SPX] yfinance: no index news found for any symbol candidate")

        if news_source == "yfinance":
            # user forced yfinance; stop here
            return pd.DataFrame(columns=["date", "title", "publisher", "link"]), meta

    # ----------------------------
    # 2) NewsAPI fallback
    # ----------------------------
    if news_source in ("auto", "newsapi"):
        df = _newsapi_fallback(start=start, end=end, max_items=max_items)
        if not df.empty:
            meta["news_source_symbol"] = "NewsAPI"
            meta["news_source_tab"] = None
            meta["news_source_used"] = "newsapi"
            # keep core columns for payload compatibility
            for col in ["date", "title", "publisher", "link"]:
                if col not in df.columns:
                    df[col] = ""
            return df, meta

    return pd.DataFrame(columns=["date", "title", "publisher", "link"]), meta


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

            fi = getattr(t, "fast_info", None)
            if isinstance(fi, dict):
                mc = fi.get("market_cap")
            else:
                mc = getattr(fi, "market_cap", None)

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

    sym_col = _find_symbol_column(uni)
    uni = uni.rename(columns={sym_col: "symbol"})

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
        return pd.DataFrame(columns=["date", "sentiment_cap_weighted", "sentiment"])

    panel = pd.concat(frames, ignore_index=True)
    panel["date"] = pd.to_datetime(panel["date"]).dt.date
    panel = panel[(panel["date"] >= start_date) & (panel["date"] <= end_date)]

    if panel.empty:
        print("[SPX] Panel empty after date filter")
        return pd.DataFrame(columns=["date", "sentiment_cap_weighted", "sentiment"])

    panel["sentiment"] = pd.to_numeric(panel["sentiment"], errors="coerce")
    panel = panel.dropna(subset=["sentiment"])

    if panel.empty:
        print("[SPX] All sentiment values were NaN after conversion")
        return pd.DataFrame(columns=["date", "sentiment_cap_weighted", "sentiment"])

    tmp = panel.assign(weighted=panel["base_weight"] * panel["sentiment"])
    grouped = (
        tmp.groupby("date", as_index=False)
        .agg(
            total_weight=("base_weight", "sum"),
            weighted_sum=("weighted", "sum"),
        )
    )
    grouped["sentiment_cap_weighted"] = grouped["weighted_sum"] / grouped["total_weight"]

    out = grouped[["date", "sentiment_cap_weighted"]].copy()
    out["sentiment"] = out["sentiment_cap_weighted"]  # UI compatibility
    out["date"] = out["date"].astype(str)

    print(f"[SPX] Built cap-weighted sentiment for {len(out)} days")
    return out


# ----------------------------------------------------------------------
# Payload builder
# ----------------------------------------------------------------------

def build_sp500_index_payload(
    prices: pd.DataFrame,
    price_source_symbol: str,
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

        # Debug / transparency
        "price_source_symbol": price_source_symbol,
        "news_source_symbol": news_meta.get("news_source_symbol"),
        "news_source_tab": news_meta.get("news_source_tab"),
        "news_source_used": news_meta.get("news_source_used"),

        "daily": daily.to_dict(orient="records"),
        "news": news.to_dict(orient="records") if not news.empty else [],
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
        help="Max news items to pull for SPX.",
    )
    parser.add_argument(
        "--news-source",
        type=str,
        default="auto",
        help="auto|yfinance|newsapi (default: auto). If auto, fallback to NewsAPI when NEWS_API_KEY is set.",
    )

    args = parser.parse_args(argv)

    end_str = args.end or _default_end_today_utc()
    start_str = args.start or _default_start_one_year(end_str)

    print(f"[SPX] Date range: {start_str} → {end_str}")

    universe = load_universe_with_weights(Path(args.universe))
    sentiment_root = Path(args.sentiment_root)

    prices, price_source_symbol = download_spx_prices(start_str, end_str)
    sentiment = compute_cap_weighted_sentiment(universe, sentiment_root, start_str, end_str)

    news, news_meta = download_spx_news(
        start=start_str,
        end=end_str,
        max_items=args.max_news,
        news_source=args.news_source,
    )

    payload = build_sp500_index_payload(prices, price_source_symbol, sentiment, news, news_meta)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "sp500_index.json"
    with out_path.open("w") as f:
        json.dump(payload, f, indent=2)

    print(f"[SPX] Wrote {out_path}")


if __name__ == "__main__":
    main()
