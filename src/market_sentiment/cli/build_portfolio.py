from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# -----------------------------
# IO helpers
# -----------------------------
def _coalesce(d: Dict[str, Any], keys: List[str]) -> Optional[Any]:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _read_json_any(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _iter_ticker_dirs(root: str) -> List[str]:
    if not os.path.isdir(root):
        return []
    out = []
    for name in sorted(os.listdir(root)):
        p = os.path.join(root, name)
        if os.path.isdir(p):
            out.append(name)
    return out


def load_price_daily(data_root: str, ticker: str) -> pd.DataFrame:
    """
    Expect: {data_root}/{ticker}/price/daily.json
    JSON list like [{"date":"YYYY-MM-DD","close":...}, ...]
    """
    candidates = [
        os.path.join(data_root, ticker, "price", "daily.json"),
        os.path.join(data_root, ticker, "price", "prices.json"),
    ]
    fpath = next((p for p in candidates if os.path.exists(p)), None)
    if not fpath:
        return pd.DataFrame(columns=["ticker", "date", "close", "ret"])

    obj = _read_json_any(fpath)
    df = pd.DataFrame(obj)
    if df.empty:
        return pd.DataFrame(columns=["ticker", "date", "close", "ret"])

    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    close_col = None
    for cand in ["close", "adjClose", "Adj Close", "adj_close", "Close", "price"]:
        if cand in df.columns:
            close_col = cand
            break
    if close_col is None:
        raise ValueError(f"[{ticker}] price file has no close column: {fpath}")

    df = df[["date", close_col]].rename(columns={close_col: "close"}).dropna()
    df = df.sort_values("date")
    df["ticker"] = ticker
    df["ret"] = df["close"].pct_change()
    return df[["ticker", "date", "close", "ret"]]


def load_sentiment_daily(data_root: str, ticker: str) -> pd.DataFrame:
    """
    Expect: {data_root}/{ticker}/sentiment/*.json
    Each file: either dict or list[dict], should include date + score_mean-ish field.
    Aggregates to one row per (ticker, date): mean(score_mean).
    """
    sdir = os.path.join(data_root, ticker, "sentiment")
    if not os.path.isdir(sdir):
        return pd.DataFrame(columns=["ticker", "date", "score_mean"])

    rows: List[Dict[str, Any]] = []
    for fn in os.listdir(sdir):
        if not fn.endswith(".json"):
            continue
        path = os.path.join(sdir, fn)
        try:
            obj = _read_json_any(path)
        except Exception:
            continue

        if isinstance(obj, list):
            for it in obj:
                if isinstance(it, dict):
                    it = dict(it)
                    it.setdefault("ticker", ticker)
                    rows.append(it)
        elif isinstance(obj, dict):
            it = dict(obj)
            it.setdefault("ticker", ticker)
            rows.append(it)

    if not rows:
        return pd.DataFrame(columns=["ticker", "date", "score_mean"])

    df = pd.DataFrame(rows)
    if "date" not in df.columns:
        return pd.DataFrame(columns=["ticker", "date", "score_mean"])

    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    if "score_mean" not in df.columns:
        df["score_mean"] = df.apply(
            lambda r: _coalesce(
                r.to_dict(),
                [
                    "score_mean",
                    "scoreMean",
                    "sentiment",
                    "score",
                    "mean_score",
                    "meanSentiment",
                ],
            ),
            axis=1,
        )

    df["score_mean"] = pd.to_numeric(df["score_mean"], errors="coerce")
    df = df.dropna(subset=["date", "score_mean"])
    out = (
        df.groupby(["ticker", "date"], as_index=False)["score_mean"]
        .mean()
        .sort_values(["date"])
    )
    return out


def build_panel(data_root: str) -> pd.DataFrame:
    """
    Returns panel: columns [ticker, date, score_mean, ret]
    """
    dfs: List[pd.DataFrame] = []
    for t in _iter_ticker_dirs(data_root):
        px = load_price_daily(data_root, t)
        se = load_sentiment_daily(data_root, t)
        if px.empty or se.empty:
            continue
        m = pd.merge(se, px[["ticker", "date", "ret"]], on=["ticker", "date"], how="inner")
        m = m.dropna(subset=["score_mean", "ret"])
        if not m.empty:
            dfs.append(m)

    if not dfs:
        return pd.DataFrame(columns=["ticker", "date", "score_mean", "ret"])

    out = pd.concat(dfs, ignore_index=True)
    out = out.sort_values(["date", "ticker"])
    return out


# -----------------------------
# Strategy (from notebook)
# -----------------------------
def _first_trading_day_each_week(dates: pd.DatetimeIndex) -> List[pd.Timestamp]:
    # "calendar week" = pandas Period('W') groups (Mon-Sun)
    wk = dates.to_period("W")
    first = pd.Series(dates, index=dates).groupby(wk).min()
    return list(pd.to_datetime(first.values))


def backtest_weekly_lagged_longshort(
    panel: pd.DataFrame,
    k: int,
    long_short: bool,
    gross: float = 1.0,
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Weekly rebalance on first trading day each calendar week.
    Signal = lag-1 sentiment score_mean.
    Portfolio:
      - long-only: top-K equal weights summing to +gross
      - long/short: top-K and bottom-K, each side uses gross/2 notional (sum longs=+gross/2, sum shorts=-gross/2)
    Returns:
      series df: [date, ret, equity]
      holdings: list of rebalance snapshots
    """
    if panel.empty:
        return pd.DataFrame(columns=["date", "ret", "equity"]), []

    df = panel.copy().sort_values(["date", "ticker"])
    df["signal"] = df.groupby("ticker")["score_mean"].shift(1)
    df = df.dropna(subset=["signal", "ret"])

    ret_w = df.pivot(index="date", columns="ticker", values="ret").sort_index()
    sig_w = df.pivot(index="date", columns="ticker", values="signal").reindex(ret_w.index)

    rebal_dates = _first_trading_day_each_week(ret_w.index)

    weights = pd.DataFrame(0.0, index=ret_w.index, columns=ret_w.columns)
    holdings: List[Dict[str, Any]] = []

    for i, start in enumerate(rebal_dates):
        start = pd.Timestamp(start)
        end = pd.Timestamp(rebal_dates[i + 1]) if (i + 1) < len(rebal_dates) else (ret_w.index.max() + pd.Timedelta(days=1))

        if start not in sig_w.index:
            continue

        sig = sig_w.loc[start].dropna()
        if sig.empty:
            continue

        sig_sorted = sig.sort_values(ascending=False)
        top = sig_sorted.head(k)

        w = pd.Series(0.0, index=ret_w.columns, dtype=float)

        if long_short:
            bot = sig_sorted.tail(k)
            kL = len(top)
            kS = len(bot)
            if kL == 0 or kS == 0:
                continue
            long_w = (gross / 2.0) / kL
            short_w = -(gross / 2.0) / kS
            w[top.index] = long_w
            w[bot.index] = short_w

            holdings.append(
                {
                    "date": start.strftime("%Y-%m-%d"),
                    "long": [{"ticker": t, "weight": float(long_w), "signal": float(sig[t])} for t in top.index],
                    "short": [{"ticker": t, "weight": float(short_w), "signal": float(sig[t])} for t in bot.index],
                }
            )
        else:
            kL = len(top)
            if kL == 0:
                continue
            long_w = gross / kL
            w[top.index] = long_w
            holdings.append(
                {
                    "date": start.strftime("%Y-%m-%d"),
                    "long": [{"ticker": t, "weight": float(long_w), "signal": float(sig[t])} for t in top.index],
                }
            )

        mask = (ret_w.index >= start) & (ret_w.index < end)
        weights.loc[mask] = w.values

    port_ret = (weights.fillna(0.0) * ret_w.fillna(0.0)).sum(axis=1)
    series = pd.DataFrame({"date": port_ret.index, "ret": port_ret.values})
    series["equity"] = (1.0 + series["ret"]).cumprod()
    series["date"] = series["date"].dt.strftime("%Y-%m-%d")
    return series, holdings


def compute_metrics(daily_ret: pd.Series, rf_annual: float = 0.04) -> Dict[str, Any]:
    r = daily_ret.dropna()
    if r.empty:
        return {}

    T = int(len(r))
    cum = float((1 + r).prod() - 1)

    # trading days per year
    ann = float((1 + cum) ** (252 / T) - 1) if T > 0 else 0.0
    vol = float(r.std(ddof=1) * np.sqrt(252)) if T > 1 else 0.0
    rf_daily = (1 + rf_annual) ** (1 / 252) - 1
    sharpe = float(((r.mean() - rf_daily) / r.std(ddof=1)) * np.sqrt(252)) if T > 1 and r.std(ddof=1) > 0 else 0.0

    equity = (1 + r).cumprod()
    peak = equity.cummax()
    dd = equity / peak - 1
    max_dd = float(dd.min())

    hit = float((r > 0).mean())

    return {
        "cumulativeReturn": cum,
        "annualizedReturn": ann,
        "annualizedVol": vol,
        "sharpe": sharpe,
        "maxDrawdown": max_dd,
        "hitRate": hit,
        "numDays": T,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="apps/web/public/data", help="Root folder with {TICKER}/price and {TICKER}/sentiment")
    ap.add_argument("--out", default="apps/web/public/portfolio.json", help="Output JSON path (served from Next public/)")
    ap.add_argument("--k", type=int, default=10, help="Top-K (and bottom-K if long/short)")
    ap.add_argument("--long-short", action="store_true", help="Enable long/short (default: long-only)")
    ap.add_argument("--gross", type=float, default=1.0, help="Gross exposure; if long/short each side uses gross/2")
    ap.add_argument("--rf", type=float, default=0.04, help="Annual risk-free for Sharpe")
    args = ap.parse_args()

    panel = build_panel(args.data)
    series, holdings = backtest_weekly_lagged_longshort(panel, k=args.k, long_short=args.long_short, gross=args.gross)

    metrics = compute_metrics(pd.Series(series["ret"].values), rf_annual=args.rf)

    meta = {
        "strategy": "weekly_rebalance_lag1_sentiment",
        "k": args.k,
        "longShort": bool(args.long_short),
        "gross": float(args.gross),
        "dataRoot": args.data,
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }

    payload = {
        "meta": meta,
        "series": series.to_dict(orient="records"),
        "metrics": metrics,
        "holdings": holdings,
    }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    print(f"Wrote {args.out} | rows={len(series)} | rebalances={len(holdings)}")


if __name__ == "__main__":
    main()
