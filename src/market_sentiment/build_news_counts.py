# src/market_sentiment/build_news_counts.py
# Aggregate news counts for each ticker over [START, END] (UTC) by summing

from __future__ import annotations

import os
import json
import glob
from pathlib import Path
import pandas as pd


def _read_universe(csv_path: str) -> list[str]:
    df = pd.read_csv(csv_path)
    col = [c for c in df.columns if c.lower() == "ticker"]
    if not col:
        return []
    return (
        df[col[0]]
        .dropna()
        .astype(str)
        .str.upper()
        .str.replace(".", "-", regex=False)
        .str.strip()
        .unique()
        .tolist()
    )


def build_counts_for_ticker(ticker: str, start: str, end: str) -> dict:
    base = Path(f"data/{ticker}")
    counts = {"ticker": ticker, "start": start, "end": end,
              "n_finnhub": 0, "n_yfinance": 0, "n_total": 0}

    sdir = base / "sentiment"
    if not sdir.exists():
        return counts

    s = pd.to_datetime(start, utc=True).date()
    e = pd.to_datetime(end, utc=True).date()

    for p in sorted(glob.glob(str(sdir / "*.json"))):
        try:
            obj = json.load(open(p, encoding="utf-8")) or {}
        except Exception:
            obj = {}
        day = obj.get("date")
        if not day:
            continue
        try:
            dd = pd.to_datetime(day, utc=True).date()
        except Exception:
            continue
        if dd < s or dd > e:
            continue

        nf = int(obj.get("n_finnhub", 0) or 0)
        ny = int(obj.get("n_yfinance", 0) or 0)
        nt = int(obj.get("n_total", nf + ny) or (nf + ny))

        counts["n_finnhub"] += nf
        counts["n_yfinance"] += ny
        counts["n_total"] += nt

    # write
    outp = base / "news" / "counts.json"
    outp.parent.mkdir(parents=True, exist_ok=True)
    json.dump(counts, open(outp, "w", encoding="utf-8"), indent=2)
    return counts


def main() -> None:
    START = os.environ["START"]
    END = os.environ["END"]
    TICKER_CSV = os.environ["TICKER_CSV"]

    for t in _read_universe(TICKER_CSV):
        c = build_counts_for_ticker(t, START, END)
        print(f"[NEWS-COUNTS] {t}: finnhub={c['n_finnhub']} "
              f"yfinance={c['n_yfinance']} total={c['n_total']}")


if __name__ == "__main__":
    main()
