# build_news_counts.py
from __future__ import annotations
import json, os, glob, pathlib, collections

ROOT = pathlib.Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data"                     # where data/<TICKER>/sentiment/*.json live
PUB_DIR = ROOT / "apps" / "web" / "public" / "data" / "ticker"

def load_daily_sentiment(ticker: str):
    """Return dict[date] = n_finnhub + n_yfinance from data/<TICKER>/sentiment/*.json."""
    base = RAW_DIR / ticker / "sentiment"
    out = {}
    if not base.exists():
        return out
    for p in sorted(glob.glob(str(base / "*.json"))):
        try:
            j = json.load(open(p, "r", encoding="utf-8"))
            date = j.get("date")
            nf = int(j.get("n_finnhub", 0) or 0)
            ny = int(j.get("n_yfinance", 0) or 0)
            if date:
                out[date] = int(nf + ny)
        except Exception:
            # ignore broken files
            continue
    return out

def inject_into_ticker_json(ticker: str, counts: dict[str, int]) -> bool:
    """Add news_count_daily to apps/web/public/data/ticker/<TICKER>.json."""
    tkr_path = PUB_DIR / f"{ticker}.json"
    if not tkr_path.exists():
        return False
    try:
        doc = json.load(open(tkr_path, "r", encoding="utf-8"))
    except Exception:
        return False

    # serialize as sorted array of {date, count} pairs; stable for git diffs
    items = [{"date": d, "count": counts[d]} for d in sorted(counts)]
    doc["news_count_daily"] = items

    tmp = tkr_path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, separators=(",", ":"), indent=None)
    os.replace(tmp, tkr_path)
    return True

def main():
    if not PUB_DIR.exists():
        print(f"[skip] {PUB_DIR} not found — build_json probably didn’t run yet.")
        return
    changed = 0
    for p in sorted(glob.glob(str(PUB_DIR / "*.json"))):
        ticker = pathlib.Path(p).stem.upper()
        counts = load_daily_sentiment(ticker)
        if not counts:
            continue
        if inject_into_ticker_json(ticker, counts):
            changed += 1
            print(f"[ok] injected news_count_daily into {ticker}.json  ({len(counts)} days)")
    if changed == 0:
        print("[info] no ticker JSONs updated")
    else:
        print(f"[done] updated {changed} ticker files")

if __name__ == "__main__":
    main()
