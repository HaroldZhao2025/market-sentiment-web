from __future__ import annotations
import argparse
from pathlib import Path
from market_sentiment.universe import fetch_sp500

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    df = fetch_sp500()
    a.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(a.out, index=False)
    print("Saved", a.out)

if __name__ == "__main__":
    main()
