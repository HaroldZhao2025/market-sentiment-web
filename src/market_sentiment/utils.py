from __future__ import annotations
from pathlib import Path
import json
import pandas as pd

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def dump_json(obj, path: Path) -> None:
    ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

def load_sp500_csv(path: Path) -> list[str]:
    df = pd.read_csv(path)
    col = None
    for c in df.columns:
        if c.lower() in ("symbol","ticker","tickers"):
            col = c; break
    if col is None:
        raise ValueError("Universe CSV must contain a 'symbol' or 'ticker' column")
    tickers = df[col].astype(str).str.upper().str.strip().tolist()
    # Keep first 505 only if duplicates, etc.
    return tickers
