from __future__ import annotations
from pathlib import Path
import json, pandas as pd

def ensure_dir(p: Path): p.mkdir(parents=True, exist_ok=True)

def load_sp500_csv(p: Path) -> list[str]:
    df = pd.read_csv(p)
    for c in df.columns:
        if c.lower() in {'ticker','symbol','tickers','symbols'}:
            col = c; break
    else:
        raise ValueError('CSV must contain ticker/symbol column.')
    return sorted(set(df[col].dropna().astype(str).str.upper().str.strip()))

def dump_json(obj, path: Path) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, indent=2, default=str), encoding='utf-8')
