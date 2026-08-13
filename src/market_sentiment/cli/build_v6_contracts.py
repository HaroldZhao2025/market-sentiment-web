from __future__ import annotations
import argparse, json, os
from pathlib import Path
from market_sentiment.v6_events import build_event_store_v3

def load(path: Path):
    try: return json.loads(path.read_text(encoding="utf-8"))
    except Exception: return {}

def save(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp=path.with_suffix(path.suffix+".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    os.replace(tmp,path)

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--root",default="apps/web/public/data/v5"); ap.add_argument("--window-days",type=int,default=2); a=ap.parse_args()
    root=Path(a.root); store=build_event_store_v3(load(root/"events.json"), max(0,a.window_days)); save(root/"events_v3.json",store)
    print(f"PHASE6 EVENTS OK | instances={len(store.get('event_instances',[]))}")

if __name__=="__main__": main()
