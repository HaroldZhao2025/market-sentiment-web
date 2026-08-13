from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path
from typing import Any

from .v5_news import normalize_title


class ReusableNewsScorer:
    def __init__(self, cache_path: Path, batch_size: int = 32):
        self.cache_path = cache_path
        self.batch_size = batch_size
        self.cache: dict[str, float] = {}
        self.model = None
        if cache_path.exists():
            try:
                with gzip.open(cache_path, "rt", encoding="utf-8") as handle:
                    self.cache = {str(k): float(v) for k, v in json.load(handle).items()}
            except Exception:
                self.cache = {}

    def _key(self, title: object) -> str:
        return hashlib.sha256(normalize_title(title).encode("utf-8")).hexdigest()

    def _save(self) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.cache_path.with_suffix(self.cache_path.suffix + ".tmp")
        with gzip.open(tmp, "wt", encoding="utf-8", compresslevel=6) as handle:
            json.dump(self.cache, handle, ensure_ascii=False, separators=(",", ":"))
        tmp.replace(self.cache_path)

    def score(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        keys = [self._key(row.get("title")) for row in items]
        missing = [(key, str(row.get("title") or "")) for key, row in zip(keys, items) if key not in self.cache]
        if missing:
            if self.model is None:
                from .finbert import FinBERT
                self.model = FinBERT()
            scores = self.model.score([title for _, title in missing], batch_size=self.batch_size)
            for (key, _), score in zip(missing, scores):
                self.cache[key] = float(score)
            self._save()
        output = []
        for row, key in zip(items, keys):
            clean = dict(row)
            clean["s"] = round(self.cache[key], 6) if key in self.cache else None
            output.append(clean)
        return output
