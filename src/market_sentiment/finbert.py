from __future__ import annotations
import re
from typing import List, Dict
import numpy as np
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

_SENT_SPLIT = re.compile(r"(?<=[\.\!\?])\s+")

def _split_sentences(text: str, max_sentences: int = 120) -> List[str]:
    if not text:
        return []
    sents = [s.strip() for s in _SENT_SPLIT.split(text) if s.strip()]
    return sents[:max_sentences]

class FinBERT:
    """
    ProsusAI/finbert inference (CPU/GPU) with batch scoring.
    Returns dicts: positive, negative, neutral, confidence.
    """
    def __init__(self, model_name: str = "ProsusAI/finbert", device: str | None = None):
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=True)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.model.to(self.device).eval()

        id2label = getattr(self.model.config, "id2label", None)
        if id2label:
            self.order = [id2label[i].lower() for i in range(len(id2label))]
        else:
            # finbert default order (neg, neu, pos)
            self.order = ["negative", "neutral", "positive"]

    def _probs_to_dict(self, row: np.ndarray) -> Dict[str, float]:
        mapping = {self.order[i]: float(row[i]) for i in range(len(self.order))}
        pos = mapping.get("positive", row[-1])
        neg = mapping.get("negative", row[0])
        neu = mapping.get("neutral", row[1] if row.size > 1 else 0.0)
        conf = float(np.max(row))
        return {"positive": pos, "negative": neg, "neutral": neu, "confidence": conf}

    @torch.inference_mode()
    def score_batch(self, texts: List[str], batch_size: int = 32, max_length: int = 128) -> List[Dict[str, float]]:
        if not texts:
            return []
        out: List[Dict[str, float]] = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            enc = self.tokenizer(
                batch, return_tensors="pt", padding=True, truncation=True, max_length=max_length
            ).to(self.device)
            logits = self.model(**enc).logits
            probs = torch.softmax(logits, dim=-1).cpu().numpy()
            out.extend(self._probs_to_dict(r) for r in probs)
        return out

    @torch.inference_mode()
    def score_long_text(self, text: str, batch_size: int = 16, max_length: int = 128) -> Dict[str, float]:
        sents = _split_sentences(text, max_sentences=120)
        if not sents:
            return {"positive": 0.0, "negative": 0.0, "neutral": 1.0, "confidence": 0.0}
        rows = self.score_batch(sents, batch_size=batch_size, max_length=max_length)
        pos = float(np.mean([r["positive"] for r in rows]))
        neg = float(np.mean([r["negative"] for r in rows]))
        neu = float(np.mean([r["neutral"] for r in rows]))
        conf = float(np.mean([r["confidence"] for r in rows]))
        return {"positive": pos, "negative": neg, "neutral": neu, "confidence": conf}
