from __future__ import annotations
import numpy as np
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification


class FinBERT:
    """
    ProsusAI/finbert inference with batch scoring.
    Returns a list of dicts with keys: positive, negative, neutral, confidence.
    """
    def __init__(self, model_name: str = "ProsusAI/finbert", device: str | None = None):
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=True)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.model.to(self.device).eval()

        # robust label mapping
        id2label = getattr(self.model.config, "id2label", None)
        if id2label:
            self.order = [id2label[i].lower() for i in range(len(id2label))]
        else:
            # ProsusAI/finbert default (neg, neu, pos)
            self.order = ["negative", "neutral", "positive"]

    def _row_to_dict(self, row: np.ndarray) -> dict:
        mapping = {self.order[i]: float(row[i]) for i in range(len(self.order))}
        pos = mapping.get("positive", row[-1])
        neg = mapping.get("negative", row[0])
        neu = mapping.get("neutral", row[1] if row.size > 1 else 0.0)
        conf = float(np.max(row))
        return {"positive": pos, "negative": neg, "neutral": neu, "confidence": conf}

    @torch.inference_mode()
    def score_batch(self, texts: list[str], batch_size: int = 32, max_length: int = 128) -> list[dict]:
        if not texts:
            return []
        out: list[dict] = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            enc = self.tokenizer(
                batch, return_tensors="pt", padding=True, truncation=True, max_length=max_length
            ).to(self.device)
            logits = self.model(**enc).logits
            probs = torch.softmax(logits, dim=-1).cpu().numpy()
            out.extend(self._row_to_dict(r) for r in probs)
        return out
