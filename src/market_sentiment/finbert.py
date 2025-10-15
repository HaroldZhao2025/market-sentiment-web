# src/market_sentiment/finbert.py
from __future__ import annotations
from typing import List

import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification


class FinBERT:
    """
    Minimal wrapper around ProsusAI/finbert that returns a scalar score per text.

    S = P(positive) - P(negative)   in [-1, 1]
    """

    def __init__(self, model_name: str = "ProsusAI/finbert", max_length: int = 256):
        self.model_name = model_name
        self.max_length = max_length

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name, use_fast=True)
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
        self.model.to(self.device)
        self.model.eval()

        # label order: 0=negative, 1=neutral, 2=positive
        self.neg_id, self.pos_id = 0, 2

    @torch.no_grad()
    def score(self, texts: List[str], batch: int = 16) -> List[float]:
        if not texts:
            return []
        out: List[float] = []
        B = max(1, int(batch))
        for i in range(0, len(texts), B):
            chunk = texts[i:i + B]
            enc = self.tokenizer(
                chunk,
                padding=True,
                truncation=True,
                max_length=self.max_length,
                return_tensors="pt",
            )
            enc = {k: v.to(self.device) for k, v in enc.items()}
            logits = self.model(**enc).logits  # [B,3]
            probs = torch.softmax(logits, dim=-1)
            s = (probs[:, self.pos_id] - probs[:, self.neg_id]).tolist()
            out.extend(float(v) for v in s)
        return out
