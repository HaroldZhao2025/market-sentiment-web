# src/market_sentiment/finbert.py
from __future__ import annotations

from typing import List
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

__all__ = ["FinBERT"]


class FinBERT:
    """
    Wrapper for ProsusAI/finbert that returns a single sentiment score per text:
        S = P(positive) - P(negative)  in [-1, 1]
    """

    def __init__(self, model_name: str = "ProsusAI/finbert"):
        # Use CPU in CI. HF cache is controlled by your workflow.
        self.device = torch.device("cpu")
        self.tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=True)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.model.to(self.device)
        self.model.eval()

    @torch.no_grad()
    def score(self, texts: List[str], batch_size: int = 16) -> List[float]:
        """
        Returns one float per input:
            S = softmax(logits)[positive] - softmax(logits)[negative]
        """
        if not texts:
            return []
        # Ensure strings
        texts = ["" if t is None else str(t) for t in texts]

        out: List[float] = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            enc = self.tokenizer(
                batch, padding=True, truncation=True, max_length=256, return_tensors="pt"
            )
            enc = {k: v.to(self.device) for k, v in enc.items()}
            logits = self.model(**enc).logits  # [B, 3]
            probs = torch.softmax(logits, dim=-1)

            # FinBERT labels: ['negative', 'neutral', 'positive']
            s = (probs[:, 2] - probs[:, 0]).cpu().tolist()
            out.extend(float(v) for v in s)

        return out
