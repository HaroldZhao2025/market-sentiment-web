# src/market_sentiment/finbert.py
from __future__ import annotations

import os
import math
from typing import List, Sequence, Union

import torch
import numpy as np
from transformers import AutoTokenizer, AutoModelForSequenceClassification


# Avoid tokenizer multi-thread noise on CI
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")


def _as_list(x: Union[str, Sequence[str]]) -> List[str]:
    if x is None:
        return []
    if isinstance(x, str):
        return [x]
    return list(x)


class FinBERT:
    """
    Thin wrapper over ProsusAI/finbert that returns a scalar sentiment score per text.

    Score definition:
        S = P(positive) - P(negative) ∈ [-1, 1]
    """

    def __init__(
        self,
        model_name: str | None = None,
        device: str | None = None,
        max_length: int = 256,
    ) -> None:
        self.model_name = model_name or os.getenv("FINBERT_MODEL", "ProsusAI/finbert")
        self.max_length = int(max_length)

        # Device resolution (CPU on CI; CUDA if available locally)
        if device is not None:
            self.device = torch.device(device)
        else:
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        # Load model/tokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name, use_fast=True)
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
        self.model.to(self.device)
        self.model.eval()

        # Determine label indices
        # Expected labels something like: {0: 'negative', 1: 'neutral', 2: 'positive'}
        id2label = self.model.config.id2label
        self.idx_pos = None
        self.idx_neg = None
        for i, lab in id2label.items():
            name = str(lab).lower()
            if "pos" in name:
                self.idx_pos = int(i)
            elif "neg" in name:
                self.idx_neg = int(i)

        if self.idx_pos is None or self.idx_neg is None:
            # Fallback guess (ProsusAI/finbert standard order)
            self.idx_neg, self.idx_pos = 0, 2

    @torch.no_grad()
    def score(
        self,
        texts: Union[str, Sequence[str]],
        *,
        batch: int = 16,
        max_length: int | None = None,
    ) -> List[float]:
        """
        Compute sentiment for a list of texts.

        Args:
            texts: str or list[str]
            batch: batch size for tokenization/inference (default 16)
            max_length: optional override of truncation length (default: self.max_length)

        Returns:
            list[float]: sentiment S per text in [-1, 1]
        """
        items = _as_list(texts)
        if not items:
            return []

        batch = max(int(batch), 1)
        max_len = int(max_length or self.max_length)

        out_scores: List[float] = []
        for i in range(0, len(items), batch):
            chunk = items[i : i + batch]

            # Replace None/NaN/whitespace with empty to avoid tokenization errors
            clean = []
            for t in chunk:
                if t is None:
                    clean.append("")
                elif isinstance(t, float) and (math.isnan(t) or math.isinf(t)):
                    clean.append("")
                else:
                    s = str(t)
                    clean.append(s if s.strip() else "")

            enc = self.tokenizer(
                clean,
                padding=True,
                truncation=True,
                max_length=max_len,
                return_tensors="pt",
            ).to(self.device)

            logits = self.model(**enc).logits  # [B, 3]
            # softmax -> probs
            probs = torch.softmax(logits, dim=-1)  # [B, 3]
            p_pos = probs[:, self.idx_pos]
            p_neg = probs[:, self.idx_neg]
            s = (p_pos - p_neg).detach().cpu().numpy().astype(np.float32)
            out_scores.extend([float(v) for v in s])

        return out_scores


def score_texts(fb: FinBERT, texts: Union[str, Sequence[str]], batch: int = 16) -> List[float]:
    """
    Backwards-compatible helper so older code can call score_texts(fb, texts, batch=...).
    """
    return fb.score(texts, batch=batch)
