# src/market_sentiment/finbert.py
from __future__ import annotations

import os
from typing import Iterable, List, Optional, Tuple

import numpy as np
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

# Keep CI stable:
# - Use CPU by default
# - Keep cache controllable by HF_HOME
# - Disable HF telemetry with HF_HUB_DISABLE_TELEMETRY=1 (already in your workflow)
_DEFAULT_MODEL_NAME = os.getenv("FINBERT_MODEL", "ProsusAI/finbert")


def _softmax(x: np.ndarray, axis: int = -1) -> np.ndarray:
    x = x - np.max(x, axis=axis, keepdims=True)
    e = np.exp(x)
    return e / np.sum(e, axis=axis, keepdims=True)


class FinBERT:
    """
    Thin wrapper around ProsusAI/finbert for batch scoring.

    Produces:
      - probs: (N,3) array with columns ordered [P(neg), P(neu), P(pos)] based on model's id2label
      - score S: P(pos) - P(neg)  in [-1, 1]
    """

    def __init__(self, model_name: str = _DEFAULT_MODEL_NAME, device: Optional[str] = None) -> None:
        self.model_name = model_name
        self.tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=True)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.model.eval()

        if device is None:
            device = "cuda" if torch.cuda.is_available() else "cpu"
        self.device = torch.device(device)
        self.model.to(self.device)

        # Map ids to canonical order [neg, neu, pos] using id2label at runtime
        id2label = {int(k): v.lower() for k, v in self.model.config.id2label.items()}
        self._neg_idx = [k for k, v in id2label.items() if "neg" in v][0]
        self._neu_idx = [k for k, v in id2label.items() if "neu" in v][0]
        self._pos_idx = [k for k, v in id2label.items() if "pos" in v][0]
        self._reorder = np.array([self._neg_idx, self._neu_idx, self._pos_idx], dtype=int)

    @torch.no_grad()
    def predict_proba(
        self,
        texts: Iterable[Optional[str]],
        batch_size: int = 32,
        max_length: int = 256,
    ) -> np.ndarray:
        """
        Returns probabilities with shape (N, 3) ordered as [P(neg), P(neu), P(pos)].
        Any None/empty text returns [1/3, 1/3, 1/3] (neutral prior).
        """
        texts_list: List[str] = []
        mask: List[bool] = []
        for t in texts:
            if t is None:
                texts_list.append("")
                mask.append(False)
            else:
                s = str(t).strip()
                texts_list.append(s)
                mask.append(bool(s))

        N = len(texts_list)
        out = np.full((N, 3), 1.0 / 3.0, dtype=np.float32)  # default neutral prior

        if N == 0:
            return out

        # Collect indices of non-empty texts for batching
        idxs = [i for i, ok in enumerate(mask) if ok]
        if not idxs:
            return out

        for start in range(0, len(idxs), batch_size):
            chunk_idx = idxs[start : start + batch_size]
            batch_texts = [texts_list[i] for i in chunk_idx]

            enc = self.tokenizer(
                batch_texts,
                padding=True,
                truncation=True,
                max_length=max_length,
                return_tensors="pt",
            ).to(self.device)

            logits = self.model(**enc).logits  # (B, C)
            logits_np = logits.detach().cpu().numpy()  # (B, C)
            # Reorder to [neg, neu, pos]
            logits_np = logits_np[:, self._reorder]
            probs = _softmax(logits_np, axis=1).astype(np.float32)  # (B, 3)

            out[np.array(chunk_idx, dtype=int)] = probs

        return out

    def score(
        self,
        texts: Iterable[Optional[str]],
        batch_size: int = 32,
        max_length: int = 256,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Returns (S, probs):
          - S = P(pos) - P(neg)  (shape (N,))
          - probs = (N,3) [P(neg), P(neu), P(pos)]
        """
        probs = self.predict_proba(texts, batch_size=batch_size, max_length=max_length)
        s = probs[:, 2] - probs[:, 0]  # pos - neg
        return s.astype(np.float32), probs


# ---- Module-level helper expected by your CLI ----
def score_texts(*args, **kwargs) -> List[float]:
    """
    Flexible wrapper that supports BOTH calling conventions:

      1) Old style (your CLI right now):
           score_texts(fb, texts, batch_size=..., max_length=...)
             - first positional arg is a FinBERT instance

      2) Newer style:
           score_texts(texts, batch=..., max_length=..., fb=...)

    Accepted aliases:
      - batch_size or batch
      - max_length or max_len
    """
    fb: Optional[FinBERT] = None
    texts: Iterable[Optional[str]] = []
    # Aliases
    batch_size = kwargs.pop("batch_size", None)
    if batch_size is None:
        batch_size = kwargs.pop("batch", 32)
    max_length = kwargs.pop("max_length", None)
    if max_length is None:
        max_length = kwargs.pop("max_len", 256)

    # Detect style
    if args and isinstance(args[0], FinBERT):
        # Style #1: score_texts(fb, texts, ...)
        fb = args[0]
        if len(args) > 1:
            texts = args[1]
        else:
            texts = []
    elif args:
        # Style #2: score_texts(texts, ...)
        texts = args[0]
        fb = kwargs.pop("fb", None)
    else:
        # All via kwargs (unlikely in your code, but supported)
        texts = kwargs.get("texts", [])
        fb = kwargs.get("fb", None)

    created = False
    if fb is None:
        fb = FinBERT()
        created = True

    try:
        s, _ = fb.score(texts, batch_size=int(batch_size), max_length=int(max_length))
        return [float(v) for v in s]
    finally:
        # nothing to close explicitly, but keep pattern if you later add resources
        if created:
            pass
