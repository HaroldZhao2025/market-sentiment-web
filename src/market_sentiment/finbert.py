from __future__ import annotations
import os, re, shutil
from typing import List, Dict
import numpy as np
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# Avoid parallelism warnings
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

_MODEL_ID = "ProsusAI/finbert"

def _hf_model_dir(model_id: str, hf_home: str) -> str:
    """
    Returns both possible cache locations. We will purge both if needed.
    """
    model_key = model_id.replace("/", "--")
    # Modern layout used by huggingface_hub
    return os.path.join(hf_home, f"models--{model_key}")

def _hf_model_dir_hub(model_id: str, hf_home: str) -> str:
    # Older layout under 'hub/'
    model_key = model_id.replace("/", "--")
    return os.path.join(hf_home, "hub", f"models--{model_key}")

def _purge_model_cache(model_id: str, hf_home: str | None) -> None:
    if not hf_home:
        hf_home = os.path.expanduser("~/.cache/huggingface")
    paths = [_hf_model_dir(model_id, hf_home), _hf_model_dir_hub(model_id, hf_home)]
    for p in paths:
        if os.path.isdir(p):
            try:
                shutil.rmtree(p)
            except Exception:
                pass

class FinBERT:
    """
    Lightweight FinBERT wrapper that:
      - uses HF_HOME (not deprecated TRANSFORMERS_CACHE)
      - retries cleanly if the cache is corrupted (symlink issues)
      - provides batch scoring for headlines + long-text scoring for earnings docs
    """
    def __init__(self, model_name: str = _MODEL_ID, device: str | None = None, hf_home: str | None = None):
        self.model_name = model_name
        self.hf_home = hf_home or os.environ.get("HF_HOME") or os.path.expanduser("~/.cache/huggingface")
        self.cache_dir = self.hf_home  # let HF manage its subfolders

        if device is None:
            device = "cuda" if torch.cuda.is_available() else "cpu"
        self.device = torch.device(device)

        # First attempt
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name, use_fast=True, cache_dir=self.cache_dir)
            self.model = AutoModelForSequenceClassification.from_pretrained(
                self.model_name, num_labels=3, cache_dir=self.cache_dir
            ).to(self.device).eval()
        except FileNotFoundError:
            # Cache is likely corrupted; purge and retry once
            _purge_model_cache(self.model_name, self.hf_home)
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name, use_fast=True, cache_dir=self.cache_dir)
            self.model = AutoModelForSequenceClassification.from_pretrained(
                self.model_name, num_labels=3, cache_dir=self.cache_dir
            ).to(self.device).eval()

    @torch.no_grad()
    def score_batch(self, texts: List[str], batch_size: int = 16, max_length: int = 96) -> List[Dict[str, float]]:
        if not texts:
            return []
        out: List[Dict[str, float]] = []
        for i in range(0, len(texts), batch_size):
            chunk = texts[i:i+batch_size]
            enc = self.tokenizer(
                chunk,
                truncation=True,
                padding=True,
                max_length=max_length,
                return_tensors="pt"
            )
            enc = {k: v.to(self.device) for k, v in enc.items()}
            logits = self.model(**enc).logits  # shape [B, 3]
            probs = torch.softmax(logits, dim=-1).cpu().numpy()

            # ProsusAI/finbert label order: 0: negative, 1: neutral, 2: positive
            for p in probs:
                neg, neu, pos = float(p[0]), float(p[1]), float(p[2])
                conf = float(np.max(p))
                out.append({"negative": neg, "neutral": neu, "positive": pos, "confidence": conf})
        return out

    def score_long_text(self, text: str, max_sentences: int = 32, min_len: int = 20) -> Dict[str, float]:
        """
        Split long earnings/PR text into sentences/chunks, score each, and average.
        """
        if not text or not isinstance(text, str):
            return {"negative": 0.0, "neutral": 1.0, "positive": 0.0, "confidence": 0.0}

        # Simple sentence-ish splitting
        parts = re.split(r"(?<=[\.\!\?])\s+", text)
        parts = [p.strip() for p in parts if len(p.strip()) >= min_len]
        if not parts:
            parts = [text.strip()[:512]]

        parts = parts[:max_sentences]
        scores = self.score_batch(parts, batch_size=16, max_length=128)
        if not scores:
            return {"negative": 0.0, "neutral": 1.0, "positive": 0.0, "confidence": 0.0}

        # Average across parts
        neg = float(np.mean([s["negative"] for s in scores]))
        neu = float(np.mean([s["neutral"] for s in scores]))
        pos = float(np.mean([s["positive"] for s in scores]))
        conf = float(np.mean([s["confidence"] for s in scores]))
        # Renormalize to sum ~ 1 (in case of small numerical drift)
        s = neg + neu + pos
        if s > 0:
            neg, neu, pos = neg/s, neu/s, pos/s
        return {"negative": neg, "neutral": neu, "positive": pos, "confidence": conf}
