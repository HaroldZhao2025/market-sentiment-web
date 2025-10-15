from __future__ import annotations
import os
import numpy as np
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

HF_MODEL = os.getenv("FINBERT_MODEL", "ProsusAI/finbert")

class FinBERT:
    def __init__(self, model_name: str = HF_MODEL, device: str | None = None):
        self.tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=True)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.model.eval()
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.model.to(self.device)

        # Map indices to labels, then find positions of pos/neu/neg
        id2label = {int(k): v for k, v in self.model.config.id2label.items()}
        lab = {v.lower(): k for k, v in id2label.items()}
        # FinBERT label names vary: "positive", "neutral", "negative"
        self.idx_pos = lab.get("positive")
        self.idx_neu = lab.get("neutral")
        self.idx_neg = lab.get("negative")
        if self.idx_pos is None or self.idx_neu is None or self.idx_neg is None:
            # Fallback: assume 0=neg,1=neu,2=pos
            self.idx_neg, self.idx_neu, self.idx_pos = 0, 1, 2

    @torch.no_grad()
    def score(self, texts: list[str], batch_size: int = 16, max_length: int = 256) -> np.ndarray:
        """
        Returns probs numpy array shape [N,3] in model's label order.
        """
        if not texts:
            return np.zeros((0, 3), dtype=float)

        out = []
        for i in range(0, len(texts), batch_size):
            chunk = texts[i:i + batch_size]
            enc = self.tokenizer(
                chunk, return_tensors="pt", truncation=True,
                max_length=max_length, padding=True
            ).to(self.device)
            logits = self.model(**enc).logits
            probs = torch.softmax(logits, dim=-1).cpu().numpy()
            out.append(probs)
        return np.vstack(out)

def add_finbert_score(df, text_col: str = "text", fb: FinBERT | None = None, batch_size: int = 16):
    """
    Adds columns: pos, neu, neg, S  (S = pos - neg)
    """
    fb = fb or FinBERT()
    if df.empty:
        df = df.copy()
        for c in ("pos", "neu", "neg", "S"):
            df[c] = 0.0
        return df

    texts = df[text_col].fillna("").astype(str).tolist()
    probs = fb.score(texts, batch_size=batch_size)

    df = df.copy()
    if probs.shape[0] == 0:
        for c in ("pos", "neu", "neg", "S"):
            df[c] = 0.0
        return df

    pos = probs[:, fb.idx_pos]
    neu = probs[:, fb.idx_neu]
    neg = probs[:, fb.idx_neg]
    df["pos"], df["neu"], df["neg"] = pos, neu, neg
    df["S"] = df["pos"] - df["neg"]
    return df
