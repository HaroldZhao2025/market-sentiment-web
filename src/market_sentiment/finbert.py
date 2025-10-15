# src/market_sentiment/finbert.py
from __future__ import annotations
from typing import List
import torch, numpy as np
import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification

class FinBERT:
    def __init__(self, model_name: str = "ProsusAI/finbert", max_len: int = 256, batch_size: int = 16):
        self.tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=True)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.model.eval()
        self.device = torch.device("cpu")
        self.model.to(self.device)
        self.max_len = max_len
        self.batch = batch_size
        # Label order is [positive, negative, neutral] for ProsusAI/finbert
        self.labels = ["positive","negative","neutral"]

    @torch.no_grad()
    def score(self, texts: List[str]) -> pd.DataFrame:
        out_rows=[]
        for i in range(0, len(texts), self.batch):
            chunk = texts[i:i+self.batch]
            enc = self.tokenizer(
                chunk, padding=True, truncation=True, max_length=self.max_len, return_tensors="pt"
            ).to(self.device)
            logits = self.model(**enc).logits.cpu().numpy()
            probs = (np.exp(logits) / np.exp(logits).sum(axis=1, keepdims=True))
            for t, p in zip(chunk, probs):
                p_pos, p_neg, p_neu = float(p[0]), float(p[1]), float(p[2])
                s = p_pos - p_neg
                label = self.labels[int(np.argmax(p))]
                conf = float(abs(s))
                out_rows.append({
                    "text": t, "p_pos": p_pos, "p_neg": p_neg, "p_neu": p_neu,
                    "s": s, "label": label, "conf": conf
                })
        return pd.DataFrame(out_rows)
