from __future__ import annotations
import os, re, shutil
from typing import List, Dict
import numpy as np
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
_MODEL_ID = "ProsusAI/finbert"

def _purge(model_id: str, hf_home: str):
    key = model_id.replace("/", "--")
    for p in [
        os.path.join(hf_home, f"models--{key}"),
        os.path.join(hf_home, "hub", f"models--{key}")
    ]:
        if os.path.isdir(p):
            try: shutil.rmtree(p)
            except Exception: pass

class FinBERT:
    def __init__(self, model_name: str = _MODEL_ID, device: str | None = None, hf_home: str | None = None):
        self.model_name = model_name
        self.hf_home = hf_home or os.environ.get("HF_HOME") or os.path.expanduser("~/.cache/huggingface")
        self.cache_dir = self.hf_home
        if device is None: device = "cuda" if torch.cuda.is_available() else "cpu"
        self.device = torch.device(device)
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name, use_fast=True, cache_dir=self.cache_dir)
            self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name, num_labels=3, cache_dir=self.cache_dir).to(self.device).eval()
        except FileNotFoundError:
            _purge(self.model_name, self.hf_home)
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name, use_fast=True, cache_dir=self.cache_dir)
            self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name, num_labels=3, cache_dir=self.cache_dir).to(self.device).eval()

    @torch.no_grad()
    def score_batch(self, texts: List[str], batch_size: int = 16, max_length: int = 96) -> List[Dict[str,float]]:
        if not texts: return []
        out: List[Dict[str,float]] = []
        for i in range(0, len(texts), batch_size):
            chunk = texts[i:i+batch_size]
            enc = self.tokenizer(chunk, truncation=True, padding=True, max_length=max_length, return_tensors="pt")
            enc = {k: v.to(self.device) for k,v in enc.items()}
            probs = torch.softmax(self.model(**enc).logits, dim=-1).cpu().numpy()
            for p in probs:
                neg, neu, pos = float(p[0]), float(p[1]), float(p[2])
                out.append({"negative":neg, "neutral":neu, "positive":pos, "confidence":float(np.max(p))})
        return out

    def score_long_text(self, text: str, max_sentences: int = 32, min_len: int = 20) -> Dict[str,float]:
        if not text or not isinstance(text, str):
            return {"negative":0.0,"neutral":1.0,"positive":0.0,"confidence":0.0}
        parts = [p.strip() for p in re.split(r"(?<=[\.\!\?])\s+", text) if len(p.strip())>=min_len][:max_sentences] or [text[:512]]
        s = self.score_batch(parts, batch_size=16, max_length=128) or [{"negative":0.0,"neutral":1.0,"positive":0.0,"confidence":0.0}]
        neg = float(np.mean([x["negative"] for x in s])); neu = float(np.mean([x["neutral"] for x in s])); pos = float(np.mean([x["positive"] for x in s])); conf = float(np.mean([x["confidence"] for x in s]))
        tot = neg+neu+pos; 
        if tot>0: neg,neu,pos = neg/tot, neu/tot, pos/tot
        return {"negative":neg,"neutral":neu,"positive":pos,"confidence":conf}
