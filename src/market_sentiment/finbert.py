from __future__ import annotations
import torch, numpy as np
from transformers import AutoTokenizer, AutoModelForSequenceClassification

LABELS = ['positive','negative','neutral']

class FinBERT:
    def __init__(self, model_name='ProsusAI/finbert', device=None):
        self.device = device or ('cuda' if torch.cuda.is_available() else 'cpu')
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name).to(self.device).eval()

    @torch.no_grad()
    def score_batch(self, texts, batch_size=32):
        res = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i+batch_size]
            inputs = self.tokenizer(batch, return_tensors='pt', truncation=True, max_length=512, padding=True).to(self.device)
            logits = self.model(**inputs).logits
            probs = torch.softmax(logits, dim=-1).cpu().numpy()
            res.extend(probs)
        # return as list of dicts
        out = []
        for p in res:
            out.append({'positive': float(p[0]), 'negative': float(p[1]), 'neutral': float(p[2]), 'conf': float(np.max(p))})
        return out
