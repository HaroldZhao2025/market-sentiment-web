# Lexicon fallback (fast) used in CI; FinBERT optional via market_sentiment.finbert
from typing import Tuple
def lexicon_score(text:str) -> Tuple[float,float,float,float]:
    POS={'beat','beats','surge','record','strong','growth','upgrade','win','optimism','positive','up','soar','raise','top'}
    NEG={'miss','plunge','weak','downgrade','loss','concern','risk','lawsuit','negative','down','cut','warn'}
    toks=(text or '').lower().split()
    pos=sum(t in POS for t in toks); neg=sum(t in NEG for t in toks)
    neu=1 if (pos==0 and neg==0) else 0
    conf=min(1.0, 0.5+0.1*(pos+neg))
    s=pos+neg+neu+1e-9
    return pos/s, neg/s, neu/s, conf
