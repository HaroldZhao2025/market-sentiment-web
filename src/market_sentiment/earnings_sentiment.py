from __future__ import annotations
import pandas as pd
from .finbert import FinBERT

_EASTERN = "America/New_York"

def score_earnings_daily(fb: FinBERT, docs: pd.DataFrame) -> pd.DataFrame:
    """
    Input docs: ['ts','title','url','text'] (UTC)
    Output: ['date','S_earn']
    """
    if docs.empty:
        return pd.DataFrame(columns=["date","S_earn"])
    rows = []
    for _, r in docs.iterrows():
        sent = fb.score_long_text(r["text"])
        S = (sent["positive"] - sent["negative"]) * sent["confidence"]
        rows.append((r["ts"], S))
    df = pd.DataFrame(rows, columns=["ts","S_earn"])
    df["date"] = pd.to_datetime(df["ts"], utc=True).dt.tz_convert(_EASTERN).dt.normalize()
    out = df.groupby("date", as_index=False)["S_earn"].mean()
    return out[["date","S_earn"]]
