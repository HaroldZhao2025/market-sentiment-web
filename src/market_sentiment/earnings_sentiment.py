from __future__ import annotations
import pandas as pd
from .finbert import FinBERT

_EASTERN = "America/New_York"

def score_earnings_daily(fb: FinBERT, transcripts: pd.DataFrame) -> pd.DataFrame:
    """
    Input: transcripts ['ts','quarter','year','text'] (UTC)
    Output: ['date','ticker','S_earn'] day-level (America/New_York normalized)
    """
    if transcripts.empty:
        return pd.DataFrame(columns=["date","ticker","S_earn"])

    rows = []
    for _, r in transcripts.iterrows():
        sent = fb.score_long_text(r["text"])
        S = (sent["positive"] - sent["negative"]) * sent["confidence"]
        rows.append((r["ts"], S))

    df = pd.DataFrame(rows, columns=["ts","S_earn"])
    df["date"] = pd.to_datetime(df["ts"], utc=True).dt.tz_convert(_EASTERN).dt.normalize()
    out = df.groupby("date", as_index=False)["S_earn"].mean()
    return out[["date","S_earn"]]
