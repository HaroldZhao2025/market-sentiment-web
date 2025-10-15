from __future__ import annotations
import os
import requests
import pandas as pd

def fetch_transcripts(ticker: str) -> pd.DataFrame:
    """
    Earnings call transcripts via FMP.
    Returns columns: ['ts','quarter','year','text'] (tz-aware UTC).
    """
    api_key = os.getenv("FMP_API_KEY", "")
    if not api_key:
        return pd.DataFrame(columns=["ts","quarter","year","text"])

    url = f"https://financialmodelingprep.com/api/v3/earning_call_transcript/{ticker}"
    try:
        r = requests.get(url, params={"apikey": api_key}, timeout=30)
        data = r.json() if r.status_code == 200 else []
    except Exception:
        data = []

    rows = []
    for it in (data or [])[:12]:  # last 12 calls
        dt = it.get("date")
        q = it.get("quarter") or it.get("quarterNumber")
        y = it.get("year")
        txt = it.get("content") or it.get("text") or ""
        if not dt or not txt:
            continue
        rows.append((pd.to_datetime(dt, utc=True), q, y, txt))
    return pd.DataFrame(rows, columns=["ts","quarter","year","text"])
