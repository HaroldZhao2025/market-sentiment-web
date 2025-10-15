import os, requests, pandas as pd

def fetch_transcripts_fmp(ticker:str) -> pd.DataFrame:
    key = os.environ.get('FMP_API_KEY')
    if not key: return pd.DataFrame(columns=['ticker','ts','quarter','year','text'])
    url = f'https://financialmodelingprep.com/api/v3/earning_call_transcript/{ticker}?apikey={key}'
    r = requests.get(url, timeout=30)
    if not r.ok: return pd.DataFrame(columns=['ticker','ts','quarter','year','text'])
    rows = []
    for it in r.json() or []:
        ts = pd.to_datetime(it.get('date')).tz_localize('America/New_York')
        rows.append((ticker, ts, it.get('quarter'), it.get('year'), it.get('content','')))
    return pd.DataFrame(rows, columns=['ticker','ts','quarter','year','text'])

def fetch_transcripts(ticker:str) -> pd.DataFrame:
    return fetch_transcripts_fmp(ticker)
