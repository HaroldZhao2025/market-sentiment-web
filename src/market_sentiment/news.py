import os, requests, pandas as pd, yfinance as yf

def news_yfinance(ticker: str) -> pd.DataFrame:
    try:
        items = yf.Ticker(ticker).news or []
    except Exception:
        items = []
    rows = []
    for it in items:
        ts = pd.to_datetime(it.get('providerPublishTime', None), unit='s', errors='coerce')
        if ts is pd.NaT: ts = pd.Timestamp.utcnow()
        ts = ts.tz_convert('America/New_York')
        rows.append((ticker, ts, it.get('publisher','unknown'), it.get('title',''), it.get('link','')))
    return pd.DataFrame(rows, columns=['ticker','ts','source','title','url'])

def news_newsapi(ticker: str, api_key: str, start: str, end: str) -> pd.DataFrame:
    url = 'https://newsapi.org/v2/everything'
    params = {'q': ticker, 'from': start, 'to': end, 'language': 'en', 'pageSize': 100, 'sortBy': 'publishedAt', 'apiKey': api_key}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    arts = r.json().get('articles', [])
    rows = []
    for a in arts:
        ts = pd.to_datetime(a.get('publishedAt')).tz_convert('America/New_York')
        src = (a.get('source') or {}).get('name', 'unknown')
        title = (a.get('title') or '') + ' ' + (a.get('description') or '')
        rows.append((ticker, ts, src, title, a.get('url','')))
    return pd.DataFrame(rows, columns=['ticker','ts','source','title','url'])

def fetch_news(ticker: str, start: str, end: str) -> pd.DataFrame:
    key = os.environ.get('NEWS_API_KEY')
    if key:
        try: return news_newsapi(ticker, key, start, end)
        except Exception: pass
    return news_yfinance(ticker)
