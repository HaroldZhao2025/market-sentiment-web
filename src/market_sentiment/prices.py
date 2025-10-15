import pandas as pd, yfinance as yf
def fetch_prices(ticker, start, end):
    df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=False)
    if df.empty: return pd.DataFrame(columns=['date','open','high','low','close','volume'])
    df = df.reset_index().rename(columns=str.lower)
    df['date'] = pd.to_datetime(df['date']).dt.tz_localize('America/New_York')
    return df[['date','open','high','low','close','volume']]
