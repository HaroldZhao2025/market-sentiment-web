import pandas as pd
WIKI = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

def fetch_sp500():
    tables = pd.read_html(WIKI, flavor='bs4')
    df = tables[0]
    rename = {}
    for c in df.columns:
        lc = str(c).lower()
        if 'symbol' in lc or 'ticker' in lc: rename[c] = 'ticker'
        if 'security' in lc: rename[c] = 'name'
        if 'gics' in lc and 'sector' in lc: rename[c] = 'sector'
    df = df.rename(columns=rename)
    df['ticker'] = df['ticker'].astype(str).str.upper().str.replace('.','-', regex=False)
    return df[['ticker','name','sector']]
