import pandas as pd, numpy as np

def apply_cutoff_and_roll(news_df, cutoff_min=30):
    news=news_df.copy(); ts=pd.to_datetime(news['ts'], errors='coerce')
    if getattr(ts.dt,'tz',None) is None: ts=ts.dt.tz_localize('America/New_York')
    else: ts=ts.dt.tz_convert('America/New_York')
    news['ts']=ts
    close=ts.dt.normalize()+pd.Timedelta(hours=16)
    thr=close-pd.Timedelta(minutes=cutoff_min)
    mask=ts>thr
    eff=ts.copy(); eff[mask]=eff[mask]+pd.Timedelta(days=1)
    news['effective_date']=eff.dt.normalize()
    return news

def add_forward_returns(prices):
    d=prices.sort_values(['date']).copy()
    d['ret_cc_1d']=d['close'].pct_change().shift(-1)
    d['ret_oc_1d']=(d['close']/d['open']-1).shift(-1)
    return d

def aggregate_daily(scored, tau_hours=12):
    df=scored.copy(); df['effective_date']=pd.to_datetime(df['effective_date']).dt.tz_convert('America/New_York')
    df['date']=df['effective_date'].dt.normalize(); df['w']=df['conf'].clip(0,1)
    df['signed']=df['pos']-df['neg']
    agg=(df.groupby(['date','ticker']).agg(S=('signed', lambda x: float(np.sum(x.values*df.loc[x.index,'w'].values))), count=('signed','size')).reset_index())
    agg=agg.sort_values(['ticker','date']); agg['ema3']=agg.groupby('ticker')['S'].apply(lambda s: s.ewm(span=3, adjust=False).mean()).reset_index(level=0, drop=True)
    agg['dS']=agg.groupby('ticker')['S'].diff()
    return agg
