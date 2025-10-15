import pandas as pd, numpy as np

def daily_long_short(scores: pd.DataFrame, long_q=0.9, short_q=0.1) -> pd.DataFrame:
    df = scores.sort_values(['date','ticker']).reset_index(drop=True)
    parts=[]
    for d,g in df.groupby('date'):
        qh=g['score'].quantile(long_q); ql=g['score'].quantile(short_q)
        w=np.where(g['score']>=qh, 1.0, np.where(g['score']<=ql, -1.0, 0.0))
        if (w>0).sum()>0: w[w>0]=w[w>0]/(w>0).sum()
        if (w<0).sum()>0: w[w<0]=w[w<0]/abs((w<0).sum())
        gg=g.copy(); gg['w']=w; parts.append(gg)
    bt=pd.concat(parts, ignore_index=True) if parts else df.copy()
    pnl=bt.groupby('date').apply(lambda x: float((x['w']*x['y']).sum())).reset_index(name='ret')
    pnl['cost']=bt.groupby('date').apply(lambda x: float(np.sum(np.abs(x['w']))*0.0005)).values
    pnl['ret_net']=pnl['ret']-pnl['cost']; pnl['equity']=(1+pnl['ret_net']).cumprod()
    return pnl
