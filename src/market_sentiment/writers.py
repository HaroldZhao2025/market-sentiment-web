from pathlib import Path
import numpy as np, pandas as pd
from .utils import dump_json

def build_ticker_json(ticker, price_df, daily_sent, recent_news):
    p=price_df.copy(); p['date']=p['date'].dt.normalize()
    s=(p[['date','close']].merge(daily_sent[['date','S']], on='date', how='left').sort_values('date'))
    s['S']=s['S'].fillna(0.0)
    last_S=float(s['S'].dropna().iloc[-1]) if len(s)>0 else 0.0
    pred=float(np.tanh(last_S/5.0)*0.01)
    headlines=(recent_news.sort_values('ts', ascending=False).head(20)[['ts','title','source','url']])
    return {'ticker':ticker,'insights':{'live_sentiment':'Positive' if last_S>0 else ('Negative' if last_S<0 else 'Neutral'),'predicted_return':pred,'advisory':'Strong Buy' if last_S>1 else ('Buy' if last_S>0.3 else ('Hold' if last_S>-0.3 else 'Sell'))},'series':{'date':s['date'].astype(str).tolist(),'price':s['close'].astype(float).tolist(),'sentiment':s['S'].astype(float).tolist()},'recent_headlines':headlines.to_dict(orient='records')}

def write_ticker_json(obj, out_dir: Path): out=Path(out_dir)/f"{obj['ticker'].upper()}.json"; dump_json(obj, out); return out

def write_index_json(summary_df: pd.DataFrame, out_dir: Path):
    out = Path(out_dir) / 'index.json'
    dump_json(summary_df.to_dict(orient='records'), out); return out

def write_portfolio_json(pnl_df: pd.DataFrame, out_dir: Path):
    out = Path(out_dir) / 'portfolio.json'
    dump_json({'date': pnl_df['date'].astype(str).tolist(),
               'equity': pnl_df['equity'].astype(float).tolist(),
               'ret_net': pnl_df['ret_net'].astype(float).tolist()}, out); return out

def write_earnings_json(ticker: str, df: pd.DataFrame, out_dir: Path):
    out = Path(out_dir) / 'earnings' / f'{ticker.upper()}.json'
    events = df.sort_values('ts').assign(ts=lambda d: d['ts'].astype(str))[['ts','quarter','year','text']]
    dump_json({'ticker':ticker,'events':events.to_dict(orient='records')}, out); return out
