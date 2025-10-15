import argparse
from pathlib import Path
import pandas as pd, numpy as np
from tqdm import tqdm

from market_sentiment.utils import ensure_dir, load_sp500_csv
from market_sentiment.prices import fetch_prices
from market_sentiment.news import fetch_news
from market_sentiment.transcripts import fetch_transcripts
from market_sentiment.sentiment import lexicon_score
from market_sentiment.finbert import FinBERT
from market_sentiment.aggregate import apply_cutoff_and_roll, add_forward_returns, aggregate_daily
from market_sentiment.writers import build_ticker_json, write_ticker_json, write_index_json, write_portfolio_json, write_earnings_json
from market_sentiment.portfolio import daily_long_short

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--universe', type=Path, required=True)
    ap.add_argument('--start', default='2023-01-01')
    ap.add_argument('--end', default='2023-12-31')
    ap.add_argument('--out', type=Path, default=Path('apps/web/public/data'))
    ap.add_argument('--cutoff', type=int, default=30)
    ap.add_argument('--sentiment', choices=['lexicon','finbert'], default='lexicon')
    ap.add_argument('--batch', type=int, default=32, help='batch size for finbert')
    ap.add_argument('--limit', type=int, default=505, help='limit tickers for fast test')
    a=ap.parse_args()

    tickers = load_sp500_csv(a.universe)[:a.limit]
    ensure_dir(a.out)

    fb = None
    if a.sentiment == 'finbert':
        fb = FinBERT()

    summary_rows = []
    all_scores = []  # for portfolio

    for t in tqdm(tickers, desc='Build JSON'):
        prices = fetch_prices(t, a.start, a.end)
        if prices.empty: continue
        prices = add_forward_returns(prices)

        news = fetch_news(t, a.start, a.end)
        if news.empty: news = pd.DataFrame(columns=['ticker','ts','source','title','url'])

        # sentiment scoring (inspired by your analyze_sentiment_batch)
        if a.sentiment == 'finbert':
            probs = fb.score_batch(news['title'].fillna('').tolist(), batch_size=a.batch) if len(news)>0 else []
            scored = pd.DataFrame(probs)
            if not scored.empty:
                scored.insert(0, 'ticker', t)
                scored.insert(1, 'ts', news['ts'].values)
                scored.insert(2, 'source', news['source'].values)
                scored.insert(3, 'title', news['title'].values)
                scored.insert(4, 'url', news['url'].values)
        else:
            rows=[]
            for _,r in news.iterrows():
                pos,neg,neu,conf=lexicon_score(r.get('title',''))
                rows.append((t,r['ts'],r['source'],r['title'],r['url'],pos,neg,neu,conf))
            scored=pd.DataFrame(rows, columns=['ticker','ts','source','title','url','pos','neg','neu','conf'])

        if scored.empty:
            daily = pd.DataFrame({'date':[], 'ticker':[], 'S':[]})
        else:
            scored = apply_cutoff_and_roll(scored, a.cutoff)
            daily = aggregate_daily(scored)

        obj = build_ticker_json(t, prices, daily, news)
        write_ticker_json(obj, a.out)

        # earnings JSON (for page)
        er = fetch_transcripts(t)
        if not er.empty:
            write_earnings_json(t, er, a.out)

        # summary for index page
        last_s = daily[daily['ticker']==t]['S'].tail(1)
        S = float(last_s.values[0]) if len(last_s)>0 else 0.0
        pred = float(np.tanh(S/5.0)*0.01)
        summary_rows.append({'ticker': t, 'S': S, 'predicted_return': pred})

        # accumulate for portfolio: pick last label y if available
        last = prices.tail(1)
        if len(last)>0:
            all_scores.append({'date': last['date'].iloc[0], 'ticker': t, 'score': pred, 'y': last['ret_cc_1d'].iloc[0]})

    # write overview & portfolio
    idx = pd.DataFrame(summary_rows).sort_values('predicted_return', ascending=False)
    write_index_json(idx, a.out)

    port_df = pd.DataFrame(all_scores)
    if not port_df.empty:
        pnl = daily_long_short(port_df, 0.9, 0.1)
        write_portfolio_json(pnl, a.out)

    print('Done ->', a.out)

if __name__=='__main__':
    main()
