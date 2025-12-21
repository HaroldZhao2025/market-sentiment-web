# Market Sentiment Web — S&P 500 (News) • Website + Daily Pipeline

## Author
- Portfolio Strategy by leolin0407-cmyk (leolin0407@gmail.com)
- Market Sentiment, Website Design, Repo Setting by HaroldZhao2025 (sz695@cornell.edu)

## Website:
https://haroldzhao2025.github.io/market-sentiment-web/

### Current features:
- S&P500 individual stock sentiment analysis
- S&P500 market index and sentiment
- Sentiment-based portfolio strategy

## Data (online)
- Prices: `yfinance`
- News: `yfinance` news (default) OR `NewsAPI` when `NEWS_API_KEY` is provided
- Earnings transcripts: `FinancialModelingPrep` when `FMP_API_KEY` is provided (extendable to EDGAR/Finnhub)

## Run locally
```bash
# Python
pip install -r requirements.txt
python -m market_sentiment.cli.build_universe --out data/sp500.csv
python -m market_sentiment.cli.build_json --universe data/sp500.csv --start 2023-01-01 --end 2023-12-31 --out apps/web/public/data
# Optionally add FinBERT (slower):
# python -m market_sentiment.cli.build_json --universe data/sp500.csv --sentiment finbert --batch 16

# Frontend
cd apps/web
npm install
npm run build && npm run export
# open apps/web/out/index.html
```

## CI / GitHub Pages
- Workflow builds JSON **daily**, builds site, deploys to Pages.
- Secrets (optional): `NEWS_API_KEY`, `FMP_API_KEY`

## Site
- `/` — S&P 500 **overview** (search, sector filter, cards; top/bottom predicted return)
- `/ticker/[symbol]` — per-ticker dashboard (overlay chart, insights, headlines)
- `sp500` - last 30 days S&P index and sentiment
- `/portfolio` — daily long/short equity curve + holdings

