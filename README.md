# Market Sentiment Web — S&P 500 (News) • Website + Daily Pipeline

A lightweight pipeline + static website for tracking **news-based sentiment** across S&P 500 constituents, alongside an index-level sentiment series and a simple sentiment-ranked portfolio strategy.

## Author
- Portfolio Strategy by **leolin0407-cmyk** (leolin0407@gmail.com)
- Market Sentiment, Website Design, Repo Setting by **HaroldZhao2025** (sz695@cornell.edu)

## Website
https://haroldzhao2025.github.io/market-sentiment-web/

## Current features
- S&P 500 **individual stock** news sentiment analysis (per-ticker dashboard)
- S&P 500 **index price + cap-weighted sentiment**
- S&P 500 **interactive map / heatmap**
- Sentiment-based **portfolio strategy** (weekly rebalance, lagged ranking, long/short)

> Note: We **do not** have earnings / transcript analysis shown on the website yet.

## Data (online)
- Prices: `yfinance`
- News:
  - Default: `yfinance` news
  - Optional: `NewsAPI` when `NEWS_API_KEY` is provided
- Earnings transcripts (pipeline-ready / extendable):
  - `FinancialModelingPrep` when `FMP_API_KEY` is provided (extendable to EDGAR / Finnhub)
  - (UI not implemented yet)

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
````

## CI / GitHub Pages

* Workflow builds JSON (scheduled), builds the site, and deploys to GitHub Pages.
* Secrets (optional):

  * `NEWS_API_KEY`
  * `FMP_API_KEY`

## Site

* `/` — S&P 500 **overview** (search, sector filter, cards; top/bottom predicted return)
* `/ticker/<symbol>` — per-ticker dashboard (overlay chart, insights, headlines)
* `/sp500` — last 30 days S&P index price + cap-weighted sentiment
* `/portfolio` — daily long/short equity curve + latest holdings
* `/research` — live empirical notes / descriptive analytics (NOT investment advice; results may change as data updates)
