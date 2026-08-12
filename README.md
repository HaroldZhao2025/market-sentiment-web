# Sentiment Intelligence — S&P 500 Market Evidence & Research

**Sentiment Intelligence** is an auditable market-intelligence and empirical-research platform built around S&P 500 news sentiment, price reactions, constituent attribution, deterministic event intelligence, signal screening, and reproducible backtests.

The product principle is simple:

> Do not compete with LLMs by generating generic financial prose. Build proprietary, deterministic, auditable market data and research that humans, LLMs, and agents can consume.

## Author

- Portfolio Strategy by **leolin0407-cmyk** (leolin0407@gmail.com)
- Market Sentiment, Website Design, Repo Setting by **HaroldZhao2025** (stevenfinch2022@outlook.com)

## Website

`https://haroldzhao2025.github.io/market-sentiment-web/`

## Current product surfaces

- `/` — market state, attention, and signal discovery
- `/ticker/<symbol>` — ticker price/sentiment history, scored headline evidence, and deterministic event drivers
- `/screener` — cross-sectional Market Screener / Signal Explorer
- `/sp500` — true S&P 500 index price, observed sentiment, coverage, and clustered constituent bubble map
- `/attribution` — company → industry → sector → index sentiment-contribution decomposition
- `/events` — Historical Event Memory with event taxonomy, novelty, source breadth, disagreement, and observed price reactions
- `/lab` — Interactive Research Lab for daily cross-sectional signal sorts and forward-return diagnostics
- `/portfolio` — lagged sentiment-driven portfolio research and benchmark comparison
- `/research` — generated empirical research library
- `/data` — machine-readable data contracts
- `/methodology` — methodology, definitions, and limitations

## Core methodology contracts

- Cross-provider duplicate headlines are deduplicated before aggregation.
- FinBERT article sentiment is `P(positive) - P(negative)`.
- **No news is missing, not neutral zero.**
- Ticker-day sentiment is the equal-weight mean of unique scored articles for that ticker/day.
- S&P cap-weighted sentiment renormalizes only across constituents with observed sentiment.
- Constituent contribution is `constituent weight × observed sentiment`.
- Article-level sentiment remains available for headline evidence.
- S&P index price uses a true index source such as `^GSPC`; SPY is not used as an index-level price substitute.
- Portfolio calculations should not be silently changed by presentation/UI work.

## Phase 3 — Intelligence Engine

Phase 3 adds deterministic intelligence products on top of the existing evidence layer:

1. **Market Screener** — filter and rank constituents by sector, weight, sentiment, sentiment change, 1D return, news evidence, event novelty, and sentiment-price divergence.
2. **Interactive Research Lab** — rank the available cross-section separately by trading date, form high/low signal quantiles, and summarize forward-return spreads over 1D / 3D / 5D / 20D horizons.
3. **Historical Event Memory** — classify retained article evidence into explicit event themes and attach observed 1D / 5D price reactions.
4. **Event Intelligence V2** — richer deterministic themes plus novelty, source breadth, and sentiment disagreement diagnostics.
5. **S&P Attribution V2** — additive company → industry → sector → index contribution decomposition.
6. **Product/UI completion** — clustered packed-circle S&P map, native dark Portfolio charts, Portfolio Strategy authorship credit, stronger Research typography, and semantic green/red price styling.

The intelligence engine intentionally computes facts deterministically. LLMs may later parse queries or explain outputs, but they should not replace the underlying calculations.

## Data sources

- Prices: `yfinance`
- News: Finnhub and Yahoo/yfinance sources in the current production pipeline
- Sentiment: ProsusAI/FinBERT
- Earnings/transcript-related pipeline components exist, but transcript analysis is not yet a production website surface.

## Run locally

```bash
# Python
pip install -r requirements.txt

# Frontend
cd apps/web
npm ci
npx tsc --noEmit
npm run build
```

Generated data artifacts are expected under `apps/web/public/data` and `apps/web/public/research` for full production/static-export behavior.

## CI / GitHub Pages

- Production data/site deployment runs through `.github/workflows/pipeline.yml`.
- Frontend PR CI is strict: TypeScript typecheck and Next.js production build must succeed; build failures are not suppressed.
- Optional production secrets include provider/API credentials used by the data pipeline.

## Important research caveats

The Research Lab and Event Memory are descriptive research tools, not causal estimators or investment recommendations. Overlapping forward-return horizons can induce serial dependence; publication-grade inference should add appropriate robust/clustered standard errors, transaction costs where relevant, and out-of-sample validation.
