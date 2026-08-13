# Sentiment Intelligence — S&P 500 Market Evidence & Research

**Sentiment Intelligence** is an auditable market-intelligence and empirical-research platform built around S&P 500 news sentiment, price reactions, constituent attribution, deterministic event intelligence, signal screening, natural-language querying, and reproducible backtests.

The product principle is simple:

> Do not compete with LLMs by generating generic financial prose. Build proprietary, deterministic, auditable market data and research that humans, LLMs, and agents can consume.

## Author

- Portfolio Strategy by **leolin0407-cmyk** (leolin0407@gmail.com)
- Market Sentiment, Website Design, Repo Setting by **HaroldZhao2025** (stevenfinch2022@outlook.com)

## Website

`https://haroldzhao2025.github.io/market-sentiment-web/`

## Current product surfaces

- `/` — market state, attention, and signal discovery
- `/ask` — Ask the Market: natural-language questions translated into explicit deterministic filters and rankings
- `/ticker/<symbol>` — ticker price/sentiment history, scored headline evidence, and deterministic event drivers
- `/screener` — cross-sectional Market Screener / Signal Explorer
- `/sp500` — true S&P 500 index price, observed sentiment, coverage, and user-selectable **Treemap / Bubbles** constituent views
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

Phase 3 added deterministic intelligence products on top of the evidence layer: Market Screener, Interactive Research Lab, Historical Event Memory, richer deterministic event diagnostics, S&P Attribution V2, and the dark market-intelligence product shell.

## Phase 4 — Query Layer

Phase 4 has started with three upgrades:

1. **Ask the Market** — natural language is parsed into an explicit query plan over the existing Screener fields. The UI shows both the interpretation and the underlying rows; it does not invent generic finance prose.
2. **Dual S&P visualization** — the constituent map now defaults to a classic rectangular treemap while retaining clustered bubbles as an alternative. Both views support Contribution / Sentiment / 1D Return, sector filtering, rich hover evidence, and ticker navigation.
3. **Constituent metadata hardening** — S&P company metadata uses a seven-day Wikipedia cache TTL and yfinance `longName` / `shortName` fallback. The builder fails if company-name coverage drops below 98% rather than silently publishing large numbers of blank names.

The next Phase 4 expansion should build structured agent/API access, Event Memory V2, and Research Lab V2 on top of the same deterministic engine.

## Data sources

- Prices: `yfinance`
- Constituent metadata: Wikipedia S&P 500 constituents table with yfinance metadata fallback
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
- Python source changes now receive compile validation plus deterministic S&P metadata-helper tests.
- Optional production secrets include provider/API credentials used by the data pipeline.

## Important research caveats

The Research Lab and Event Memory are descriptive research tools, not causal estimators or investment recommendations. Overlapping forward-return horizons can induce serial dependence; publication-grade inference should add appropriate robust/clustered standard errors, transaction costs where relevant, and out-of-sample validation.