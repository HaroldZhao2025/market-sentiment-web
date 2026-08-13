# Sentiment Intelligence — S&P 500 Market Evidence & Research

**Sentiment Intelligence** is an auditable market-intelligence and empirical-research platform built around S&P 500 news sentiment, price reactions, constituent attribution, deterministic event intelligence, signal screening, natural-language querying, machine-readable agent contracts, and reproducible research.

> Do not compete with LLMs by generating generic financial prose. Build proprietary, deterministic, auditable market data and research that humans, LLMs, and agents can consume.

## Author

- Portfolio Strategy by **leolin0407-cmyk** (leolin0407@gmail.com)
- Market Sentiment, Website Design, Repo Setting by **HaroldZhao2025** (stevenfinch2022@outlook.com)

## Website

`https://haroldzhao2025.github.io/market-sentiment-web/`

## Current product surfaces

- `/` — market state, attention, and signal discovery
- `/ask` — **Ask the Market**: natural-language questions translated into explicit deterministic filters and rankings
- `/ticker/<symbol>` — ticker price/sentiment history, scored headline evidence, and deterministic event drivers
- `/screener` — cross-sectional Market Screener / Signal Explorer
- `/sp500` — true S&P 500 index price, observed sentiment, coverage, and user-selectable **Treemap / Bubbles** views
- `/attribution` — company → industry → sector → index sentiment-contribution decomposition
- `/events` — Historical Event Memory with taxonomy, novelty, source breadth, disagreement, and observed price reactions
- `/lab` — **Research Lab V2** with observed-only cross-sectional sorts, HAC inference, OOS checks, turnover and cost sensitivity
- `/agent` — machine-interface documentation for external agents and research workflows
- `/portfolio` — lagged sentiment-driven portfolio research and benchmark comparison
- `/research` — generated empirical research library
- `/data` — machine-readable data contracts
- `/methodology` — methodology, definitions, and limitations
- `/agent-manifest.json` — static discovery/semantics contract for agents

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

## Phase 4 — completed

Phase 4 turns the Intelligence Engine into a queryable research platform:

1. **Ask the Market** — natural language maps to an explicit query plan over deterministic Screener fields; the UI shows the interpretation and underlying rows.
2. **Dual S&P visualization** — classic Treemap is the default, with clustered Bubbles available as an alternative. Both support Contribution / Sentiment / 1D Return, sector filtering, rich hover evidence, and ticker navigation.
3. **Constituent metadata hardening** — seven-day Wikipedia metadata TTL, yfinance `longName` / `shortName` fallback, ticker-variant handling, and a 98% company-name coverage gate.
4. **Research Lab V2** — uses only `sentiment_observed=true` dates, prior observed sentiment for changes, daily cross-sectional sorts, Newey-West/Bartlett HAC inference, chronological 70/30 validation, turnover diagnostics, transaction-cost sensitivity, and downloadable JSON specifications/results.
5. **Agent interface** — `/agent` plus `/agent-manifest.json` provide stable static machine-readable contracts appropriate for GitHub Pages rather than pretending the site has an always-on dynamic API server.

## Data sources

- Prices: `yfinance`
- Constituent metadata: Wikipedia S&P 500 constituents table with yfinance metadata fallback
- News: Finnhub and Yahoo/yfinance sources in the current production pipeline
- Sentiment: ProsusAI/FinBERT

## Run locally

```bash
pip install -r requirements.txt
cd apps/web
npm ci
npx tsc --noEmit
npm run build
```

## CI / GitHub Pages

- Production data/site deployment runs through `.github/workflows/pipeline.yml`.
- Frontend PR CI strictly runs TypeScript typecheck and Next.js production build.
- Python source changes receive compile validation plus deterministic S&P metadata-helper tests.
- Build failures are not suppressed.

## Research caveats

Research Lab V2 and Event Memory are descriptive research tools, not causal estimators or investment recommendations. HAC inference addresses overlapping-return serial dependence only at the displayed diagnostic level; publication-grade work should still consider richer clustering, transaction-cost assumptions, multiple-testing concerns, point-in-time universe construction, and genuinely held-out validation.