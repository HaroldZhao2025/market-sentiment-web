# Sentiment Intelligence — U.S. Company Evidence & S&P 500 Research

**Sentiment Intelligence** is an auditable market-intelligence and empirical-research platform combining a broad U.S. company evidence layer with a strictly separated S&P 500 index/research core.

## Authors

- Portfolio Strategy by **leolin0407-cmyk** — leolin0407@gmail.com
- Market Sentiment, Website Design, Repo Setting by **HaroldZhao2025** — stevenfinch2022@outlook.com

Live site: `https://haroldzhao2025.github.io/market-sentiment-web/`

## Current product surfaces

- `/` — S&P 500 market state and signal discovery
- `/companies` — Composite 1500 company explorer with company visuals and coverage status
- `/ticker/<symbol>` — unified company workspace with **Daily News / Earnings Call** tabs
- `/earnings/<symbol>` — backward-compatible standalone earnings route
- `/ask` — deterministic natural-language query layer over S&P 500 screener fields
- `/screener` — S&P 500 cross-sectional signal explorer
- `/sp500` — true S&P 500 price, observed sentiment, coverage, Treemap and Bubbles
- `/attribution` — company → industry → sector → index contribution
- `/events` — persistent company event stream
- `/lab` — Research Lab V3 with observed-only signal construction and reproducible specification IDs
- `/portfolio` — sentiment strategy research; Portfolio Strategy by leolin0407-cmyk
- `/research` — generated empirical research library
- `/agent` — machine-interface documentation
- `/data` — public JSON contracts
- `/methodology` — definitions, source policy and limitations
- `/agent-manifest.json` — static discovery/semantics contract

## Universe architecture

The broad company layer is the **S&P Composite 1500** coverage target:

- S&P 500
- S&P MidCap 400
- S&P SmallCap 600

This layer supplies company metadata, retained news, extended daily history, earnings evidence and event instances.

The **S&P 500 core remains separate** for index weighting, constituent contribution, portfolio research, Screener/Ask, and Research Lab. Extended companies are never silently injected into those calculations.

## Data semantics

- Cross-source duplicate headlines are removed before aggregation.
- FinBERT article sentiment is `P(positive) - P(negative)`.
- **No news is missing, not neutral zero.**
- Ticker-day sentiment is the equal-weight mean of unique scored headlines for that company/day.
- S&P cap-weighted sentiment renormalizes only across constituents with observed sentiment.
- Constituent contribution is `weight × observed sentiment` and is additive.
- The S&P index price uses a true index source such as `^GSPC`; SPY is not an index-level substitute.
- `sentiment_observed` distinguishes fresh observations from any display carry-forward value.
- Portfolio calculations must not be silently changed by UI work.

## Free-public source policy

Production company intelligence uses **free, publicly accessible sources only**.

- Market/price history: Yahoo Finance public market data via yfinance
- Company news discovery: Yahoo public finance surfaces + Google News RSS
- Sentiment model: ProsusAI/FinBERT
- Earnings/call discovery: free public transcript pages, public Yahoo/Motley Fool discovery, company/public links and SEC EDGAR evidence
- Constituent metadata: public S&P constituent tables/Wikipedia with yfinance metadata fallback

Paid/Premium news or transcript feeds are not production dependencies.

Third-party transcript body text may be processed transiently to derive speaker/section sentiment, uncertainty, forward-looking language and topics. Public artifacts retain **derived diagnostics and source URLs, not the transcript body text**.

## Company fulfillment artifacts

Key public resources under `apps/web/public/data/v5/`:

- `universe.json` — Composite 1500 company layer
- `news/{SYMBOL}.json` — retained free-public news
- `history/{SYMBOL}.json` — extended daily price + observed sentiment history
- `company_data_coverage.json` — news/history readiness across the universe
- `earnings/{SYMBOL}.json` — EPS history, structured call diagnostics where available, source links, filings
- `earnings_coverage.json` — structured-call coverage audit
- `event_instances.json` — persistent clustered event instances

Dedicated fulfillment workflows progressively fill missing news/history and earnings-call coverage rather than representing unavailable evidence as zero.

## Research Lab V3

Research Lab remains on the S&P 500 core and uses fresh observed sentiment only.

- Signals: sentiment level, observed sentiment change, sentiment/price divergence
- Horizons: 1D / 3D / 5D / 20D
- Quantiles: 20% / 25% / 33%
- Samples: full / first 70% / last 30%
- Newey-West/Bartlett HAC inference with lag `horizon - 1`
- Turnover and transaction-cost sensitivity
- Reproducible specification IDs and downloadable results

## Run locally

```bash
pip install -r requirements.txt
cd apps/web
npm ci
npx tsc --noEmit
npm run build
```

## CI and deployment

- Frontend PR CI: `npm ci` → `npx tsc --noEmit` → `npm run build`
- Python source CI: compile + deterministic regression/source-policy guards
- Main production site/data deployment: `.github/workflows/pipeline.yml`
- Extended company refresh, earnings fulfillment and company-data fulfillment run separately so they do not alter S&P core semantics.

## Research caveats

The current company layer is a broad current-universe evidence product, not a fully reconstructed historical point-in-time membership dataset. Free-public news/transcript availability varies by company and date. Event history is bounded by collected evidence. Research statistics and backtests are diagnostics rather than causal estimates or investment recommendations.
