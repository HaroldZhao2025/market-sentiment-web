# PROJECT MEMORY — Sentiment Intelligence

> **Read this file before making any change to this repository.**
>
> This is the persistent handoff for ChatGPT, Codex, Claude, future contributors, and the project owner. Current `main` is always the source of truth when this file conflicts with older conversation history.

**Repository:** `HaroldZhao2025/market-sentiment-web`  
**Live site:** `https://haroldzhao2025.github.io/market-sentiment-web/`  
**Last updated:** 2026-08-12  
**Current product phase:** Phase 3 Intelligence Engine completed; next product work begins from Phase 4 / targeted engineering hardening.

---

## 1. Product identity

The product is **Sentiment Intelligence**: a transparent, auditable market-intelligence and empirical-research platform built around S&P 500 news sentiment, price reactions, constituent attribution, event intelligence, signal screening, portfolio research, and empirical studies.

Long-run principle:

> Do not compete with LLMs by generating generic financial prose. Build proprietary, deterministic, auditable market data, event intelligence, screens, research, and backtests that LLMs and agents can consume.

The product should answer:

1. What is the current market sentiment?
2. What changed?
3. Why did it change?
4. Which stocks, industries, and sectors drove the move?
5. Where is sentiment diverging from price?
6. Has this type of event happened before?
7. Does the signal predict anything historically?
8. Can users test their own hypotheses?

---

## 2. Authorship

Read authorship from `README.md`; do not guess or overwrite it.

Current attribution:

- **Portfolio Strategy:** `leolin0407-cmyk` — `leolin0407@gmail.com`
- **Market Sentiment, Website Design, Repo Setting:** `HaroldZhao2025` — `stevenfinch2022@outlook.com`

The Portfolio page visibly credits:

**Portfolio Strategy by leolin0407-cmyk**

---

## 3. Current product surfaces

Main routes on current `main`:

- `/` — market overview / market state
- `/ticker/<symbol>` — ticker intelligence, article evidence, event drivers
- `/screener` — cross-sectional Market Screener / Signal Explorer
- `/sp500` — S&P 500 index state and clustered constituent bubble map
- `/attribution` — company → industry → sector → index attribution
- `/events` — Historical Event Memory / deterministic event intelligence
- `/lab` — Interactive Research Lab
- `/portfolio` — portfolio strategy and benchmark research
- `/research` — empirical research library
- `/research/<slug>` — individual empirical studies
- `/data` — machine-readable data contracts
- `/methodology` — methodology, definitions, caveats

Global product name: **Sentiment Intelligence**.

---

## 4. Core data semantics — do not break these

### 4.1 Article sentiment

News comes primarily from Finnhub and Yahoo/yfinance sources in the current production pipeline.

Cross-provider duplicate headlines are deduplicated before aggregation.

FinBERT article score:

`article_sentiment = P(positive) - P(negative)`

Article-level records should preserve title, URL, source/provider, timestamp, article score `s`, sentiment label, and probability outputs when available.

If ticker UI displays headline sentiment, the underlying article-level score must exist; do not silently discard it upstream.

### 4.2 No-news semantics

**No news is missing, not neutral zero.**

A true neutral score near zero and an unobserved ticker/day are different states.

Do not coerce null/missing sentiment to `0.0` for convenience.

### 4.3 Ticker daily sentiment

Ticker-day sentiment is the equal-weight mean of unique scored article sentiment for that ticker/day.

### 4.4 S&P cap-weighted sentiment

Only constituents with actual observed sentiment belong in the denominator:

`S_t = sum(w_i * s_i for observed i) / sum(w_i for observed i)`

Diagnostics should preserve market-cap coverage, observed ticker count, ticker coverage, and unique-news count.

### 4.5 Contribution

Constituent contribution is deterministic and additive:

`contribution_i = constituent_weight_i * observed_sentiment_i`

Industry and sector contributions are sums of constituent contribution. Group sentiment is a separate renormalized statistic over observed weight within the group.

### 4.6 Price data

S&P 500 index level uses `^GSPC` or equivalent true index source. **Do not use SPY as an S&P index-level price substitute.**

`yfinance` end dates are exclusive and MultiIndex columns must be normalized robustly.

---

## 5. Portfolio strategy safeguards

Portfolio rules currently intended include:

- weekly rebalance on the last trading day
- signal observed with lag
- execute on the next trading day
- sentiment level/change blended with momentum/volatility where configured
- inverse-volatility / risk-aware sizing
- max individual weight constraints
- selection buffer to reduce turnover
- transaction costs
- bounded gross exposure
- correct exits; do not resurrect sold positions through zero→NaN forward-fill bugs
- robust handling of missing/corrupt prices
- risk/performance metrics such as Sharpe, drawdown, turnover and related diagnostics

Do not silently change portfolio calculations during UI work.

Do not relabel a simple sentiment moving average as a literal predicted return / Buy / Sell forecast.

---

## 6. Completed phases

### Phase 1 — completed

Added the Sentiment Intelligence product shell, market-state homepage, ticker signal explorer, Data/Methodology pages, and frontend PR CI foundation.

### Phase 2 — completed via PR #2

Added:

- removal of unwanted generic marketing blocks
- ticker **Why sentiment changed**
- deterministic event-theme grouping
- positive/negative headline drivers
- removal of misleading Predicted Return / Buy / Sell presentation
- S&P contribution attribution
- Contribution / Sentiment / 1D Return modes
- coverage / observed ticker / unique-news evidence
- dark product styling improvements

### Phase 3 — Intelligence Engine — completed via PR #3 + validation/completion PR #4

Phase 3 added the following product and methodology upgrades.

#### 6.3.1 Market Screener

Route: `/screener`

Implemented deterministic cross-sectional screening across:

- ticker / company
- sector / industry
- constituent weight
- price and 1D return
- observed sentiment
- sentiment change
- sentiment-price divergence
- dominant deterministic event theme
- event novelty
- source breadth
- sentiment disagreement
- unique-news evidence

Missing sentiment remains missing and can be filtered explicitly.

#### 6.3.2 Interactive Research Lab

Route: `/lab`

Signals:

- sentiment level
- sentiment change
- sentiment-price divergence

Forward horizons:

- 1D
- 3D
- 5D
- 20D

Universes:

- All
- sector-specific subsets

Quantile choices:

- top/bottom 20%
- top/bottom 25%
- top/bottom 33%

**Important corrected methodology:**

The Lab does **not** pool all stock-dates into one quantile sort. For each trading date it:

1. selects the available observed cross-section,
2. ranks stocks by the chosen signal,
3. forms equal-weight high- and low-signal groups,
4. computes each group's forward return,
5. computes that date's high-minus-low spread,
6. summarizes the resulting daily spread series.

Diagnostic outputs include:

- mean high-signal return
- mean low-signal return
- mean long-short spread
- simple t-stat across daily spreads
- hit rate
- horizon-adjusted diagnostic Sharpe
- stock-day observation count
- valid cross-sectional date count
- sample range

Overlapping forward horizons can create serial dependence. The displayed t-stat and Sharpe are diagnostics, **not publication-grade causal inference**. Formal research should add robust/clustered inference, transaction costs where relevant, and out-of-sample validation.

Current divergence diagnostic:

`observed sentiment - clip(1D return / 0.05, -1, 1)`

#### 6.3.3 Historical Event Memory

Route: `/events`

Uses retained scored article history to classify deterministic event themes and attach observed price reactions.

Current richer theme family includes concepts such as:

- Earnings beat / miss
- Guidance & outlook
- Product & AI
- M&A & strategic deals
- Capital return & financing
- Regulation & antitrust
- Legal & litigation
- Management change
- Operations & demand
- Analyst action

Diagnostics include:

- event count
- ticker count
- source count
- average article sentiment
- average 1D / 5D price reaction
- positive 1D reaction rate
- headline novelty
- sentiment disagreement
- recent retained examples

Event Memory is bounded by retained ticker JSON article history; it is not a complete historical news database.

#### 6.3.4 S&P Attribution V2

Route: `/attribution`

Implemented company → industry → sector → index contribution decomposition.

Group contribution sums constituent `weight × observed sentiment`. Group sentiment renormalizes over observed weight inside the group.

#### 6.3.5 S&P clustered bubble map

`apps/web/app/sp500/Sp500HeatmapClient.tsx` no longer uses the rectangular binary treemap.

Current visualization:

- packed/clustered stock circles
- bubble area approximately reflects constituent weight
- sector attraction creates organic clusters
- modes: Contribution / Sentiment / 1D Return
- major-bubble labels
- hover tooltip with ticker, company, sector, industry, weight, price, 1D return, sentiment, contribution, unique-news count
- click bubble → ticker page
- dependency-light deterministic layout; no D3 dependency added

#### 6.3.6 UI completion

Completed outstanding UI work:

- ticker latest price uses green/red semantic color according to latest return
- Screener price uses the same semantic direction color
- Portfolio chart is natively dark; explicit white panel/light SVG background removed
- Portfolio Strategy author credit is visible
- Research shell/studies use stronger hierarchy and dedicated dark academic typography
- README now identifies the product as Sentiment Intelligence and documents Phase 3

#### 6.3.7 Intelligence code organization

New Phase 3 deterministic application logic lives in normal source code:

`apps/web/lib/intelligence.ts`

It contains shared event taxonomy, screener metrics, Research Lab calculations, Event Memory calculations, and attribution logic. Ticker/heatmap file reads are cached during static generation to avoid thousands of repeated reads.

**Legacy production-pipeline note:** `.github/workflows/pipeline.yml` still contains substantial older embedded Python heredoc logic. Do not add new Phase 3 business logic there. A broader extraction of legacy production scripts into normal Python modules should be handled as a dedicated infrastructure refactor with production-data regression validation rather than mixed casually into a UI/product PR.

---

## 7. Frontend validation contract

PR #4 corrected the frontend CI gate.

`.github/workflows/frontend-ci.yml` now:

1. sparse-checks out only `apps/web` to avoid downloading the entire ~large data repository for a frontend PR,
2. runs `npm ci --no-audit --no-fund`,
3. runs `npx tsc --noEmit`,
4. runs strict `npm run build`.

Do not restore `npm run build || true`.

Latest Phase 3 completion PR validation passed both TypeScript and Next.js production build before merge.

---

## 8. Current UI / design rules

Visual direction: dark, clean **market-intelligence terminal**, not a light student-dashboard aesthetic.

Preferred:

- dark neutral surfaces
- subtle borders
- low-noise hierarchy
- readable metrics and research tables
- restrained green/red semantic color
- charts visually integrated with the page
- deterministic explanations rather than generic finance prose

Semantic market colors:

- positive / rising price: green
- negative / falling price: red
- flat / missing: neutral gray

Avoid:

- white chart panels inside dark pages
- giant generic marketing blocks
- excessive gradients/glows
- tiny weak research typography
- rectangular S&P treemap layouts
- hiding all important information behind hover

---

## 9. Current generated/public data family

Important generated artifacts include variants of:

- `apps/web/public/data/_tickers.json`
- `apps/web/public/data/ticker/<SYMBOL>.json`
- `apps/web/public/data/SPX/sp500_index.json`
- `apps/web/public/data/SPX/sp500_heatmap.json`
- `apps/web/public/data/portfolio_strategy.json`
- `apps/web/public/research/index.json`
- research overview / study JSON artifacts

Stable data contracts are preferred to runtime React source patching.

---

## 10. Known historical failure modes — never reintroduce

### Build suppression

Never use:

`npm run build || true`

### Runtime frontend rewriting

Do not dynamically rewrite React/TypeScript source during the production workflow.

### Sentiment version mismatch

Do not duplicate incompatible algorithm-version constants across independent scripts. Current production env uses the shared `SENTIMENT_ALGORITHM_VERSION` contract.

### Missing → zero

Never convert absent news/sentiment into neutral zero simply for aggregation or display convenience.

### Article sentiment dropped

If the UI displays headline sentiment, article-level inference must remain available in the generated data/cache.

### SPX source inconsistency

Do not let stale root/public S&P files conflict, and never substitute SPY scale for the S&P index level.

### Portfolio ghost holdings

Do not convert explicit zero portfolio weights to NaN before forward fill; that can resurrect sold positions.

### Research static routes

Static export requires valid `generateStaticParams()` behavior even in clean frontend CI where production research artifacts may be absent.

---

## 11. Recommended next phase — Phase 4

Phase 4 should add an LLM/agent interface **on top of**, not instead of, the deterministic engine.

High-value directions:

1. **Ask the Market**
   - natural-language question → structured deterministic query
   - answer should cite the underlying ticker/event/screen/research evidence

2. **Natural-language Screener**
   - parse user constraints into the existing deterministic Screener fields

3. **API / agent interface**
   - structured endpoints for market state, screener results, event memory, attribution, and research-lab specifications
   - suitable for agent/MCP consumption

4. **Historical Event Memory V2**
   - persist a deeper historical event store instead of relying only on the latest retained ticker snapshot
   - richer event-instance IDs and provider consensus

5. **Research Lab V2**
   - Newey-West / clustered inference where appropriate
   - transaction-cost controls
   - point-in-time universe/weights where available
   - out-of-sample windows
   - downloadable specification/results

6. **Watchlists / alerts / personalization** after the deterministic API layer is stable.

Engineering hardening can proceed in parallel, especially a dedicated migration of legacy embedded Python from `pipeline.yml` into tested `src/market_sentiment` modules.

---

## 12. Development workflow

For UI/product work:

1. read this file
2. read current `README.md`
3. inspect actual latest `main`
4. create a clean branch from latest `main`
5. make real file-level changes
6. keep business logic in normal source files
7. open a focused PR
8. require Frontend CI typecheck + Next build success
9. merge only after validation

For production pipeline changes:

- keep changes narrow
- preserve strict build/deploy gates
- validate shell and Python syntax
- execute critical builders against representative/synthetic fixtures when possible
- compare generated data contracts before/after
- do not combine a giant pipeline migration with unrelated UI work

---

## 13. Communication preferences for this project

The owner prefers:

- discussion mainly in Chinese
- code and code comments in English
- direct implementation instead of long speculative checklists
- GitHub-connected changes when possible
- fewer manual upload/retry loops
- clear root-cause explanations
- no claims of success before CI/build/data validation

If the owner says “直接改”, make the changes rather than returning only a proposal.

---

## 14. First instruction to any new AI / contributor

Before acting:

1. Read this entire file.
2. Read current `README.md`.
3. Inspect current `main` and relevant generated data contracts.
4. Treat `main` as source of truth over old conversation history.
5. Preserve no-news ≠ zero, article evidence, deterministic contribution, true SPX pricing, and portfolio safeguards.
6. Treat Phase 1, Phase 2, and Phase 3 as already completed unless current `main` proves otherwise.
7. Continue from Phase 4 or from a newer instruction given by the owner.
