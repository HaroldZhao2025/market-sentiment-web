# PROJECT MEMORY — Sentiment Intelligence

> **Read this file before making any change to this repository.**
>
> This is the persistent handoff for ChatGPT, Codex, Claude, future contributors, and the project owner. Current `main` is always the source of truth when this file conflicts with older conversation history.

**Repository:** `HaroldZhao2025/market-sentiment-web`  
**Live site:** `https://haroldzhao2025.github.io/market-sentiment-web/`  
**Last updated:** 2026-08-12  
**Current product phase:** Phase 1–3 complete; Phase 4 Query Layer started via PR #6.

---

## 1. Product identity

The product is **Sentiment Intelligence**: a transparent, auditable market-intelligence and empirical-research platform built around S&P 500 news sentiment, price reactions, constituent attribution, event intelligence, signal screening, natural-language querying, portfolio research, and empirical studies.

Long-run principle:

> Do not compete with LLMs by generating generic financial prose. Build proprietary, deterministic, auditable market data, event intelligence, screens, research, and backtests that LLMs and agents can consume.

The product should answer:

1. What is the current market sentiment?
2. What changed and why?
3. Which stocks, industries, and sectors drove the move?
4. Where is sentiment diverging from price?
5. Has this event type happened before?
6. Does the signal predict anything historically?
7. Can users test their own hypotheses?
8. Can users ask these questions in natural language without sacrificing auditability?

---

## 2. Authorship

Read authorship from `README.md`; do not guess or overwrite it.

- **Portfolio Strategy:** `leolin0407-cmyk` — `leolin0407@gmail.com`
- **Market Sentiment, Website Design, Repo Setting:** `HaroldZhao2025` — `stevenfinch2022@outlook.com`

The Portfolio page must visibly credit:

**Portfolio Strategy by leolin0407-cmyk**

---

## 3. Current product surfaces

Main routes on current `main`:

- `/` — market overview / market state
- `/ask` — **Ask the Market**, deterministic natural-language query layer
- `/ticker/<symbol>` — ticker intelligence, article evidence, event drivers
- `/screener` — cross-sectional Market Screener / Signal Explorer
- `/sp500` — S&P 500 index state and user-selectable **Treemap / Bubbles** constituent views
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

### Article sentiment

News comes primarily from Finnhub and Yahoo/yfinance in the current production pipeline. Cross-provider duplicate headlines are deduplicated before aggregation.

FinBERT article score:

`article_sentiment = P(positive) - P(negative)`

Preserve title, URL, source/provider, timestamp, article score `s`, sentiment label, and probability outputs where available.

### No-news semantics

**No news is missing, not neutral zero.**

Do not coerce null/missing sentiment to `0.0` for convenience.

### Ticker daily sentiment

Ticker-day sentiment is the equal-weight mean of unique scored article sentiment for that ticker/day.

### S&P cap-weighted sentiment

Only constituents with observed sentiment belong in the denominator:

`S_t = sum(w_i * s_i for observed i) / sum(w_i for observed i)`

Keep market-cap coverage, observed ticker count, ticker coverage, and unique-news count beside the aggregate.

### Contribution

`contribution_i = constituent_weight_i * observed_sentiment_i`

Industry and sector contributions sum constituent contribution. Group sentiment is a separate statistic renormalized over observed weight inside the group.

### Price data

S&P index level uses `^GSPC` or another true index source. **Never use SPY as an S&P index-level price substitute.**

`yfinance` end dates are exclusive and MultiIndex columns must be normalized robustly.

---

## 5. S&P constituent metadata contract

PR #6 fixed a recurring company-name problem in `src/market_sentiment/cli/build_sp500_heatmap.py`.

The previous builder could reuse the Wikipedia constituent cache indefinitely. New or changed S&P constituents could therefore miss `Security` metadata and publish with blank company names.

Current contract:

- Wikipedia S&P constituent metadata cache has a default **7-day TTL**.
- When stale, refresh the constituent table.
- If refresh fails, stale cache may be used rather than destroying a working production build.
- Missing company names fall back to yfinance `longName`, then `shortName`.
- Missing sector/industry may fall back to yfinance metadata.
- Ticker variants such as `BRK.B` / `BRK-B` must map correctly.
- Heatmap stats record `missing_name`, `missing_classification`, and `yfinance_name_fallbacks`.
- The builder fails if company-name coverage falls below **98%**; do not silently publish a large set of nameless companies.
- Final tile `name` has a ticker fallback only as the last defensive display fallback.

Do not move this logic into React or patch names manually in the UI.

---

## 6. Portfolio strategy safeguards

Do not silently change portfolio calculations during UI/product work.

Important intended rules include:

- weekly rebalance on the last trading day
- lagged signal / next-trading-day execution
- configured sentiment + momentum/volatility logic
- inverse-volatility / risk-aware sizing
- max individual weight constraints
- selection buffer
- transaction costs
- bounded gross exposure
- correct exits; do not resurrect sold positions through zero→NaN forward-fill bugs
- robust missing/corrupt price handling
- Sharpe, drawdown, turnover and related diagnostics

Do not relabel a simple sentiment moving average as a literal predicted return / Buy / Sell forecast.

---

## 7. Completed phases

### Phase 1 — completed

Sentiment Intelligence shell, market-state homepage, ticker explorer, Data/Methodology pages, frontend CI foundation.

### Phase 2 — completed via PR #2

Added deterministic **Why sentiment changed**, positive/negative article drivers, S&P contribution attribution, Contribution/Sentiment/1D Return modes, coverage/evidence diagnostics, and dark product styling.

### Phase 3 — Intelligence Engine — completed via PR #3 + validation PR #4

Added:

- `/screener` Market Screener
- `/lab` Interactive Research Lab
- `/events` Historical Event Memory
- `/attribution` company → industry → sector → index decomposition
- richer deterministic event taxonomy
- novelty, source breadth, disagreement diagnostics
- dark Portfolio chart and visible portfolio authorship
- stronger Research typography
- semantic green/red prices
- shared deterministic application logic in `apps/web/lib/intelligence.ts`

#### Research Lab methodology

Do **not** pool all stock-dates into one quantile sort. For each trading date:

1. select the available observed cross-section,
2. rank by signal,
3. form equal-weight high- and low-signal groups,
4. compute each group's forward return,
5. compute that date's high-minus-low spread,
6. summarize the daily spread series.

Current signals: sentiment level, sentiment change, sentiment-price divergence. Horizons: 1D / 3D / 5D / 20D. Quantiles: 20% / 25% / 33%.

Overlapping horizons create serial dependence. Displayed t-stat and Sharpe are descriptive diagnostics, not publication-grade causal inference.

Current divergence diagnostic:

`observed sentiment - clip(1D return / 0.05, -1, 1)`

#### Event Memory limitation

Current Event Memory is bounded by retained ticker JSON article history. It is not yet a complete historical event database.

---

## 8. Phase 4 — Query Layer — started via PR #6

### 8.1 Ask the Market

Route: `/ask`

New deterministic query layer:

- user asks a natural-language question,
- `apps/web/lib/marketQuery.ts` maps the language to explicit filters/rankings,
- execution runs over the existing deterministic Screener rows,
- UI exposes the parsed query plan,
- result table links back to underlying ticker evidence.

Supported concepts currently include:

- sector filters
- ticker filters
- event-theme filters
- positive/negative sentiment
- improving/deteriorating sentiment
- positive/negative 1D return
- sentiment-price divergence
- largest positive/negative contributors
- highest/lowest sentiment
- index weight
- news attention
- novelty
- disagreement
- result count limits
- minimum news evidence

**Important:** Ask the Market does not currently call an LLM. Natural language is a deterministic parser over known fields. Do not replace underlying calculations with generated prose.

### 8.2 Dual S&P visualization

`apps/web/app/sp500/Sp500HeatmapClient.tsx` supports both:

- **Treemap** — default, because the owner prefers the classic rectangular market-map view.
- **Bubbles** — clustered organic alternative.

Users can switch freely. Both views share:

- sector filtering
- Contribution / Sentiment / 1D Return modes
- area approximately proportional to constituent weight
- semantic green/red color
- hover: ticker, company, sector, industry, weight, price, 1D return, sentiment, contribution, unique-news count
- click → ticker page

Do **not** remove either view unless explicitly requested. The old rule “avoid rectangular S&P treemap layouts” is obsolete.

### 8.3 Next Phase 4 priorities

High-value continuation:

1. structured API / agent interface for market state, screener, events, attribution, and research specifications
2. Event Memory V2 with a deeper persisted historical event store and event-instance IDs
3. Research Lab V2 with Newey-West/clustered inference, transaction costs, OOS controls, downloadable specs/results
4. optional LLM layer that parses broader language or explains deterministic outputs **with evidence references**
5. watchlists / alerts / personalization after the deterministic API layer is stable

---

## 9. UI / design rules

Visual direction: dark, clean **market-intelligence terminal**, not a light student-dashboard aesthetic.

Preferred:

- dark neutral surfaces
- subtle borders
- low-noise hierarchy
- readable metrics and research tables
- restrained green/red semantic color
- charts integrated with the page
- deterministic explanations rather than generic finance prose
- Treemap as the default S&P constituent view, with Bubbles available as a user-selectable alternative

Semantic colors:

- positive / rising price: green
- negative / falling price: red
- flat / missing: neutral gray

Avoid:

- white chart panels inside dark pages
- giant generic marketing blocks
- excessive gradients/glows
- tiny weak research typography
- hiding all important information behind hover
- removing one S&P visualization mode when both can coexist

---

## 10. Validation contract

### Frontend CI

`.github/workflows/frontend-ci.yml`:

1. sparse checkout `apps/web`
2. `npm ci --no-audit --no-fund`
3. `npx tsc --noEmit`
4. strict `npm run build`

Never restore `npm run build || true`.

### Python source CI

PR #6 added `.github/workflows/python-source-ci.yml` for `src/market_sentiment/**` changes:

- sparse checkout Python source
- Python 3.11
- compile source with `compileall`
- deterministic helper tests for S&P metadata parsing, ticker variants, and cache freshness

PR #6 passed both the strict Frontend CI and Python Source CI before merge.

Production pipeline/data changes still require data-contract validation beyond syntax checks.

---

## 11. Current generated/public data family

Important generated artifacts include:

- `apps/web/public/data/_tickers.json`
- `apps/web/public/data/ticker/<SYMBOL>.json`
- `apps/web/public/data/SPX/sp500_index.json`
- `apps/web/public/data/SPX/sp500_heatmap.json`
- `apps/web/public/data/portfolio_strategy.json`
- `apps/web/public/research/index.json`
- research overview / study JSON artifacts

Stable data contracts are preferred to runtime source patching.

---

## 12. Known historical failure modes — never reintroduce

- **Build suppression:** never `npm run build || true`.
- **Runtime frontend rewriting:** do not dynamically rewrite React/TypeScript during production workflows.
- **Sentiment version mismatch:** do not duplicate incompatible algorithm-version constants.
- **Missing → zero:** never convert absent news/sentiment into neutral zero for aggregation convenience.
- **Article sentiment dropped:** headline UI requires retained article-level inference.
- **SPX source inconsistency:** never substitute SPY for index level.
- **Portfolio ghost holdings:** do not turn explicit zero weights into NaN before forward fill.
- **Stale constituent metadata:** do not reuse an unbounded Wikipedia cache; preserve metadata TTL/fallback/coverage checks.
- **Research static routes:** static export must work even when production research artifacts are absent in clean frontend CI.

---

## 13. Legacy production pipeline engineering debt

`.github/workflows/pipeline.yml` still contains substantial embedded Python heredoc logic.

Do not add new product/business logic there when normal source modules are appropriate. A full migration should be a dedicated infrastructure PR with representative-data regression validation, not mixed casually into UI work.

---

## 14. Development workflow

For UI/product work:

1. read this file and current `README.md`
2. inspect latest `main`
3. create a clean branch from `main`
4. make real file-level changes
5. keep deterministic business logic in normal source files
6. open a focused PR
7. require relevant CI to pass
8. merge only after validation

For production pipeline changes:

- keep changes narrow
- validate shell/Python syntax
- test critical builders against representative/synthetic fixtures
- compare generated data contracts before/after
- preserve strict build/deploy gates

---

## 15. Communication preferences for this project

The owner prefers:

- discussion mainly in Chinese
- code/comments in English
- direct implementation instead of long speculative checklists
- GitHub-connected changes when possible
- fewer manual upload/retry loops
- clear root-cause explanations
- no success claims before CI/build/data validation

If the owner says “直接改”, make the changes rather than returning only a proposal.

---

## 16. First instruction to any new AI / contributor

Before acting:

1. Read this entire file.
2. Read current `README.md`.
3. Inspect current `main` and relevant generated data contracts.
4. Treat `main` as source of truth over old conversation history.
5. Preserve no-news ≠ zero, article evidence, deterministic contribution, true SPX pricing, portfolio safeguards, and S&P metadata coverage checks.
6. Treat Phase 1–3 as complete and Phase 4 Query Layer as started unless current `main` proves otherwise.
7. Preserve both S&P visualization modes; Treemap is the current default.
8. Continue from the remaining Phase 4 priorities or a newer instruction from the owner.