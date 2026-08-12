# PROJECT MEMORY — Sentiment Intelligence

> **Read this file before making any change to this repository.**
>
> This is the persistent handoff / memory for ChatGPT, Codex, Claude, future contributors, and the project owner. It records product intent, architecture, data semantics, authorship, completed phases, known failure modes, UI rules, and the current next tasks.

**Repository:** `HaroldZhao2025/market-sentiment-web`  
**Live site:** `https://haroldzhao2025.github.io/market-sentiment-web/`  
**Last updated:** 2026-08-12

---

## 1. Product identity

The project is evolving from a simple **Market Sentiment Website** into **Sentiment Intelligence**: a transparent, auditable market-intelligence and empirical-research platform built around S&P 500 news sentiment, price reactions, portfolio signals, and research artifacts.

The long-run product principle is:

> Do not compete with LLMs by generating generic financial prose. Build proprietary, auditable, queryable market intelligence that LLMs and agents can consume.

The site should answer increasingly valuable questions:

1. What is the current market sentiment?
2. What changed?
3. Why did it change?
4. Which stocks or sectors drove the move?
5. Where is sentiment diverging from price?
6. Has this type of event happened before?
7. Does the signal predict anything historically?
8. Can users test their own hypotheses?

---

## 2. Authorship

Read authorship from the repository README; do not guess or overwrite it.

Current README attribution:

- **Portfolio Strategy:** `leolin0407-cmyk` — `leolin0407@gmail.com`
- **Market Sentiment, Website Design, Repo Setting:** `HaroldZhao2025` — `stevenfinch2022@outlook.com`

The Portfolio webpage should visibly credit **leolin0407-cmyk** as the Portfolio Strategy author.

---

## 3. Main product surfaces

Current main routes:

- `/` — market overview / market state
- `/ticker/<symbol>` — ticker intelligence
- `/sp500` — S&P 500 sentiment, index state, constituent intelligence
- `/portfolio` — sentiment-driven portfolio strategy and benchmarks
- `/research` — empirical research index
- `/research/<slug>` — individual empirical studies
- `/data` — machine-readable data contracts
- `/methodology` — methodology and caveats

The product name shown in the global shell is **Sentiment Intelligence**.

---

## 4. Core data semantics — do not break these

### 4.1 Article sentiment

News comes primarily from Finnhub and Yahoo/yfinance sources in the current production pipeline.

Cross-provider duplicate headlines are deduplicated before sentiment aggregation.

FinBERT article score is:

`article_sentiment = P(positive) - P(negative)`

Article-level records should preserve at least:

- title
- URL
- source/provider
- timestamp
- `s` article sentiment
- `sentiment_label`
- positive / neutral / negative probabilities when available

Displayed ticker headlines should not silently show `-` because an article score was discarded upstream. If a displayed headline is intended to show sentiment, the data contract must contain a real article-level score.

### 4.2 No-news semantics

**No news is missing, not neutral zero.**

Do not convert a missing sentiment observation into `0.0` merely to simplify frontend code or index aggregation.

A true neutral score near zero and an unobserved day are conceptually different.

### 4.3 Ticker daily sentiment

Ticker-day sentiment is the equal-weight mean of unique scored article sentiment for that ticker/day.

### 4.4 S&P cap-weighted sentiment

Only tickers with actual observed sentiment belong in the cap-weighted denominator:

`S_t = sum(w_i * s_i for observed i) / sum(w_i for observed i)`

Do not include fake zeros for no-news tickers.

Important diagnostics should remain visible or available:

- `sentiment_cap_weighted`
- `sentiment_equal_weighted`
- market-cap coverage
- observed ticker count
- ticker coverage
- unique news count

### 4.5 Contribution

Ticker contribution to index sentiment is deterministic:

`contribution_i = constituent_weight_i * observed_sentiment_i`

Missing sentiment is not contribution zero in the conceptual model; it is missing evidence.

### 4.6 Price data

S&P 500 index level should use `^GSPC` or equivalent true index source. Do not use SPY as a fallback for the index level because its scale is different.

`yfinance` end dates are exclusive.

`yfinance` may return MultiIndex columns; builders must normalize them robustly.

---

## 5. Portfolio strategy — important implementation notes

The portfolio implementation was upgraded from the earlier fragile version.

Key rules currently intended:

- weekly rebalance on the last trading day
- signal observed with lag
- execute on the next trading day to reduce look-ahead bias
- blend sentiment level/change with momentum and volatility where configured
- inverse-volatility / risk-aware sizing
- max individual weight constraints
- selection buffer to reduce turnover
- transaction costs
- gross exposure bounded
- correct exits (do not resurrect sold positions by converting zero weights to NaN before forward fill)
- safer handling of missing/corrupt prices
- metrics include Sharpe, Sortino/Calmar where available, drawdown, turnover, etc.

Do not reintroduce the old “ghost holdings” bug.

Do not present a simple sentiment moving average as a literal **Predicted Return / Buy / Sell** forecast unless there is a real predictive model supporting that claim.

The Portfolio page should explicitly credit:

**Portfolio Strategy by leolin0407-cmyk**

---

## 6. Completed product phases

### Phase 1 — merged into `main`

Phase 1 repositioned the project as a market-intelligence product.

Completed work included:

- global dark product shell under **Sentiment Intelligence**
- homepage organized around Market State / What Changed / Attention / Explore
- robust frontend missing-value semantics
- ticker search and signal explorer
- positive / negative / divergence screening
- sentiment-change sorting
- Data page documenting public JSON contracts
- Methodology page
- lightweight frontend PR CI using TypeScript typecheck

### Phase 2 — merged into `main` via PR #2

PR #2 title: **Phase 2: event intelligence and constituent attribution**

Completed work included:

- removed the unwanted “Agent-ready direction” marketing block
- ticker **Why sentiment changed** section
- deterministic event-theme grouping over scored headlines
- positive and negative headline drivers
- removal of misleading Predicted Return / Buy / Sell presentation
- S&P constituent contribution attribution
- Contribution / Sentiment / 1D Return modes
- S&P coverage, observed ticker count, unique-news evidence
- Portfolio / Research visual alignment toward the dark product shell
- dark compatibility layer for legacy components

Current deterministic ticker theme groups include concepts such as:

- Earnings & guidance
- Product & AI
- Regulation & legal
- Deals & capital
- Operations & demand
- Analyst & market

These are deterministic keyword-group explanations, not LLM-generated narratives.

---

## 7. Current UI / design rules

The visual direction is a dark, clean **market intelligence terminal**, not a light student-dashboard aesthetic.

Preferred design characteristics:

- dark neutral surfaces
- subtle borders
- low-noise hierarchy
- readable metrics
- restrained green/red semantic color
- minimal decorative marketing blocks
- charts should visually belong to the page rather than appear as white embedded images

### Semantic colors

For market values:

- positive / rising price: green
- negative / falling price: red
- missing / neutral: neutral gray

Price itself should visually reflect the latest price move where appropriate, not always remain white.

### Avoid

- white chart background panels inside dark pages
- large generic marketing statements that do not help analysis
- excessive gradients or glows
- tiny / weak research typography
- visual components that look pasted in from an older design system

---

## 8. Current next tasks — highest priority

These requests are explicitly outstanding as of 2026-08-12 and should be handled next.

### 8.1 Stock price coloring

On ticker pages and any relevant market summaries:

- if latest price return > 0: price should be green
- if latest price return < 0: price should be red
- if exactly zero or unavailable: neutral

This applies both to the return and, where visually appropriate, the latest price number itself.

### 8.2 Portfolio white chart background

The outer Portfolio page was darkened, but `apps/web/components/PortfolioChart.tsx` still explicitly renders:

- `bg-white`
- a white / light SVG gradient background
- light grid lines
- legacy dark text classes

Fix the chart component itself rather than relying only on a wrapper compatibility layer.

The chart should have a transparent/dark surface and visually integrate with the Portfolio page.

### 8.3 Research typography and dark styling

Research index and research study internals still contain many legacy `zinc-*`, `bg-white`, and small-font styles.

Important files:

- `apps/web/app/research/ResearchIndexClient.tsx`
- `apps/web/app/research/ResearchStudyClient.tsx`
- `apps/web/app/research/[slug]/page.tsx`

Upgrade typography and hierarchy:

- clearer title scale
- better body readability
- less tiny gray text
- dark cards/tables
- consistent font weight and spacing
- retain rigorous academic / empirical feel

Do not change the empirical results just to redesign the page.

### 8.4 S&P constituent visualization — replace rectangular treemap

The current `Sp500HeatmapClient.tsx` still uses a rectangular binary treemap.

The owner wants a **clustered bubble / packed-circle visualization**, not rectangles.

Desired behavior:

- each stock is a circle / bubble
- bubble area approximately reflects market cap or constituent weight
- bubbles appear as organic clusters / packed groups rather than a grid
- ideally cluster by sector while preserving an understandable overall map
- color modes remain:
  - Contribution
  - Sentiment
  - 1D Return
- hover should show a rich tooltip with:
  - ticker
  - company name
  - sector / industry
  - weight
  - price
  - 1D return
  - sentiment
  - contribution
  - unique news count
- clicking a bubble should navigate to the ticker page
- interaction should feel smooth and premium
- do not hide essential data behind hover only; labels for major bubbles are useful

Prefer a dependency-light implementation. If adding D3 hierarchy / pack materially improves robustness, evaluate bundle / static export impact first.

### 8.5 Portfolio author credit

Add visible authorship to the Portfolio page using the repository README as the source of truth:

**Portfolio Strategy by leolin0407-cmyk**

Optionally show the email if appropriate, but the author name is required.

---

## 9. Future roadmap after current UI fixes

### Phase 3 — Intelligence Engine

Recommended high-value next phase:

1. **Market Screener / Signal Explorer**
   - combine sector, market cap, sentiment, sentiment change, price return, news evidence, divergence
   - deterministic filters and sorting

2. **Interactive Research Lab**
   - selectable signal
   - horizon (1D / 3D / 5D / 20D)
   - universe / sector
   - long-short quantiles
   - controls
   - sample period
   - output coefficient/spread, t-stat, Sharpe, hit rate, drawdown, N

3. **Historical Event Memory**
   - map current event themes to historical comparable events
   - report historical price reactions / distributions

4. **Event Intelligence V2**
   - richer taxonomy: earnings beat/miss/guidance, product launch/delay, regulation, M&A, management, legal, analyst actions
   - novelty
   - source/provider consensus
   - disagreement
   - price reaction

5. **S&P Attribution V2**
   - Company → Industry → Sector → Index decomposition

6. **Pipeline modularization**
   - move business logic out of giant embedded GitHub Actions Python blocks into real Python modules
   - workflow should orchestrate, not contain the application

### Phase 4 — LLM / agent layer

Only after the deterministic engine is strong:

- Ask the Market
- natural-language screener
- structured query conversion
- API endpoints
- agent/MCP interfaces

LLM should parse/explain; deterministic code should compute facts and backtests.

### Phase 5 — personalization

- watchlists
- saved screens
- saved strategies
- alerts
- “since your last visit” intelligence
- portfolio monitoring

---

## 10. Public / generated data contracts

Important generated artifacts include variants of:

- `apps/web/public/data/_tickers.json`
- `apps/web/public/data/ticker/<SYMBOL>.json`
- `apps/web/public/data/SPX/sp500_index.json`
- `apps/web/public/data/SPX/sp500_heatmap.json`
- `apps/web/public/data/portfolio_strategy.json`
- `apps/web/public/research/index.json`
- research overview / study JSON artifacts

The website should consume stable contracts rather than repeatedly patching React code to accommodate inconsistent JSON.

---

## 11. Pipeline history and known failure modes

This project experienced repeated failures because too much business logic was embedded directly inside `.github/workflows/pipeline.yml`.

Important historical bugs / lessons:

### Do not suppress builds

Never restore:

`npm run build || true`

Build failures must fail the workflow.

### Static Research routes

Next static export needs valid `generateStaticParams()` for `/research/[slug]`.

Research artifacts must exist before the Next build when production slugs are expected.

### Sentiment cache version mismatch

A previous failure occurred because the sentiment generator wrote V3 while a downstream site builder still hard-coded V2.

Preferred rule:

- one shared algorithm-version source / environment contract
- do not duplicate version strings across embedded scripts

### Missing Python imports

A previous research builder used `os.environ` but forgot `import os`.

Static compile alone is not always enough; execute critical builders against synthetic fixtures when modifying them.

### Frontend TypeScript runtime patching

A previous pipeline dynamically rewrote `page.tsx` and produced a type error comparing `number` to `""`.

Do not mutate core React/TypeScript source at production runtime.

Frontend source changes belong in the repository and should pass PR CI.

### S&P root/public precedence

Historically the homepage could read a root `data/SPX/sp500_index.json` before the final public aggregate, causing price/sentiment inconsistencies.

Data contracts should now be explicit and consistent. Avoid dual inconsistent copies.

### Article sentiment dropped from cache

An earlier V2 aggregation retained only ticker-day means and discarded article-level sentiment. Ticker headline sentiment therefore displayed `-`.

Do not discard article-level inference if the UI needs article evidence.

---

## 12. Development workflow

Preferred workflow for UI/product upgrades:

1. read `PROJECT_MEMORY.md`
2. inspect the actual current `main` files
3. create a clean branch from latest `main`
4. make real file-level changes
5. avoid runtime source patching in pipeline YAML
6. open a focused PR
7. let Frontend CI run TypeScript checks
8. inspect CI result before claiming success
9. merge only after review / validation

For production pipeline changes:

- keep changes narrow
- validate shell and Python syntax
- execute important scripts with representative/synthetic data if possible
- preserve strict build/deploy gates

---

## 13. Communication preferences for this project

The owner prefers:

- direct implementation rather than long speculative upgrade checklists
- fewer manual upload / retry loops
- GitHub-connected changes when possible
- clear explanations of real root causes
- no claims of “100% guaranteed” without evidence
- Chinese is generally preferred for discussion
- code and code comments should remain in English

If the owner says “直接改”, make the changes rather than returning only a proposal.

---

## 14. First instruction to any new AI / contributor

Before acting:

1. Read this entire file.
2. Read the current `README.md`.
3. Inspect current `main`, because this memory may lag new commits.
4. Treat `main` and current generated data contracts as the source of truth when they conflict with older conversation history.
5. Preserve the core semantics: no-news ≠ zero; contribution is auditable; article evidence remains traceable; portfolio calculations should not be silently changed during UI work.

Then continue from **Current Next Tasks** unless the owner gives a newer instruction.
