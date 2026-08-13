# PROJECT MEMORY — Sentiment Intelligence

> **Read this file before making any change to this repository.** Current `main` is always the source of truth over older conversation history.

**Repository:** `HaroldZhao2025/market-sentiment-web`  
**Live site:** `https://haroldzhao2025.github.io/market-sentiment-web/`  
**Last updated:** 2026-08-12  
**Current product phase:** **Phase 1–4 completed.** Next work should begin from Phase 5 / targeted infrastructure hardening unless the owner gives a newer instruction.

---

## 1. Product identity

The product is **Sentiment Intelligence**: a transparent, auditable market-intelligence and empirical-research platform built around S&P 500 news sentiment, price reactions, constituent attribution, event intelligence, signal screening, natural-language querying, machine-readable agent contracts, portfolio research, and empirical studies.

Long-run principle:

> Do not compete with LLMs by generating generic financial prose. Build proprietary, deterministic, auditable market data, event intelligence, screens, research, and backtests that humans, LLMs, and agents can consume.

---

## 2. Authorship

Read authorship from `README.md`; do not guess or overwrite it.

- **Portfolio Strategy:** `leolin0407-cmyk` — `leolin0407@gmail.com`
- **Market Sentiment, Website Design, Repo Setting:** `HaroldZhao2025` — `stevenfinch2022@outlook.com`

Portfolio page must visibly show: **Portfolio Strategy by leolin0407-cmyk**.

---

## 3. Current routes

- `/` — market overview
- `/ask` — **Ask the Market**, deterministic natural-language query layer
- `/ticker/<symbol>` — ticker intelligence and article evidence
- `/screener` — Market Screener / Signal Explorer
- `/sp500` — S&P index state and user-selectable **Treemap / Bubbles** constituent views
- `/attribution` — company → industry → sector → index attribution
- `/events` — Historical Event Memory
- `/lab` — **Research Lab V2**
- `/agent` — machine-interface documentation
- `/portfolio` — portfolio strategy research
- `/research` and `/research/<slug>` — empirical research library
- `/data` — data contracts
- `/methodology` — methodology and caveats
- `/agent-manifest.json` — static machine-readable discovery/semantics contract

---

## 4. Non-negotiable data semantics

### Article sentiment

Cross-provider duplicate headlines are deduplicated before aggregation.

`article_sentiment = P(positive) - P(negative)` from FinBERT.

Retain article title, URL, source/provider, timestamp, article score `s`, label, and probabilities where available.

### No-news semantics

**No news is missing, not neutral zero.** Never coerce absent sentiment to `0.0` for aggregation convenience.

### Ticker-day sentiment

Equal-weight mean of unique scored articles for that ticker/day.

### S&P cap-weighted sentiment

Only observed constituents enter the denominator:

`S_t = sum(w_i * s_i for observed i) / sum(w_i for observed i)`

Preserve coverage, observed ticker count, and unique-news evidence.

### Contribution

`contribution_i = constituent_weight_i * observed_sentiment_i`

Industry/sector contribution is additive. Group sentiment is a separately renormalized statistic over observed group weight.

### S&P price

Use `^GSPC` or another true index source. **Never use SPY as an S&P index-level price substitute.**

---

## 5. S&P constituent metadata contract

PR #6 fixed company-name coverage at the data-builder level in `src/market_sentiment/cli/build_sp500_heatmap.py`.

Current rules:

- Wikipedia S&P constituent metadata cache default TTL = **7 days**.
- Refresh stale cache; use stale cache only if refresh fails.
- Missing company names fall back to yfinance `longName`, then `shortName`.
- Missing sector/industry may fall back to yfinance metadata.
- Handle ticker variants such as `BRK.B` / `BRK-B`.
- Heatmap stats record missing-name/classification diagnostics and yfinance fallbacks.
- Builder fails if company-name coverage falls below **98%**.
- Frontend ticker-name fallback is only defensive; do not manually hardcode company names in React.

---

## 6. Portfolio safeguards

Do not silently change portfolio calculations during UI/product work.

Keep intended lag/rebalance/exposure/cost logic, correct exits, missing-price handling, and risk metrics. Do not resurrect sold positions through zero→NaN forward-fill bugs. Do not label a moving average as a literal predicted return / Buy / Sell forecast.

---

## 7. Completed phases

### Phase 1 — complete

Product shell, market-state homepage, ticker explorer, Data/Methodology pages, frontend CI foundation.

### Phase 2 — complete via PR #2

Deterministic **Why sentiment changed**, positive/negative article drivers, S&P contribution attribution, coverage/evidence diagnostics, and dark product styling.

### Phase 3 — Intelligence Engine — complete via PR #3 + PR #4

Added:

- `/screener`
- `/lab` first-generation daily cross-sectional Research Lab
- `/events`
- `/attribution`
- richer deterministic event taxonomy
- novelty/source-breadth/disagreement diagnostics
- dark Portfolio chart and authorship credit
- Research typography upgrade
- semantic green/red prices
- shared intelligence logic in `apps/web/lib/intelligence.ts`
- strict frontend CI: sparse checkout → `npm ci` → `npx tsc --noEmit` → `npm run build`

### Phase 4 — Query & Research Layer — complete via PR #6 + PR #8

#### 7.4.1 Ask the Market

Route: `/ask`  
Parser: `apps/web/lib/marketQuery.ts`

Natural language is mapped to explicit deterministic filters/rankings over Screener data. Supported concepts include sector/ticker/event filters, positive/negative sentiment, sentiment change, 1D return, divergence, contribution, weight, news attention, novelty, disagreement, result limits, and minimum news evidence.

The UI exposes the parsed query plan. It does **not** replace calculations with generated finance prose.

#### 7.4.2 Dual S&P visualization

`apps/web/app/sp500/Sp500HeatmapClient.tsx` supports both:

- **Treemap — default**, because the owner prefers the rectangular market-map view.
- **Bubbles — optional**, sector-clustered alternative.

Both share sector filtering, Contribution / Sentiment / 1D Return modes, area by constituent weight, semantic colors, rich hover evidence, and click-through to ticker pages.

**Do not remove either mode unless explicitly requested.** The former rule “avoid rectangular treemaps” is obsolete.

#### 7.4.3 Research Lab V2

Route: `/lab`  
Engine: `apps/web/lib/researchLabV2.ts`

Important corrected data semantics:

- Public ticker `S` can be carried forward for chart display.
- **Research Lab V2 only treats rows with `sentiment_observed=true` as fresh sentiment observations.**
- Carried-forward display sentiment is not a new research signal.
- Sentiment change uses the previous **observed** sentiment value, never synthetic zero.

Methodology:

1. For each trading date, take the available observed cross-section.
2. Rank by sentiment level, sentiment change, or sentiment-price divergence.
3. Form equal-weight high/low quantiles (20%, 25%, or 33%).
4. Compute forward returns at 1D / 3D / 5D / 20D.
5. Form the daily high-minus-low spread.
6. Report gross spread, hit rate, Sharpe, turnover, simple t-stat, and **Newey-West/Bartlett HAC t-stat**.
7. HAC lag = `horizon - 1` to address mechanical serial dependence from overlapping forward returns.
8. Provide chronological first-70% in-sample / last-30% out-of-sample views.
9. Provide user-selected transaction-cost sensitivity using average long+short one-way turnover.
10. Allow selected specification/result export as JSON.

These remain descriptive diagnostics, not causal investment recommendations.

#### 7.4.4 Agent Interface

Route: `/agent`  
Manifest: `/agent-manifest.json`

GitHub Pages has no always-on backend, so the project deliberately exposes a **static machine-readable interface contract** rather than pretending to have a dynamic API server.

The manifest documents resources such as:

- `/data/SPX/sp500_index.json`
- `/data/SPX/sp500_heatmap.json`
- `/data/_tickers.json`
- `/data/ticker/{SYMBOL}.json`
- `/data/portfolio_strategy.json`
- research JSON resources

Agent consumers must preserve no-news missingness, article-level sentiment, true SPX pricing, and deterministic contribution semantics.

---

## 8. Current design rules

Visual direction: dark, clean **market-intelligence terminal**.

Preferred:

- dark neutral surfaces
- subtle borders
- readable tables/metrics
- restrained semantic green/red
- deterministic explanations
- Treemap default + Bubbles available

Avoid:

- white panels inside dark pages
- generic marketing blocks
- excessive glow/gradients
- tiny weak research typography
- hiding all important information behind hover
- removing one S&P visualization mode when both can coexist

Semantic colors:

- positive/rising: green
- negative/falling: red
- flat/missing: neutral gray

---

## 9. Validation contract

### Frontend CI

`.github/workflows/frontend-ci.yml`:

1. sparse checkout `apps/web`
2. `npm ci --no-audit --no-fund`
3. `npx tsc --noEmit`
4. strict `npm run build`

Never use `npm run build || true`.

PR #6 and PR #8 both passed TypeScript and Next.js production build before merge. PR #8 build confirmed static generation of `/ask`, `/agent`, and `/lab`.

### Python source CI

`.github/workflows/python-source-ci.yml` validates `src/market_sentiment/**` changes with Python compilation and S&P metadata helper tests.

PR #6 passed both Frontend CI and Python Source CI before merge.

Production data refreshes still require actual production-pipeline/data-contract validation beyond syntax checks.

---

## 10. Important generated/public artifacts

- `apps/web/public/data/_tickers.json`
- `apps/web/public/data/ticker/<SYMBOL>.json`
- `apps/web/public/data/SPX/sp500_index.json`
- `apps/web/public/data/SPX/sp500_heatmap.json`
- `apps/web/public/data/portfolio_strategy.json`
- `apps/web/public/research/*`
- `apps/web/public/agent-manifest.json`

Prefer stable data contracts over runtime React source rewriting.

---

## 11. Historical failure modes — never reintroduce

- Never suppress build failures with `|| true`.
- Do not dynamically rewrite React/TypeScript in production workflows.
- Do not duplicate incompatible sentiment algorithm-version constants.
- Never convert missing sentiment to neutral zero.
- Do not drop article-level sentiment if UI/agents depend on it.
- Never substitute SPY for S&P index level.
- Do not create portfolio ghost holdings via zero→NaN forward fill.
- Do not reuse an unbounded constituent metadata cache.
- Do not use carried-forward display sentiment as a fresh Research Lab observation.
- Static research routes must still compile in clean frontend CI without production artifacts.

---

## 12. Legacy production-pipeline debt

`.github/workflows/pipeline.yml` still contains substantial embedded Python heredoc logic.

Do not add new product/business logic there when a normal source module is appropriate. A full migration should be a dedicated infrastructure PR with representative-data regression validation.

---

## 13. Recommended Phase 5 direction

The next generation should build on the now-stable Query Layer rather than adding generic prose.

High-value Phase 5 candidates:

1. **Persistent Event Store V2** — deeper retained history, stable event-instance IDs, sector/industry event propagation, surprise/consensus features where auditable.
2. **Formal Research Infrastructure** — point-in-time universe controls, stronger clustered inference, multiple-testing discipline, downloadable daily portfolio legs, and reproducible specification hashes.
3. **Agent Query Protocol** — versioned query specification schema and deterministic result bundles that external agents can execute without scraping UI.
4. **Watchlists / alerts / personalization** only after the deterministic query/result schema is stable.
5. **Optional LLM interpretation layer** only downstream of evidence, with explicit source/result references and no hidden alteration of calculations.
6. **Legacy pipeline modularization** — extract embedded production scripts into `src/market_sentiment` with data-contract regression tests.

---

## 14. Project-owner workflow preferences

- Discussion mainly in Chinese.
- Code/comments in English.
- Direct implementation instead of long speculative checklists.
- Use GitHub-connected changes when possible.
- Avoid manual upload/retry loops when connector access exists.
- Explain root causes clearly.
- Never claim success before CI/build/data validation.

If the owner says “直接改”, make the changes rather than returning only a proposal.

---

## 15. First instruction to any new AI / contributor

1. Read this file and current `README.md`.
2. Inspect latest `main` and relevant generated data contracts.
3. Treat `main` as source of truth over old conversation history.
4. Preserve no-news ≠ zero, article evidence, true SPX pricing, contribution semantics, company-name coverage safeguards, and portfolio boundaries.
5. Treat Phase 1–4 as completed unless current `main` proves otherwise.
6. Preserve both S&P visualization modes; Treemap is default.
7. Research Lab work must respect `sentiment_observed` rather than carried-forward display `S`.
8. Continue from Phase 5 priorities or a newer explicit instruction from the owner.