# New Chat Prompt — Sentiment Intelligence

Copy the prompt below into a brand-new ChatGPT / Codex / Claude conversation when continuing this project.

---

You are continuing development of my GitHub project:

`HaroldZhao2025/market-sentiment-web`

Live site:

`https://haroldzhao2025.github.io/market-sentiment-web/`

## FIRST: read the repository memory

Before proposing or changing anything, use the connected GitHub repository and read these files from the latest `main`:

1. `PROJECT_MEMORY.md` — this is the primary project handoff and persistent memory.
2. `README.md` — current authorship and project overview.
3. Inspect the actual current source files relevant to the task I give you.

Do **not** rely only on old conversation history if it conflicts with current `main`.

## Working style

- Please communicate with me mainly in **Chinese**.
- Code and code comments should be in **English**.
- I prefer you to **directly implement changes** in GitHub rather than give me long upgrade checklists.
- When possible, create a clean branch from the latest `main`, make real file-level changes, open a focused PR, and check CI.
- Do not ask me to manually upload files if the GitHub connector can do the work.
- Do not claim a fix is successful until you have checked the relevant CI/build/data contract.
- Avoid giant runtime source patches inside GitHub Actions. Application logic belongs in normal source files.

## Product direction

The project is now called **Sentiment Intelligence**.

It is evolving from a market-sentiment dashboard into an auditable market-intelligence and research platform.

Core principle:

> Do not compete with LLMs by generating generic financial prose. Build proprietary, deterministic, auditable market data, event intelligence, screens, research, and backtests that LLMs can consume.

Important methodology that must not be broken:

- Cross-provider news deduplication.
- FinBERT article sentiment = `P(positive) - P(negative)`.
- **No news is missing, not neutral zero.**
- Ticker daily sentiment is based on unique scored articles.
- S&P cap-weighted sentiment renormalizes only over tickers with real observed sentiment.
- Ticker index contribution = constituent weight × observed sentiment.
- Article-level sentiment must remain available if the ticker UI displays headline sentiment.
- Do not use SPY as an S&P index-level price substitute.
- Portfolio logic should not be silently changed during pure UI work.

## Authorship

Read authorship from `README.md`, not from guesses.

Current attribution:

- Portfolio Strategy by **leolin0407-cmyk** (`leolin0407@gmail.com`)
- Market Sentiment, Website Design, Repo Setting by **HaroldZhao2025** (`stevenfinch2022@outlook.com`)

The Portfolio page should visibly credit **leolin0407-cmyk**.

## Completed work

Phase 1 and Phase 2 are already merged into `main`.

Phase 1 added the Sentiment Intelligence product shell, market-state homepage, ticker signal explorer, Data/Methodology pages, and frontend PR CI.

Phase 2 added ticker **Why sentiment changed**, deterministic event themes, positive/negative headline drivers, S&P contribution attribution, and dark-product styling improvements.

## CURRENT NEXT TASKS

Unless I give you a newer instruction, these are the immediate UI tasks to finish:

1. **Stock price should also be green/red**
   - latest price number should be green if latest return > 0
   - red if latest return < 0
   - neutral if flat/missing

2. **Portfolio chart still has a white background**
   - inspect `apps/web/components/PortfolioChart.tsx`
   - remove explicit white/light SVG background and light chart panel
   - make the chart natively fit the dark Portfolio page instead of relying only on compatibility CSS

3. **Research typography is not good**
   - redesign typography and hierarchy in:
     - `apps/web/app/research/ResearchIndexClient.tsx`
     - `apps/web/app/research/ResearchStudyClient.tsx`
     - `apps/web/app/research/[slug]/page.tsx`
   - keep empirical results unchanged
   - use readable dark cards/tables, stronger title hierarchy, less tiny gray text

4. **Replace the current S&P rectangular heatmap with a packed bubble / cluster visualization**
   - no rectangular treemap
   - stock bubbles / circles packed into organic groups
   - bubble area ≈ market cap or constituent weight
   - ideally sector clustering
   - retain modes: Contribution / Sentiment / 1D Return
   - mouse hover should show ticker, company, sector/industry, weight, price, 1D return, sentiment, contribution, unique-news count
   - click bubble → ticker page
   - labels for large bubbles
   - smooth premium interaction

5. **Add Portfolio author credit**
   - visible on `/portfolio`
   - `Portfolio Strategy by leolin0407-cmyk`
   - source of truth is the README

## After these fixes

The recommended Phase 3 is the **Intelligence Engine**:

- Market Screener / Signal Explorer
- Interactive Research Lab
- Historical Event Memory
- richer event taxonomy / novelty / consensus
- sector → industry → company S&P attribution
- modularize business logic out of giant workflow YAML into normal Python modules

Later phases can add Ask the Market, API / agent interfaces, watchlists, alerts, and personalization.

## Important historical mistakes to avoid

Read `PROJECT_MEMORY.md` for details, but especially avoid:

- `npm run build || true`
- runtime rewriting of React/TypeScript source
- duplicated sentiment algorithm version strings in multiple scripts
- missing → zero conversions
- dropping article-level sentiment from the cache
- root/public S&P JSON inconsistencies
- portfolio ghost holdings caused by zero weights being converted to NaN before forward fill

Now inspect the latest `main` and continue directly from my next instruction. Do not give me a generic plan unless I explicitly ask for one.
