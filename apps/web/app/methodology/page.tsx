import type { Metadata } from "next";
import Link from "next/link";

export const metadata: Metadata = {
  title: "Methodology",
  description: "How Sentiment Intelligence deduplicates news, scores FinBERT sentiment, aggregates the S&P 500, classifies events, and evaluates signals.",
};

const steps = [
  {
    number: "01",
    title: "Collect raw evidence",
    body: "Ticker-level headlines are collected from Finnhub and Yahoo Finance alongside market prices. Raw source files are retained separately from derived sentiment artifacts.",
  },
  {
    number: "02",
    title: "Deduplicate across providers",
    body: "Normalized headlines and canonical URLs are used to remove repeated coverage of the same item. The intent is to weight information events, not the number of feeds that happened to carry them.",
  },
  {
    number: "03",
    title: "Score each unique headline",
    body: "ProsusAI/FinBERT produces positive, neutral, and negative probabilities. The scalar article score is P(positive) − P(negative), bounded near [−1, 1].",
  },
  {
    number: "04",
    title: "Build ticker-day observations",
    body: "Unique article scores are averaged within ticker × day. A day with no scored news is missing in the live sentiment layer; it is not silently relabeled as a neutral zero.",
  },
  {
    number: "05",
    title: "Aggregate and attribute the index",
    body: "Cap-weighted S&P sentiment renormalizes constituent weights only across observed tickers. Raw contribution remains constituent weight × observed sentiment and can be aggregated company → industry → sector → index.",
  },
  {
    number: "06",
    title: "Build deterministic event intelligence",
    body: "Retained scored headlines are assigned to explicit keyword taxonomies such as earnings, guidance, product/AI, M&A, regulation, legal, management, operations, and analyst actions. Novelty and disagreement are diagnostics computed from retained evidence, not generated prose.",
  },
  {
    number: "07",
    title: "Evaluate signals cross-sectionally",
    body: "The Research Lab ranks the available ticker cross-section separately on each trading date, forms equal-weight high- and low-signal groups, computes forward-return spreads, and summarizes the resulting daily spread series.",
  },
  {
    number: "08",
    title: "Treat backtests as research",
    body: "Portfolio and research outputs use deterministic calculations with explicit execution lag, trading costs, exposure limits, and generated empirical artifacts. Diagnostic statistics are not causal claims or performance promises.",
  },
];

const definitions = [
  ["Article sentiment", "s = P(positive) − P(negative) from FinBERT."],
  ["Ticker-day sentiment", "Equal-weight mean of deduplicated, scored headlines for one company on one day."],
  ["Cap-weighted sentiment", "Weighted mean across observed constituents, with weights renormalized inside the observed set."],
  ["Raw contribution", "Constituent index weight × observed ticker sentiment. It is additive across company, industry, and sector groupings."],
  ["Coverage", "How much of the constituent universe / market-cap weight has an observed signal on a given date."],
  ["Sentiment-price divergence", "Observed sentiment minus a clipped 1D price-return signal, where the return is scaled by 5% before clipping to [−1, 1]."],
  ["Event novelty", "One minus the highest Jaccard token similarity between a retained headline and other retained headlines for the ticker snapshot."],
  ["Sentiment disagreement", "Cross-headline standard deviation of article-level sentiment scores within the retained evidence set."],
  ["Research Lab spread", "For each date: mean forward return of the high-signal cross-sectional quantile minus the low-signal quantile; summary statistics are then computed across dates."],
];

export default function MethodologyPage() {
  return (
    <main className="space-y-12">
      <section className="max-w-4xl">
        <div className="eyebrow">Trust layer</div>
        <h1 className="page-title mt-3">Every number should have a reason.</h1>
        <p className="mt-4 max-w-3xl text-base leading-7 text-neutral-400">
          Sentiment is useful only when the observation, aggregation, event-classification, and missing-data rules are explicit. The intelligence engine is deterministic by design so market scores, screens, event diagnostics, and research results can be audited back to retained evidence.
        </p>
        <div className="mt-6 flex flex-wrap gap-2">
          <Link href="/data" className="pill">Inspect data endpoints →</Link>
          <Link href="/screener" className="pill">Open Screener →</Link>
          <Link href="/lab" className="pill">Open Research Lab →</Link>
          <Link href="/research" className="pill">Empirical research →</Link>
        </div>
      </section>

      <section className="space-y-4">
        <div>
          <h2 className="section-title">Signal and intelligence pipeline</h2>
          <p className="section-copy">The live website, index aggregate, ticker pages, event memory, screener, portfolio, and research all originate from this evidence chain.</p>
        </div>
        <div className="grid gap-3 lg:grid-cols-2">
          {steps.map((step) => (
            <div key={step.number} className="card p-5 md:p-6">
              <div className="flex gap-4">
                <div className="text-sm font-semibold tabular-nums text-emerald-400">{step.number}</div>
                <div>
                  <h3 className="font-semibold text-white">{step.title}</h3>
                  <p className="mt-2 text-sm leading-6 text-neutral-500">{step.body}</p>
                </div>
              </div>
            </div>
          ))}
        </div>
      </section>

      <section className="space-y-4">
        <div>
          <h2 className="section-title">Core definitions</h2>
          <p className="section-copy">These definitions are the semantic contract. A future model or agent should not reinterpret missing values, contribution, or the research sorting rules.</p>
        </div>
        <div className="table-shell overflow-x-auto">
          <table className="w-full min-w-[680px] text-left text-sm">
            <thead className="border-b border-white/10 bg-white/[0.025] text-[11px] uppercase tracking-[0.12em] text-neutral-500">
              <tr><th className="px-5 py-3 font-medium">Concept</th><th className="px-5 py-3 font-medium">Definition</th></tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {definitions.map(([name, definition]) => (
                <tr key={name}><td className="px-5 py-4 font-medium text-neutral-200">{name}</td><td className="px-5 py-4 leading-6 text-neutral-500">{definition}</td></tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-4">
        <MethodCard eyebrow="Missing data" title="No news is not neutral news." copy="A missing observation remains missing. This prevents low-news tickers and days from mechanically pulling live aggregate sentiment toward zero." />
        <MethodCard eyebrow="Attribution" title="Contribution is additive." copy="Company contribution is weight × observed sentiment. Sector and industry contributions sum those company-level terms; group sentiment is separately renormalized over observed group weight." />
        <MethodCard eyebrow="Research Lab" title="Rank within each date." copy="Signal quantiles are formed independently for each trading date before forward returns are averaged. Different calendar dates are never pooled into one cross-sectional ranking." />
        <MethodCard eyebrow="Backtests" title="Execution assumptions matter." copy="Portfolio results should include signal lag, rebalance timing, exposure constraints, turnover, and transaction costs. Backtests are research outputs, not performance promises." />
      </section>

      <section className="card border-amber-400/10 bg-amber-400/[0.035] p-6">
        <div className="text-sm font-semibold text-amber-200">Important limitations</div>
        <div className="mt-3 grid gap-3 text-sm leading-6 text-neutral-400 md:grid-cols-2">
          <p>Current-cap weighting is not a historical point-in-time constituent-weight dataset. Historical index analysis should not be interpreted as a fully reconstructed historical S&P 500 membership series.</p>
          <p>Headline sentiment and event classes are model- and rule-derived features, not causal interpretations of news. Source selection, retained article history, duplicate detection, wording, and calibration can affect the signal.</p>
          <p>Event Memory is bounded by the article history retained in current ticker artifacts. It is not a complete historical news archive, and event-reaction averages may contain overlapping or correlated observations.</p>
          <p>Research Lab t-statistics and Sharpe ratios are diagnostics. Overlapping forward horizons can create serial dependence; publication-grade inference should use appropriate robust or clustered standard errors and out-of-sample validation.</p>
        </div>
      </section>
    </main>
  );
}

function MethodCard({ eyebrow, title, copy }: { eyebrow: string; title: string; copy: string }) {
  return (
    <div className="card p-6">
      <div className="eyebrow">{eyebrow}</div>
      <h3 className="mt-3 text-lg font-semibold text-white">{title}</h3>
      <p className="mt-3 text-sm leading-6 text-neutral-500">{copy}</p>
    </div>
  );
}
