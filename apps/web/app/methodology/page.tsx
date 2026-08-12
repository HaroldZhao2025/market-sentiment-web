import type { Metadata } from "next";
import Link from "next/link";

export const metadata: Metadata = {
  title: "Methodology",
  description: "How Sentiment Intelligence deduplicates news, scores FinBERT sentiment, aggregates the S&P 500, and evaluates portfolio signals.",
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
    title: "Aggregate the index",
    body: "Cap-weighted S&P sentiment renormalizes constituent market-cap weights only across tickers with an observed sentiment value that day. Coverage metrics accompany the aggregate.",
  },
  {
    number: "06",
    title: "Evaluate, do not narrate",
    body: "The portfolio and research layers use deterministic calculations with explicit execution lag, trading costs, exposure limits, and generated empirical artifacts.",
  },
];

const definitions = [
  ["Article sentiment", "s = P(positive) − P(negative) from FinBERT."],
  ["Ticker-day sentiment", "Equal-weight mean of deduplicated, scored headlines for one company on one day."],
  ["Cap-weighted sentiment", "Weighted mean across observed constituents, with weights renormalized inside the observed set."],
  ["Coverage", "How much of the constituent universe / market-cap weight has an observed signal on a given date."],
  ["Divergence", "A diagnostic where news sentiment and contemporaneous price return have opposite signs."],
  ["Research panel", "A reproducible daily panel derived from the same generated data family used by the website."],
];

export default function MethodologyPage() {
  return (
    <main className="space-y-12">
      <section className="max-w-4xl">
        <div className="eyebrow">Trust layer</div>
        <h1 className="page-title mt-3">Every number should have a reason.</h1>
        <p className="mt-4 max-w-3xl text-base leading-7 text-neutral-400">
          Sentiment is useful only when the observation, aggregation, and missing-data rules are explicit. This page documents the production logic at a product level so that a market score can be interpreted rather than merely consumed.
        </p>
        <div className="mt-6 flex flex-wrap gap-2">
          <Link href="/data" className="pill">Inspect data endpoints →</Link>
          <Link href="/research" className="pill">Inspect empirical research →</Link>
        </div>
      </section>

      <section className="space-y-4">
        <div>
          <h2 className="section-title">Signal pipeline</h2>
          <p className="section-copy">The live website, index aggregate, ticker pages, portfolio, and research all originate from this evidence chain.</p>
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
          <p className="section-copy">These definitions are the intended semantic contract. A future model or agent should not reinterpret missing values or exposure rules.</p>
        </div>
        <div className="table-shell overflow-x-auto">
          <table className="w-full min-w-[680px] text-left text-sm">
            <thead className="border-b border-white/10 bg-white/[0.025] text-[11px] uppercase tracking-[0.12em] text-neutral-500">
              <tr>
                <th className="px-5 py-3 font-medium">Concept</th>
                <th className="px-5 py-3 font-medium">Definition</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {definitions.map(([name, definition]) => (
                <tr key={name}>
                  <td className="px-5 py-4 font-medium text-neutral-200">{name}</td>
                  <td className="px-5 py-4 leading-6 text-neutral-500">{definition}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-3">
        <MethodCard
          eyebrow="Missing data"
          title="No news is not neutral news."
          copy="A missing observation should remain visibly missing in the live signal layer. This prevents low-news days from mechanically pulling aggregate sentiment toward zero."
        />
        <MethodCard
          eyebrow="Index weights"
          title="Coverage is part of the estimate."
          copy="An index score based on a small observed subset should not look as authoritative as a broad-coverage score. Market-cap coverage and observed ticker counts belong beside the estimate."
        />
        <MethodCard
          eyebrow="Backtests"
          title="Execution assumptions matter."
          copy="Portfolio results should include signal lag, rebalance timing, exposure constraints, turnover, and transaction costs. Backtests are research outputs, not performance promises."
        />
      </section>

      <section className="card border-amber-400/10 bg-amber-400/[0.035] p-6">
        <div className="text-sm font-semibold text-amber-200">Important limitations</div>
        <div className="mt-3 grid gap-3 text-sm leading-6 text-neutral-400 md:grid-cols-2">
          <p>Current-cap weighting is not a historical point-in-time constituent-weight dataset. Historical index analysis should not be interpreted as a fully reconstructed historical S&P 500 membership series.</p>
          <p>Headline sentiment is a model-derived feature, not a causal interpretation of news. Source selection, headline wording, duplicate detection, and model calibration can all affect the signal.</p>
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
