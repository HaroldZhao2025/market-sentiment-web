import type { Metadata } from "next";
import Link from "next/link";

export const metadata: Metadata = {
  title: "Methodology",
  description: "How Sentiment Intelligence collects free-public news and earnings evidence, scores FinBERT sentiment, aggregates the S&P 500, and evaluates research signals.",
};

const steps = [
  {
    number: "01",
    title: "Collect free-public company evidence",
    body: "Extended-company news is collected from Yahoo Finance public news and Google News RSS. Market prices come from public Yahoo Finance market data. Earnings fulfillment searches free public transcript pages plus SEC EDGAR and public IR material; paid and Premium APIs are excluded from production.",
  },
  {
    number: "02",
    title: "Deduplicate articles",
    body: "Normalized headlines and canonical URLs remove repeated coverage across sources so repeated syndication does not receive extra weight.",
  },
  {
    number: "03",
    title: "Score each retained headline",
    body: "ProsusAI/FinBERT produces positive, neutral and negative probabilities. Article sentiment is P(positive) − P(negative), bounded near [−1, 1].",
  },
  {
    number: "04",
    title: "Build company-day observations",
    body: "Unique scored articles are averaged within ticker × day. A day with no scored news remains missing in stored data. Company charts may carry the last observed value forward only for visual continuity after the first real observation.",
  },
  {
    number: "05",
    title: "Keep index and company universes separate",
    body: "The Composite 1500 company layer supports company news, history and earnings. S&P 500 weighting, attribution, portfolio and research calculations continue to use their explicit core S&P inputs rather than silently adding MidCap or SmallCap names.",
  },
  {
    number: "06",
    title: "Aggregate the S&P 500",
    body: "Cap-weighted S&P sentiment renormalizes constituent weights only across observed tickers. Raw contribution is constituent weight × observed sentiment and remains additive company → industry → sector → index.",
  },
  {
    number: "07",
    title: "Build events and earnings-call diagnostics",
    body: "Related company headlines are clustered into persistent event instances. When a real free-public earnings transcript is found, transcript text is analyzed transiently into prepared/Q&A, speaker, topic and tone diagnostics; third-party transcript body text is not republished.",
  },
  {
    number: "08",
    title: "Evaluate research signals",
    body: "Research Lab and portfolio outputs use explicit sample, lag, horizon, turnover and trading-cost assumptions. Diagnostics are descriptive research rather than causal claims or performance promises.",
  },
];

const definitions = [
  ["Article sentiment", "s = P(positive) − P(negative) from FinBERT."],
  ["Ticker-day sentiment", "Equal-weight mean of deduplicated, scored headlines for one company on one day."],
  ["Missing company-day", "No scored news for that ticker-day. Stored research data remains missing; display carry-forward is not a new observation."],
  ["Cap-weighted sentiment", "Weighted mean across observed S&P 500 constituents, with weights renormalized inside the observed set."],
  ["Raw contribution", "S&P constituent index weight × observed ticker sentiment. It is additive across company, industry and sector."],
  ["Company-data coverage", "Whether a Composite-universe company has retained news and usable daily price history artifacts."],
  ["Structured earnings call", "Derived analytics from an actual free-public transcript. A filing, webcast link or article mentioning a call is not labeled as a structured transcript."],
  ["Call coverage", "Company-level status: complete structured call, partial structured call, source-link only, or no free-public structured transcript found yet."],
  ["Research Lab spread", "For each date: mean forward return of the high-signal cross-sectional quantile minus the low-signal quantile; summary statistics are then computed across dates."],
];

export default function MethodologyPage() {
  return (
    <main className="space-y-12">
      <section className="max-w-4xl">
        <div className="eyebrow">Methodology</div>
        <h1 className="page-title mt-3">How the published numbers are built</h1>
        <p className="mt-4 max-w-3xl text-base leading-7 text-neutral-400">
          The important rules are source policy, missing-data treatment, universe boundaries and reproducible aggregation. Those rules stay stable across the website, research tools and machine-readable files.
        </p>
        <div className="mt-6 flex flex-wrap gap-2">
          <Link href="/data" className="pill">Data endpoints →</Link>
          <Link href="/companies" className="pill">Companies →</Link>
          <Link href="/lab" className="pill">Research Lab →</Link>
          <Link href="/research" className="pill">Research →</Link>
        </div>
      </section>

      <section className="space-y-4">
        <div>
          <h2 className="section-title">Pipeline</h2>
          <p className="section-copy">Collection, scoring, aggregation and downstream research are separated so each published artifact can be interpreted correctly.</p>
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
          <p className="section-copy">These definitions are also the contract for agents and downstream analysis.</p>
        </div>
        <div className="table-shell overflow-x-auto">
          <table className="w-full min-w-[680px] text-left text-sm">
            <thead className="border-b border-white/10 bg-white/[0.025] text-[11px] uppercase tracking-[0.12em] text-neutral-500"><tr><th className="px-5 py-3 font-medium">Concept</th><th className="px-5 py-3 font-medium">Definition</th></tr></thead>
            <tbody className="divide-y divide-white/5">
              {definitions.map(([name, definition]) => <tr key={name}><td className="px-5 py-4 font-medium text-neutral-200">{name}</td><td className="px-5 py-4 leading-6 text-neutral-500">{definition}</td></tr>)}
            </tbody>
          </table>
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-4">
        <MethodCard eyebrow="Sources" title="Free public only" copy="Production company news and earnings fulfillment excludes paid and Premium APIs." />
        <MethodCard eyebrow="Missing data" title="Missing is not neutral" copy="A missing sentiment observation is not converted to zero in stored company or research data." />
        <MethodCard eyebrow="Attribution" title="Contribution is additive" copy="Company contribution is S&P weight × observed sentiment; grouped contributions sum those company terms." />
        <MethodCard eyebrow="Research" title="Scope is explicit" copy="Composite-company coverage does not silently change the S&P-based portfolio or empirical research sample." />
      </section>

      <section className="card border-amber-400/10 bg-amber-400/[0.035] p-6">
        <div className="text-sm font-semibold text-amber-200">Important limitations</div>
        <div className="mt-3 grid gap-3 text-sm leading-6 text-neutral-400 md:grid-cols-2">
          <p>Current-cap weighting is not a historical point-in-time constituent-weight dataset. Historical S&P analysis is not a fully reconstructed historical membership series.</p>
          <p>Free-public transcript availability differs by company and quarter. A missing structured call means the fulfillment system did not obtain a usable free-public transcript, not that no earnings call occurred.</p>
          <p>Retained company news is a broad archive assembled from the supported free-public discovery sources, not a claim to contain every article ever published about a company.</p>
          <p>Research statistics are diagnostics. Overlapping horizons, repeated specification search and changing samples require robust inference and out-of-sample validation for publication-grade claims.</p>
        </div>
      </section>
    </main>
  );
}

function MethodCard({ eyebrow, title, copy }: { eyebrow: string; title: string; copy: string }) {
  return <div className="card p-6"><div className="eyebrow">{eyebrow}</div><h3 className="mt-3 text-lg font-semibold text-white">{title}</h3><p className="mt-3 text-sm leading-6 text-neutral-500">{copy}</p></div>;
}
