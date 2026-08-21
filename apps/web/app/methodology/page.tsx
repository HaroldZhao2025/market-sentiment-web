import type { Metadata } from "next";
import Link from "next/link";

export const metadata: Metadata = { title: "Methodology", description: "How Sentiment Intelligence collects free-public company evidence, scores FinBERT sentiment, aggregates the S&P 500, builds events, and evaluates signals." };

const steps = [
  { number: "01", title: "Collect free-public company data", body: "Company news is discovered from Yahoo public finance surfaces and Google News RSS. Daily market history uses public Yahoo Finance data through yfinance. Paid/Premium feeds are not production dependencies." },
  { number: "02", title: "Deduplicate article coverage", body: "Normalized headlines and canonical URLs remove repeated coverage before scoring and aggregation, so duplicated stories do not receive extra weight merely because multiple discovery surfaces carried them." },
  { number: "03", title: "Score unique headlines", body: "ProsusAI/FinBERT produces positive, neutral and negative probabilities. The published scalar is P(positive) − P(negative). Article-level scores remain attached to retained source links." },
  { number: "04", title: "Keep no-news days missing", body: "Ticker-day sentiment is the mean of unique scored articles for one company and day. A day without scored news remains missing. Display charts must not turn carried values into fresh observations." },
  { number: "05", title: "Separate company coverage from SPX", body: "The broad U.S. layer supplies company news, history and earnings pages. S&P 500 weighting, attribution, portfolio and Research Lab calculations remain on the S&P core." },
  { number: "06", title: "Resolve earnings evidence", body: "Free-public transcript discovery uses public transcript pages and SEC EDGAR exhibits/links. A call becomes structured only after transcript text can be parsed into prepared remarks and Q&A. Filing links never masquerade as transcript text." },
  { number: "07", title: "Publish derived call diagnostics", body: "Transcript text is processed transiently for speaker sections, FinBERT tone, uncertainty, forward-looking language and topics. Public artifacts retain derived metrics and source URLs, not third-party transcript body text." },
  { number: "08", title: "Aggregate, attribute and test", body: "Observed S&P constituent sentiment is cap-weighted with observed weights renormalized in the denominator. Research ranks the available observed cross-section by date and reports robust diagnostics while portfolio assumptions remain explicit." },
];

const definitions = [
  ["Article sentiment", "s = P(positive) − P(negative) from FinBERT."],
  ["Ticker-day sentiment", "Equal-weight mean of deduplicated scored headlines for one company on one day."],
  ["Observed day", "A date with fresh scored article evidence. A carried display value is not a new observation."],
  ["Cap-weighted sentiment", "Weighted mean across observed S&P 500 constituents, renormalizing only inside the observed set."],
  ["Raw contribution", "S&P constituent weight × observed ticker sentiment; additive company → industry → sector."],
  ["Company history", "Extended daily price history plus observed-only sentiment flags for the broad U.S. company pages."],
  ["Structured earnings call", "A free-public transcript parsed into turns/sections with derived diagnostics; a source link alone is not a structured call."],
  ["Event instance", "Persistent grouping of nearby company articles with the same deterministic event theme, preserving article/source breadth and disagreement."],
  ["Research Lab spread", "For each date: mean forward return of the high-signal cross-sectional quantile minus the low-signal quantile."],
];

export default function MethodologyPage() {
  return <main className="space-y-12">
    <section className="max-w-4xl"><div className="eyebrow">Methodology</div><h1 className="page-title mt-3">From public source to published signal.</h1><p className="mt-4 max-w-3xl text-base leading-7 text-neutral-400">The key rules are source policy, missing-data treatment, universe boundaries and reproducible aggregation. Those rules stay consistent across the site.</p><div className="mt-6 flex flex-wrap gap-2"><Link href="/data" className="pill">Data contracts →</Link><Link href="/companies" className="pill">Companies →</Link><Link href="/lab" className="pill">Research Lab →</Link></div></section>

    <section className="space-y-4"><div><h2 className="section-title">Pipeline</h2><p className="section-copy">Company fulfillment and S&amp;P research share article scoring, but they do not share the same universe contract.</p></div><div className="grid gap-3 lg:grid-cols-2">{steps.map((step) => <div key={step.number} className="card p-5 md:p-6"><div className="flex gap-4"><div className="text-sm font-semibold tabular-nums text-emerald-400">{step.number}</div><div><h3 className="font-semibold text-white">{step.title}</h3><p className="mt-2 text-sm leading-6 text-neutral-500">{step.body}</p></div></div></div>)}</div></section>

    <section className="space-y-4"><h2 className="section-title">Definitions</h2><div className="table-shell overflow-x-auto"><table className="w-full min-w-[680px] text-left text-sm"><thead className="border-b border-white/10 bg-white/[0.025] text-[11px] uppercase tracking-[0.12em] text-neutral-500"><tr><th className="px-5 py-3 font-medium">Concept</th><th className="px-5 py-3 font-medium">Definition</th></tr></thead><tbody className="divide-y divide-white/5">{definitions.map(([name, definition]) => <tr key={name}><td className="px-5 py-4 font-medium text-neutral-200">{name}</td><td className="px-5 py-4 leading-6 text-neutral-500">{definition}</td></tr>)}</tbody></table></div></section>

    <section className="grid gap-4 lg:grid-cols-4"><MethodCard eyebrow="Sources" title="Free public only" copy="News, market history, transcript discovery and SEC evidence do not require a paid/Premium production feed." /><MethodCard eyebrow="Missing data" title="No news is not neutral" copy="Absent article evidence stays missing and is excluded from observed-only aggregation and research signals." /><MethodCard eyebrow="Universe" title="Broad U.S. ≠ SPX" copy="Broader company pages do not silently alter S&P index weights, attribution, portfolio or Research Lab samples." /><MethodCard eyebrow="Transcripts" title="Derived metrics, not copied text" copy="Call pages publish source links and derived speaker/section diagnostics without redistributing third-party transcript bodies." /></section>

    <section className="card border-amber-400/10 bg-amber-400/[0.035] p-6"><div className="text-sm font-semibold text-amber-200">Limitations</div><div className="mt-3 grid gap-3 text-sm leading-6 text-neutral-400 md:grid-cols-2"><p>Current S&amp;P weights and membership are not a fully reconstructed historical point-in-time constituent dataset.</p><p>Free-public news and transcript availability varies by company and date. Coverage artifacts report what has been found rather than filling unavailable evidence.</p><p>Event history is bounded by retained company news. It is a persistent archive of collected evidence, not a claim of complete media coverage.</p><p>Research t-statistics, Sharpe ratios and backtests are diagnostics. Publication-grade work still requires careful multiple-testing, clustering, costs and genuinely held-out validation.</p></div></section>
  </main>;
}

function MethodCard({ eyebrow, title, copy }: { eyebrow: string; title: string; copy: string }) {
  return <div className="card p-6"><div className="eyebrow">{eyebrow}</div><h3 className="mt-3 text-lg font-semibold text-white">{title}</h3><p className="mt-3 text-sm leading-6 text-neutral-500">{copy}</p></div>;
}