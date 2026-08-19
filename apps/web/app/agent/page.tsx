import Link from "next/link";

export const dynamic = "force-static";
export const metadata = { title: "Agent Interface", description: "Machine-readable market and company contracts for external agents." };

const resources = [
  ["Agent manifest", "/agent-manifest.json", "Discovery, semantics, source policy and resource paths."],
  ["Query schema", "/agent-query-schema-v2.json", "Versioned deterministic query/result specification."],
  ["U.S. company universe", "/data/v5/universe.json", "Composite 1500 plus broader U.S. company coverage."],
  ["Company news", "/data/v5/news/AAPL.json", "Retained deduplicated company news and article scores."],
  ["Company history", "/data/v5/history/AAPL.json", "Extended daily price history with observed sentiment flags."],
  ["Company data coverage", "/data/v5/company_data_coverage.json", "News/history fulfillment coverage across the company universe."],
  ["Company earnings", "/data/v5/earnings/AAPL.json", "EPS results, structured call diagnostics where available, source links and filings."],
  ["Earnings coverage", "/data/v5/earnings_coverage.json", "Structured-call coverage status across the company universe."],
  ["Event instances", "/data/v5/event_instances.json", "Persistent clustered company event history."],
  ["Multi-index sentiment", "/data/v5/index_sentiment.json", "Observed sentiment across S&P tiers and the broader U.S. layer."],
  ["S&P index", "/data/SPX/sp500_index.json", "True-index close, sentiment and coverage."],
  ["S&P constituents", "/data/SPX/sp500_heatmap.json", "Weights, returns and constituent sentiment."],
  ["Research index", "/research/index.json", "Generated empirical-study registry."],
];

const BASE = (process.env.NEXT_PUBLIC_BASE_PATH || "").replace(/\/$/, "");
const publicHref = (value: string) => `${BASE}${value.startsWith("/") ? value : `/${value}`}`;

export default function AgentPage() {
  return <main className="space-y-8">
    <section className="max-w-4xl"><div className="eyebrow">Machine interface</div><h1 className="page-title mt-2">Agent Interface</h1><p className="mt-3 max-w-2xl text-sm leading-6 text-neutral-400">Static contracts for market, company, event and research workflows on GitHub Pages.</p><div className="mt-5 flex flex-wrap gap-2"><a href={publicHref("/agent-manifest.json")} className="pill">Manifest →</a><Link href="/ask" className="pill">Ask →</Link><Link href="/data" className="pill">Data →</Link></div></section>
    <section className="grid gap-3 md:grid-cols-3"><div className="card p-5"><div className="eyebrow">Nulls</div><h2 className="mt-2 text-lg font-semibold text-white">Missing stays missing</h2><p className="mt-2 text-sm leading-6 text-neutral-500">Use observation flags where available; absent news sentiment is never neutral zero.</p></div><div className="card p-5"><div className="eyebrow">Boundaries</div><h2 className="mt-2 text-lg font-semibold text-white">Broad U.S. ≠ SPX</h2><p className="mt-2 text-sm leading-6 text-neutral-500">Extended company coverage never changes S&amp;P 500 weighting, attribution, portfolio or research semantics.</p></div><div className="card p-5"><div className="eyebrow">Hosting</div><h2 className="mt-2 text-lg font-semibold text-white">Static by design</h2><p className="mt-2 text-sm leading-6 text-neutral-500">GitHub Pages serves versioned files rather than an always-on API.</p></div></section>
    <section className="space-y-3"><h2 className="section-title">Resources</h2><div className="table-shell overflow-x-auto"><table className="w-full min-w-[760px] text-sm"><thead className="border-b border-white/10 bg-white/[0.025] text-left text-[11px] uppercase tracking-[0.1em] text-neutral-600"><tr><th className="px-4 py-3">Resource</th><th className="px-4 py-3">Path</th><th className="px-4 py-3">Use</th></tr></thead><tbody>{resources.map(([name, href, description]) => <tr key={name} className="border-b border-white/[0.06] last:border-0"><td className="px-4 py-3 font-medium text-neutral-200">{name}</td><td className="px-4 py-3"><a href={publicHref(href)} className="font-mono text-xs text-emerald-300 hover:underline">{href}</a></td><td className="px-4 py-3 text-sm leading-6 text-neutral-500">{description}</td></tr>)}</tbody></table></div></section>
    <section className="card p-5 text-sm leading-6 text-neutral-500"><span className="font-semibold text-neutral-300">Contract:</span> preserve article scores, null semantics, true S&amp;P index pricing, additive contribution, universe boundaries, and transcript non-redistribution.</section>
  </main>;
}