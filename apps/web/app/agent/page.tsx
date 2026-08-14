import Link from "next/link";

export const dynamic = "force-static";

export const metadata = {
  title: "Agent Interface",
  description: "Machine-readable market and company contracts for external agents.",
};

const resources = [
  ["Agent manifest", "/agent-manifest.json", "Discovery, semantics and resource paths."],
  ["Query schema", "/agent-query-schema-v2.json", "Versioned query/result specification."],
  ["Composite company universe", "/data/v5/universe.json", "S&P 500, MidCap 400 and SmallCap 600 company layer."],
  ["Company news", "/data/v5/news/AAPL.json", "Deduplicated recent company articles and scores."],
  ["Company earnings", "/data/v5/earnings/AAPL.json", "Results, calls when available, and filing fallback."],
  ["Event instances", "/data/v5/event_instances.json", "Clustered company event history."],
  ["S&P index", "/data/SPX/sp500_index.json", "Index close, sentiment and coverage."],
  ["S&P constituents", "/data/SPX/sp500_heatmap.json", "Weights, returns and constituent sentiment."],
  ["Research index", "/research/index.json", "Generated empirical-study registry."],
];

const BASE = (process.env.NEXT_PUBLIC_BASE_PATH || "").replace(/\/$/, "");
const publicHref = (path: string) => `${BASE}${path.startsWith("/") ? path : `/${path}`}`;

export default function AgentPage() {
  return (
    <main className="space-y-8">
      <section className="max-w-4xl">
        <div className="eyebrow">Machine interface</div>
        <h1 className="page-title mt-2">Agent Interface</h1>
        <p className="mt-3 max-w-2xl text-sm leading-6 text-neutral-400">Static JSON contracts for market, company, event and research workflows.</p>
        <div className="mt-5 flex flex-wrap gap-2"><a href={publicHref("/agent-manifest.json")} className="pill">Manifest →</a><Link href="/ask" className="pill">Ask →</Link><Link href="/data" className="pill">Data →</Link></div>
      </section>

      <section className="grid gap-3 md:grid-cols-3">
        <div className="card p-5"><div className="eyebrow">Inputs</div><h2 className="mt-2 text-lg font-semibold text-white">Structured first</h2><p className="mt-2 text-sm leading-6 text-neutral-500">Read published market state and article scores before generating an explanation.</p></div>
        <div className="card p-5"><div className="eyebrow">Nulls</div><h2 className="mt-2 text-lg font-semibold text-white">Missing stays missing</h2><p className="mt-2 text-sm leading-6 text-neutral-500">Use observation flags where available; do not convert absent news sentiment to zero.</p></div>
        <div className="card p-5"><div className="eyebrow">Hosting</div><h2 className="mt-2 text-lg font-semibold text-white">Static by design</h2><p className="mt-2 text-sm leading-6 text-neutral-500">GitHub Pages serves versioned files rather than an always-on API.</p></div>
      </section>

      <section className="space-y-3">
        <h2 className="section-title">Resources</h2>
        <div className="table-shell overflow-x-auto">
          <table className="w-full min-w-[760px] text-sm">
            <thead className="border-b border-white/10 bg-white/[0.025] text-left text-[11px] uppercase tracking-[0.1em] text-neutral-600"><tr><th className="px-4 py-3">Resource</th><th className="px-4 py-3">Path</th><th className="px-4 py-3">Use</th></tr></thead>
            <tbody>{resources.map(([name, href, description]) => <tr key={name} className="border-b border-white/[0.06] last:border-0"><td className="px-4 py-3 font-medium text-neutral-200">{name}</td><td className="px-4 py-3"><a href={publicHref(href)} className="font-mono text-xs text-emerald-300 hover:underline">{href}</a></td><td className="px-4 py-3 text-sm leading-6 text-neutral-500">{description}</td></tr>)}</tbody>
          </table>
        </div>
      </section>

      <section className="card p-5 text-sm leading-6 text-neutral-500"><span className="font-semibold text-neutral-300">Contract:</span> preserve article scores, null semantics, true S&amp;P index pricing, additive contribution and portfolio boundaries.</section>
    </main>
  );
}
