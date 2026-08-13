import Link from "next/link";

export const dynamic = "force-static";

export const metadata = {
  title: "Agent Interface",
  description: "Machine-readable Sentiment Intelligence contracts for external agents and research workflows.",
};

const resources = [
  ["Agent manifest", "/agent-manifest.json", "Stable contract registry, methodology semantics, resource paths, and query fields."],
  ["S&P index", "/data/SPX/sp500_index.json", "Daily true-index close, observed sentiment, coverage, and evidence diagnostics."],
  ["S&P constituents", "/data/SPX/sp500_heatmap.json", "Latest company metadata, weights, price reaction, sentiment, and news evidence."],
  ["Ticker universe", "/data/_tickers.json", "Generated ticker snapshot inventory."],
  ["Research index", "/research/index.json", "Generated empirical-study registry when available."],
];

const BASE = (process.env.NEXT_PUBLIC_BASE_PATH || "").replace(/\/$/, "");
const publicHref = (path: string) => `${BASE}${path.startsWith("/") ? path : `/${path}`}`;

export default function AgentPage() {
  return (
    <main className="space-y-8">
      <section className="max-w-4xl">
        <div className="eyebrow">Phase 4 · machine interface</div>
        <h1 className="page-title mt-2">Agent Interface</h1>
        <p className="mt-3 max-w-3xl text-sm leading-6 text-neutral-400">
          Sentiment Intelligence exposes deterministic facts as static JSON contracts that can be consumed by external LLMs, agents, notebooks, and research pipelines without requiring them to scrape presentation text or reinterpret missing values.
        </p>
        <div className="mt-5 flex flex-wrap gap-2">
          <a href={publicHref("/agent-manifest.json")} className="pill">Open agent manifest →</a>
          <Link href="/ask" className="pill">Ask the Market →</Link>
          <Link href="/data" className="pill">Data contracts →</Link>
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-3">
        <div className="card p-5"><div className="eyebrow">Deterministic first</div><h2 className="mt-2 text-lg font-semibold text-white">Facts before narration</h2><p className="mt-2 text-sm leading-6 text-neutral-500">Agents should read structured market state, constituent evidence, article scores, event diagnostics, and research results first. Any LLM explanation should remain downstream of these facts.</p></div>
        <div className="card p-5"><div className="eyebrow">Missing data</div><h2 className="mt-2 text-lg font-semibold text-white">Missing is a state</h2><p className="mt-2 text-sm leading-6 text-neutral-500">No news is not neutral news. Agent consumers must preserve null/missing sentiment and use observation flags where available instead of coercing gaps to zero.</p></div>
        <div className="card p-5"><div className="eyebrow">Static architecture</div><h2 className="mt-2 text-lg font-semibold text-white">GitHub Pages compatible</h2><p className="mt-2 text-sm leading-6 text-neutral-500">This deployment has no always-on backend. The agent interface therefore uses stable static JSON resources and manifests rather than pretending to offer a dynamic server API.</p></div>
      </section>

      <section className="space-y-3">
        <div><div className="eyebrow">Resource registry</div><h2 className="section-title mt-1">Machine-readable endpoints</h2><p className="section-copy">Paths below are relative to the deployed Sentiment Intelligence base path.</p></div>
        <div className="table-shell overflow-x-auto">
          <table className="w-full min-w-[760px] text-sm">
            <thead className="border-b border-white/10 bg-white/[0.025] text-left text-[11px] uppercase tracking-[0.1em] text-neutral-600"><tr><th className="px-4 py-3">Resource</th><th className="px-4 py-3">Path</th><th className="px-4 py-3">Purpose</th></tr></thead>
            <tbody>{resources.map(([name, href, description]) => <tr key={name} className="border-b border-white/[0.06] last:border-0"><td className="px-4 py-3 font-medium text-neutral-200">{name}</td><td className="px-4 py-3"><a href={publicHref(href)} className="font-mono text-xs text-emerald-300 hover:underline">{href}</a></td><td className="px-4 py-3 text-sm leading-6 text-neutral-500">{description}</td></tr>)}</tbody>
          </table>
        </div>
      </section>

      <section className="card p-5 text-sm leading-6 text-neutral-500">
        <div className="font-semibold text-neutral-300">Consumer contract</div>
        <p className="mt-2">Treat <code>agent-manifest.json</code> as the discovery document. Preserve article-level FinBERT scores, no-news missingness, true S&P index pricing, additive contribution semantics, and portfolio authorship/strategy boundaries. The natural-language `/ask` layer is a deterministic convenience interface, not a replacement for the data contract.</p>
      </section>
    </main>
  );
}
