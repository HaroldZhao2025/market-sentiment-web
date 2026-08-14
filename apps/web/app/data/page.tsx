import type { Metadata } from "next";
import Link from "next/link";

export const metadata: Metadata = {
  title: "Data",
  description: "Public market, company, earnings, event and research JSON contracts.",
};

const basePath = process.env.NEXT_PUBLIC_BASE_PATH || "";

const endpoints = [
  { name: "Composite company universe", path: "/data/v5/universe.json", shape: "ExtendedUniverse", description: "Large, mid and small-cap company metadata, price, return and coverage fields." },
  { name: "Company news", path: "/data/v5/news/AAPL.json", shape: "CompanyNews", description: "Up to 60 deduplicated recent articles with article sentiment where scored." },
  { name: "Company earnings", path: "/data/v5/earnings/AAPL.json", shape: "EarningsArtifact", description: "Earnings history, call analysis when available, and filing fallback." },
  { name: "Event instances", path: "/data/v5/event_instances.json", shape: "EventStoreV3", description: "Article events clustered into company/theme event instances." },
  { name: "Core ticker list", path: "/data/_tickers.json", shape: "string[]", description: "Symbols with full historical ticker snapshots." },
  { name: "Ticker snapshot", path: "/data/ticker/AAPL.json", shape: "TickerSnapshot", description: "Price, sentiment observations and recent scored headlines." },
  { name: "S&P 500 index", path: "/data/SPX/sp500_index.json", shape: "Sp500Index", description: "True-index close, aggregate sentiment and coverage." },
  { name: "S&P 500 constituents", path: "/data/SPX/sp500_heatmap.json", shape: "HeatmapSnapshot", description: "Weights, classifications, returns and latest constituent sentiment." },
  { name: "Portfolio strategy", path: "/data/portfolio_strategy.json", shape: "PortfolioBacktest", description: "Equity curve, holdings, exposure, turnover, costs and metrics." },
  { name: "Research index", path: "/research/index.json", shape: "ResearchIndexItem[]", description: "Generated empirical-study catalog." },
];

const principles = [
  ["Missing stays missing", "No-news observations are not converted to zero."],
  ["Aggregation is explicit", "Article → ticker-day → observed-only index aggregation."],
  ["Coverage travels with signal", "Index sentiment is paired with ticker and market-cap coverage."],
  ["One published layer", "The site, research tools and agents read the same artifact family."],
];

export default function DataPage() {
  return (
    <main className="space-y-9">
      <section className="max-w-4xl">
        <div className="eyebrow">Public JSON</div>
        <h1 className="page-title mt-3">Data contracts</h1>
        <p className="mt-3 max-w-2xl text-sm leading-6 text-neutral-400">Inspect or reuse the market and company artifacts behind the site.</p>
        <div className="mt-5 flex flex-wrap gap-2"><Link href="/methodology" className="pill">Methodology →</Link><Link href="/agent" className="pill">Agent interface →</Link></div>
      </section>

      <section className="space-y-4">
        <h2 className="section-title">Published endpoints</h2>
        <div className="grid gap-3 lg:grid-cols-2">
          {endpoints.map((endpoint) => {
            const href = `${basePath}${endpoint.path}`;
            return (
              <a key={endpoint.path} href={href} className="card card-hover block p-5">
                <div className="flex items-start justify-between gap-4"><div><div className="font-semibold text-white">{endpoint.name}</div><code className="mt-2 block break-all text-xs text-emerald-300">{endpoint.path}</code></div><span className="rounded-lg border border-sky-400/20 bg-sky-400/10 px-2 py-1 text-[10px] font-semibold uppercase tracking-wider text-sky-300">JSON</span></div>
                <p className="mt-3 text-sm leading-6 text-neutral-500">{endpoint.description}</p>
                <div className="mt-3 text-xs text-neutral-700">{endpoint.shape}</div>
              </a>
            );
          })}
        </div>
      </section>

      <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        {principles.map(([title, copy]) => <div key={title} className="card p-4"><div className="text-sm font-semibold text-neutral-200">{title}</div><p className="mt-2 text-xs leading-5 text-neutral-500">{copy}</p></div>)}
      </section>
    </main>
  );
}
