import type { Metadata } from "next";
import Link from "next/link";

export const metadata: Metadata = { title: "Data", description: "Public market, company, earnings, event and research JSON contracts." };
const basePath = process.env.NEXT_PUBLIC_BASE_PATH || "";

const endpoints = [
  { name: "Composite company universe", path: "/data/v5/universe.json", shape: "ExtendedUniverse", description: "S&P 500, MidCap 400 and SmallCap 600 metadata plus latest company fields." },
  { name: "Company news archive", path: "/data/v5/news/AAPL.json", shape: "CompanyNews", description: "Up to 120 retained, deduplicated free-public headlines with FinBERT scores where available." },
  { name: "Company daily history", path: "/data/v5/history/AAPL.json", shape: "CompanyHistory", description: "Extended daily price history with observed-only sentiment and explicit observation flags." },
  { name: "Company data coverage", path: "/data/v5/company_data_coverage.json", shape: "CompanyDataCoverage", description: "Universe-wide news/history readiness and per-company fulfillment status." },
  { name: "Company earnings", path: "/data/v5/earnings/AAPL.json", shape: "EarningsArtifact", description: "EPS history, structured call diagnostics when a free transcript is available, public source links and filing evidence." },
  { name: "Earnings coverage", path: "/data/v5/earnings_coverage.json", shape: "EarningsCoverage", description: "Complete, partial, link-only and missing structured-call coverage across the company universe." },
  { name: "Event instances", path: "/data/v5/event_instances.json", shape: "EventStoreV3", description: "Persistent company/theme events clustered from retained article evidence." },
  { name: "Core ticker list", path: "/data/_tickers.json", shape: "string[]", description: "Symbols with the original full S&P ticker snapshots." },
  { name: "Core ticker snapshot", path: "/data/ticker/AAPL.json", shape: "TickerSnapshot", description: "Core S&P price/sentiment history and recent scored article evidence." },
  { name: "S&P 500 index", path: "/data/SPX/sp500_index.json", shape: "Sp500Index", description: "True-index close, aggregate sentiment and coverage." },
  { name: "S&P 500 constituents", path: "/data/SPX/sp500_heatmap.json", shape: "HeatmapSnapshot", description: "Weights, classifications, returns and latest constituent sentiment." },
  { name: "Portfolio strategy", path: "/data/portfolio_strategy.json", shape: "PortfolioBacktest", description: "Equity curve, holdings, exposure, turnover, costs and metrics." },
  { name: "Research index", path: "/research/index.json", shape: "ResearchIndexItem[]", description: "Generated empirical-study catalog." },
];

const principles = [
  ["Free public sources", "Company news and earnings fulfillment do not depend on paid/Premium feeds."],
  ["Missing stays missing", "No-news observations are not converted to neutral zero."],
  ["Core boundary", "Composite 1500 company coverage does not alter S&P 500 weighting, attribution, portfolio or research contracts."],
  ["Transcript restraint", "Third-party transcript body text is analyzed transiently; published artifacts keep derived metrics and source URLs."],
];

export default function DataPage() {
  return (
    <main className="space-y-9">
      <section className="max-w-4xl"><div className="eyebrow">Public JSON</div><h1 className="page-title mt-3">Data contracts</h1><p className="mt-3 max-w-2xl text-sm leading-6 text-neutral-400">Inspect the same artifacts used by the website, research tools and machine interface.</p><div className="mt-5 flex flex-wrap gap-2"><Link href="/methodology" className="pill">Methodology →</Link><Link href="/agent" className="pill">Agent interface →</Link></div></section>

      <section className="space-y-4"><h2 className="section-title">Published endpoints</h2><div className="grid gap-3 lg:grid-cols-2">{endpoints.map((endpoint) => { const href = `${basePath}${endpoint.path}`; return <a key={endpoint.path} href={href} className="card card-hover block p-5"><div className="flex items-start justify-between gap-4"><div><div className="font-semibold text-white">{endpoint.name}</div><code className="mt-2 block break-all text-xs text-emerald-300">{endpoint.path}</code></div><span className="rounded-lg border border-sky-400/20 bg-sky-400/10 px-2 py-1 text-[10px] font-semibold uppercase tracking-wider text-sky-300">JSON</span></div><p className="mt-3 text-sm leading-6 text-neutral-500">{endpoint.description}</p><div className="mt-3 text-xs text-neutral-700">{endpoint.shape}</div></a>; })}</div></section>

      <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">{principles.map(([title, copy]) => <div key={title} className="card p-4"><div className="text-sm font-semibold text-neutral-200">{title}</div><p className="mt-2 text-xs leading-5 text-neutral-500">{copy}</p></div>)}</section>
    </main>
  );
}
