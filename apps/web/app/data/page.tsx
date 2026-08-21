import type { Metadata } from "next";
import Link from "next/link";

export const metadata: Metadata = { title: "Data", description: "Public market, company, earnings, event and research JSON contracts." };
const basePath = process.env.NEXT_PUBLIC_BASE_PATH || "";

const endpoints = [
  { name: "U.S. company universe", path: "/data/v5/universe.json", shape: "ExtendedUniverse", description: "S&P Composite 1500 plus broader U.S. company metadata and current coverage fields." },
  { name: "Company news archive", path: "/data/v5/news/AAPL.json", shape: "CompanyNews", description: "Retained deduplicated company headlines with FinBERT article scores where available." },
  { name: "Company daily history", path: "/data/v5/history/AAPL.json", shape: "CompanyHistory", description: "Extended daily price history with observed sentiment and explicit sentiment_observed flags." },
  { name: "Company data coverage", path: "/data/v5/company_data_coverage.json", shape: "CompanyDataCoverage", description: "Universe-wide news/history readiness and per-company fulfillment status." },
  { name: "Company earnings", path: "/data/v5/earnings/AAPL.json", shape: "EarningsArtifact", description: "EPS history, structured call diagnostics when a free transcript is available, public source links and filing evidence." },
  { name: "Earnings coverage", path: "/data/v5/earnings_coverage.json", shape: "EarningsCoverage", description: "Complete, partial, link-only and missing structured-call coverage across the company universe." },
  { name: "Event instances", path: "/data/v5/event_instances.json", shape: "EventStoreV3", description: "Persistent company/theme events clustered from retained article evidence." },
  { name: "Multi-index sentiment", path: "/data/v5/index_sentiment.json", shape: "IndexSentiment", description: "Observed sentiment for S&P 500, MidCap 400, SmallCap 600, Composite 1500 and the broader U.S. layer." },
  { name: "Core ticker list", path: "/data/_tickers.json", shape: "string[]", description: "Symbols with original S&P-core ticker snapshots." },
  { name: "Core ticker snapshot", path: "/data/ticker/AAPL.json", shape: "TickerSnapshot", description: "Core S&P price/sentiment history and scored article evidence." },
  { name: "S&P 500 index", path: "/data/SPX/sp500_index.json", shape: "Sp500Index", description: "True-index close, aggregate observed sentiment and coverage." },
  { name: "S&P 500 constituents", path: "/data/SPX/sp500_heatmap.json", shape: "HeatmapSnapshot", description: "Weights, classifications, returns and latest constituent sentiment." },
  { name: "Portfolio strategy", path: "/data/portfolio_strategy.json", shape: "PortfolioBacktest", description: "S&P-core portfolio equity, holdings, turnover, costs and metrics." },
  { name: "Research index", path: "/research/index.json", shape: "ResearchIndexItem[]", description: "Generated empirical-study catalog." },
];

const principles = [
  ["Missing stays missing", "No-news observations are not converted to neutral zero."],
  ["Universe boundary", "Broader company coverage does not alter S&P 500 weighting, attribution, portfolio or Research Lab contracts."],
  ["Coverage is published", "Company news/history and earnings coverage are explicit artifacts, not hidden assumptions."],
  ["Transcript restraint", "Public artifacts keep derived call metrics and source URLs, not third-party transcript body text."],
];

export default function DataPage() {
  return <main className="space-y-9">
    <section className="max-w-4xl"><div className="eyebrow">Public JSON</div><h1 className="page-title mt-3">Data contracts</h1><p className="mt-3 max-w-2xl text-sm leading-6 text-neutral-400">Inspect the same market and company artifacts used by the site, research tools and machine interface.</p><div className="mt-5 flex flex-wrap gap-2"><Link href="/methodology" className="pill">Methodology →</Link><Link href="/agent" className="pill">Agent interface →</Link></div></section>
    <section className="space-y-4"><h2 className="section-title">Published endpoints</h2><div className="grid gap-3 lg:grid-cols-2">{endpoints.map((endpoint) => { const href = `${basePath}${endpoint.path}`; return <a key={endpoint.path} href={href} className="card card-hover block p-5"><div className="flex items-start justify-between gap-4"><div><div className="font-semibold text-white">{endpoint.name}</div><code className="mt-2 block break-all text-xs text-emerald-300">{endpoint.path}</code></div><span className="rounded-lg border border-sky-400/20 bg-sky-400/10 px-2 py-1 text-[10px] font-semibold uppercase tracking-wider text-sky-300">JSON</span></div><p className="mt-3 text-sm leading-6 text-neutral-500">{endpoint.description}</p><div className="mt-3 text-xs text-neutral-700">{endpoint.shape}</div></a>; })}</div></section>
    <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">{principles.map(([title, copy]) => <div key={title} className="card p-4"><div className="text-sm font-semibold text-neutral-200">{title}</div><p className="mt-2 text-xs leading-5 text-neutral-500">{copy}</p></div>)}</section>
  </main>;
}