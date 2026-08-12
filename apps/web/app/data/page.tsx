import type { Metadata } from "next";
import Link from "next/link";

export const metadata: Metadata = {
  title: "Data",
  description: "Machine-readable market sentiment, S&P 500, portfolio, and research data contracts.",
};

const basePath = process.env.NEXT_PUBLIC_BASE_PATH || "";

const endpoints = [
  {
    name: "Ticker universe",
    path: "/data/_tickers.json",
    shape: "string[]",
    description: "Symbols available in the current generated web snapshot.",
  },
  {
    name: "Ticker intelligence",
    path: "/data/ticker/AAPL.json",
    shape: "TickerSnapshot",
    description: "Price series, observed sentiment series, news counts, and scored recent headlines for one symbol.",
  },
  {
    name: "S&P 500 index intelligence",
    path: "/data/SPX/sp500_index.json",
    shape: "Sp500Index",
    description: "Index close, cap-weighted sentiment, equal-weighted sentiment, coverage, and news-observation diagnostics.",
  },
  {
    name: "S&P 500 heatmap",
    path: "/data/SPX/sp500_heatmap.json",
    shape: "HeatmapSnapshot",
    description: "Constituent classifications, weights, price returns, and latest observed sentiment where available.",
  },
  {
    name: "Portfolio strategy",
    path: "/data/portfolio_strategy.json",
    shape: "PortfolioBacktest",
    description: "Strategy equity curve, returns, holdings, exposure, turnover, costs, and performance metrics.",
  },
  {
    name: "Research index",
    path: "/research/index.json",
    shape: "ResearchIndexItem[]",
    description: "Discoverable catalog of generated empirical studies and their latest metadata.",
  },
];

const principles = [
  ["Missing is not neutral", "A no-news day is not silently converted into sentiment = 0 in the live intelligence layer."],
  ["Observed before aggregated", "Article scores roll into ticker-day observations; only observed tickers enter an index sentiment denominator."],
  ["Coverage travels with the signal", "Index sentiment should be interpreted beside observed ticker count and market-cap coverage."],
  ["Static, inspectable artifacts", "Published JSON is the same artifact family used by the website, portfolio layer, and research pages."],
];

export default function DataPage() {
  return (
    <main className="space-y-10">
      <section className="max-w-4xl">
        <div className="eyebrow">Machine-readable layer</div>
        <h1 className="page-title mt-3">Data that humans and agents can inspect.</h1>
        <p className="mt-4 max-w-3xl text-base leading-7 text-neutral-400">
          The website is one client of the data. These static JSON artifacts make the underlying market-intelligence state explicit, auditable, and easy to consume without scraping charts.
        </p>
        <div className="mt-6 flex flex-wrap gap-2">
          <Link href="/methodology" className="pill">Read methodology →</Link>
          <Link href="/research" className="pill">Open Research Lab →</Link>
        </div>
      </section>

      <section className="space-y-4">
        <div>
          <h2 className="section-title">Published endpoints</h2>
          <p className="section-copy">Paths below are stable public artifacts under the GitHub Pages deployment.</p>
        </div>

        <div className="grid gap-4 lg:grid-cols-2">
          {endpoints.map((endpoint) => {
            const href = `${basePath}${endpoint.path}`;
            return (
              <a key={endpoint.path} href={href} className="card card-hover block p-5">
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <div className="font-semibold text-white">{endpoint.name}</div>
                    <code className="mt-2 block break-all text-xs text-emerald-300">{endpoint.path}</code>
                  </div>
                  <span className="rounded-lg border border-sky-400/20 bg-sky-400/10 px-2 py-1 text-[10px] font-semibold uppercase tracking-wider text-sky-300">
                    JSON
                  </span>
                </div>
                <p className="mt-4 text-sm leading-6 text-neutral-400">{endpoint.description}</p>
                <div className="mt-4 text-xs text-neutral-600">Shape: <code className="text-neutral-400">{endpoint.shape}</code></div>
              </a>
            );
          })}
        </div>
      </section>

      <section className="space-y-4">
        <div>
          <h2 className="section-title">Contract principles</h2>
          <p className="section-copy">The goal is to make model outputs interpretable enough to reuse in research, screens, and future agent tools.</p>
        </div>
        <div className="grid gap-3 md:grid-cols-2">
          {principles.map(([title, copy]) => (
            <div key={title} className="card p-5">
              <div className="text-sm font-semibold text-neutral-200">{title}</div>
              <p className="mt-2 text-sm leading-6 text-neutral-500">{copy}</p>
            </div>
          ))}
        </div>
      </section>

      <section className="card p-6">
        <div className="eyebrow">Agent-ready direction</div>
        <h2 className="mt-2 text-xl font-semibold text-white">The next interface will be queryable, not just browsable.</h2>
        <p className="mt-3 max-w-3xl text-sm leading-6 text-neutral-400">
          Phase 1 exposes the canonical artifacts. The next step is a deterministic screening and backtest interface that an LLM can translate natural-language questions into, while the actual filtering and calculations remain reproducible code.
        </p>
      </section>
    </main>
  );
}
