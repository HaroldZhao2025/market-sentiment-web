// apps/web/app/portfolio/page.tsx
import path from "path";
import { promises as fs } from "fs";
import PortfolioClient from "./PortfolioClient";

export const metadata = {
  title: "Portfolio | Market Sentiment",
};

export const dynamic = "force-static";

type PortfolioStrategy = {
  meta?: any;
  metrics?: any;
  dates: string[];
  equity: number[];
  portfolio_return: number[];
  holdings?: any[];
  benchmark_series?: { ticker: string; equity: number[] };
};

async function readStrategy(): Promise<PortfolioStrategy | null> {
  const file = path.join(process.cwd(), "public", "data", "portfolio_strategy.json");
  try {
    const raw = await fs.readFile(file, "utf-8");
    const parsed = JSON.parse(raw);
    if (!parsed?.dates?.length || !parsed?.equity?.length) return null;
    return parsed;
  } catch {
    return null;
  }
}

export default async function PortfolioPage() {
  const data = await readStrategy();

  if (!data) {
    return (
      <main className="mx-auto max-w-3xl px-4 py-10 space-y-3">
        <h1 className="text-2xl font-bold">Portfolio</h1>
        <p className="text-neutral-700">
          Missing <code className="px-1 py-0.5 rounded bg-neutral-100">public/data/portfolio_strategy.json</code>.
        </p>
        <p className="text-neutral-700">
          Run{" "}
          <code className="px-1 py-0.5 rounded bg-neutral-100">
            python -m market_sentiment.cli.build_portfolio ...
          </code>{" "}
          after <code className="px-1 py-0.5 rounded bg-neutral-100">build_json</code>.
        </p>
      </main>
    );
  }

  return (
    <PortfolioClient
      meta={data.meta}
      metrics={data.metrics}
      dates={data.dates}
      equity={data.equity}
      portfolio_return={data.portfolio_return}
      holdings={data.holdings ?? []}
      benchmark_series={data.benchmark_series}
    />
  );
}
