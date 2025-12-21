// apps/web/app/portfolio/page.tsx
import path from "path";
import { promises as fs } from "fs";
import PortfolioClient from "./PortfolioClient";

export const metadata = {
  title: "Portfolio | Market Sentiment",
};

export const dynamic = "force-static";

type EquitySeries = { ticker: string; equity: number[] };

type Sp500Index = {
  dates: string[];
  sentiment: number[];
  sentiment_ma7: number[];
};

type PortfolioStrategy = {
  meta?: any;
  metrics?: any;
  dates: string[];
  equity: number[];
  portfolio_return: number[];
  holdings?: any[];
  benchmark_series?: EquitySeries | null;
};

async function readJsonFile(file: string): Promise<any | null> {
  try {
    const raw = await fs.readFile(file, "utf-8");
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function ma7(arr: number[]) {
  const out: number[] = [];
  let run = 0;
  for (let i = 0; i < arr.length; i++) {
    const v = Number(arr[i] ?? NaN);
    run += Number.isFinite(v) ? v : 0;
    if (i >= 7) {
      const prev = Number(arr[i - 7] ?? NaN);
      run -= Number.isFinite(prev) ? prev : 0;
    }
    out.push(i >= 6 ? run / 7 : NaN);
  }
  return out;
}

function computeEquityFromPrice(price: number[]) {
  const eq: number[] = [];
  let cur = 1;
  for (let i = 0; i < price.length; i++) {
    if (i === 0) {
      eq.push(1);
      continue;
    }
    const p0 = price[i - 1];
    const p1 = price[i];
    const r = p0 && Number.isFinite(p0) && Number.isFinite(p1) ? p1 / p0 - 1 : 0;
    cur = cur * (1 + (Number.isFinite(r) ? r : 0));
    eq.push(cur);
  }
  return eq;
}

function alignByDate(targetDates: string[], sourceDates: string[], sourceValues: number[]) {
  const m = new Map<string, number>();
  for (let i = 0; i < Math.min(sourceDates.length, sourceValues.length); i++) {
    const d = sourceDates[i];
    const v = sourceValues[i];
    if (d && Number.isFinite(v)) m.set(d, v);
  }
  // If missing a day, keep it as NaN (chart will gap). You can also forward-fill if you want.
  return targetDates.map((d) => (m.has(d) ? (m.get(d) as number) : NaN));
}

async function readStrategy(): Promise<PortfolioStrategy | null> {
  const file = path.join(process.cwd(), "public", "data", "portfolio_strategy.json");
  const parsed = await readJsonFile(file);
  if (!parsed?.dates?.length || !parsed?.equity?.length) return null;
  return parsed;
}

async function readSp500Index(): Promise<Sp500Index | null> {
  // This is the same index series your homepage/sp500 page uses (cap-weighted sentiment).
  const file = path.join(process.cwd(), "public", "data", "portfolio.json");
  const parsed = await readJsonFile(file);
  if (!parsed?.dates?.length) return null;

  const dates: string[] = parsed.dates ?? [];
  const sentiment: number[] = (parsed.S ?? parsed.sentiment ?? []).map((x: any) => Number(x));

  if (!dates.length || !sentiment.length) return null;

  const n = Math.min(dates.length, sentiment.length);
  const s = sentiment.slice(0, n);
  return {
    dates: dates.slice(0, n),
    sentiment: s,
    sentiment_ma7: ma7(s),
  };
}

async function readTickerEquity(ticker: string, targetDates: string[]): Promise<EquitySeries | null> {
  const base = path.join(process.cwd(), "public", "data", "ticker");

  // Try a few filename conventions (your repo uses BRK-B for BRK.B, and some environments store ^GSPC as %5EGSPC).
  const candidates = [
    `${ticker}.json`,
    `${ticker.replace(".", "-")}.json`,
    `${ticker.replace("^", "")}.json`,
    `${encodeURIComponent(ticker)}.json`,
    `${encodeURIComponent(ticker).replace("%2E", "-")}.json`,
  ];

  let obj: any | null = null;
  for (const name of candidates) {
    obj = await readJsonFile(path.join(base, name));
    if (obj) break;
  }
  if (!obj?.dates?.length || !obj?.price?.length) return null;

  const srcDates: string[] = obj.dates;
  const price: number[] = obj.price.map((x: any) => Number(x));
  const eq = computeEquityFromPrice(price);

  const aligned = alignByDate(targetDates, srcDates, eq);
  return { ticker: obj.ticker ?? ticker, equity: aligned };
}

export default async function PortfolioPage() {
  const strategy = await readStrategy();

  if (!strategy) {
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

  const sp500Index = await readSp500Index();

  // Pull both SPY and S&P 500 index if you have them available.
  const [spySeries, spxSeries] = await Promise.all([
    readTickerEquity("SPY", strategy.dates),
    readTickerEquity("^GSPC", strategy.dates),
  ]);

  return (
    <PortfolioClient
      meta={strategy.meta}
      metrics={strategy.metrics}
      dates={strategy.dates}
      equity={strategy.equity}
      portfolio_return={strategy.portfolio_return}
      holdings={strategy.holdings ?? []}
      benchmark_series={strategy.benchmark_series ?? spySeries ?? null}
      sp500_price_series={spxSeries}
      sp500_index={sp500Index}
    />
  );
}
