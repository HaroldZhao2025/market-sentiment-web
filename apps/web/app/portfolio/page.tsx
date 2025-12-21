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

function computeEquityFromClose(close: number[]) {
  const eq: number[] = [];
  let cur = 1;
  for (let i = 0; i < close.length; i++) {
    if (i === 0) {
      eq.push(1);
      continue;
    }
    const p0 = close[i - 1];
    const p1 = close[i];
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
  return targetDates.map((d) => (m.has(d) ? (m.get(d) as number) : NaN));
}

async function readStrategy(): Promise<PortfolioStrategy | null> {
  const file = path.join(process.cwd(), "public", "data", "portfolio_strategy.json");
  const parsed = await readJsonFile(file);
  if (!parsed?.dates?.length || !parsed?.equity?.length) return null;
  return parsed;
}

/**
 * Read SPX index bundle from repo file:
 *   data/SPX/sp500_index.json
 *
 * Example shape:
 * {
 *   "symbol": "SPX",
 *   "price_symbol_candidates": ["^GSPC","^SPX","SPY"],
 *   "daily": [
 *     { "date":"2024-12-23", "close_^GSPC": 5974.06, "sentiment_cap_weighted": 0.0624 },
 *     ...
 *   ]
 * }
 */
async function readSpxIndexBundle(): Promise<{
  symbol: string;
  dates: string[];
  close: number[];
  sentiment: number[];
} | null> {
  // In a monorepo, process.cwd() during next build is typically apps/web
  // so repo root is ../../
  const candidates = [
    path.join(process.cwd(), "data", "SPX", "sp500_index.json"),
    path.join(process.cwd(), "..", "..", "data", "SPX", "sp500_index.json"),
  ];

  let parsed: any | null = null;
  for (const file of candidates) {
    parsed = await readJsonFile(file);
    if (parsed) break;
  }
  if (!parsed?.daily?.length) return null;

  const symbol = String(parsed.symbol ?? "SPX");
  const priceCandidates: string[] = Array.isArray(parsed.price_symbol_candidates)
    ? parsed.price_symbol_candidates.map((x: any) => String(x))
    : ["^GSPC", "^SPX", "SPY"];

  const rows: any[] = parsed.daily;
  const dates: string[] = [];
  const close: number[] = [];
  const sentiment: number[] = [];

  for (const r of rows) {
    const d = r?.date ? String(r.date) : "";
    if (!d) continue;

    // pick close field by candidate order: close_^GSPC, close_^SPX, close_SPY, ...
    let px: number = NaN;
    for (const c of priceCandidates) {
      const key = `close_${c}`;
      if (r[key] != null) {
        const v = Number(r[key]);
        if (Number.isFinite(v)) {
          px = v;
          break;
        }
      }
    }
    // fallback: any close_* field
    if (!Number.isFinite(px)) {
      for (const k of Object.keys(r)) {
        if (!k.startsWith("close_")) continue;
        const v = Number(r[k]);
        if (Number.isFinite(v)) {
          px = v;
          break;
        }
      }
    }

    const s = Number(r?.sentiment_cap_weighted);

    dates.push(d);
    close.push(Number.isFinite(px) ? px : NaN);
    sentiment.push(Number.isFinite(s) ? s : NaN);
  }

  // must have at least some close or sentiment
  const hasAnyClose = close.some((x) => Number.isFinite(x));
  const hasAnySent = sentiment.some((x) => Number.isFinite(x));
  if (!hasAnyClose && !hasAnySent) return null;

  return { symbol, dates, close, sentiment };
}

async function readTickerEquityFromSnapshots(ticker: string, targetDates: string[]): Promise<EquitySeries | null> {
  const base = path.join(process.cwd(), "public", "data", "ticker");

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
  const eq = computeEquityFromClose(price);

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

  // 1) SPX bundle from data/SPX/sp500_index.json
  const spxBundle = await readSpxIndexBundle();

  // Build sentiment index (for the sentiment chart)
  const sp500_index: Sp500Index | null =
    spxBundle?.dates?.length && spxBundle?.sentiment?.length
      ? {
          dates: spxBundle.dates,
          sentiment: spxBundle.sentiment,
          sentiment_ma7: ma7(spxBundle.sentiment),
        }
      : null;

  // Build SPX equity curve (for the "all graphs" comparison chart)
  const sp500_price_series: EquitySeries | null =
    spxBundle?.dates?.length && spxBundle?.close?.length
      ? (() => {
          const eq = computeEquityFromClose(spxBundle.close);
          const aligned = alignByDate(strategy.dates, spxBundle.dates, eq);
          return { ticker: spxBundle.symbol ?? "SPX", equity: aligned };
        })()
      : null;

  // 2) SPY benchmark from snapshots (optional fallback)
  const spySeries = await readTickerEquityFromSnapshots("SPY", strategy.dates);

  return (
    <PortfolioClient
      meta={strategy.meta}
      metrics={strategy.metrics}
      dates={strategy.dates}
      equity={strategy.equity}
      portfolio_return={strategy.portfolio_return}
      holdings={strategy.holdings ?? []}
      benchmark_series={strategy.benchmark_series ?? spySeries ?? null}
      sp500_price_series={sp500_price_series}
      sp500_index={sp500_index}
    />
  );
}
