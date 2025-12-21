// apps/web/app/portfolio/page.tsx
import path from "path";
import { promises as fs } from "fs";
import PortfolioClient from "./PortfolioClient";

export const metadata = {
  title: "Portfolio | Market Sentiment",
};

export const dynamic = "force-static";

type EquitySeries = { ticker: string; equity: number[] };

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

function computeEquityFromClose(close: number[]) {
  // close may have NaNs; treat missing as "no move" by carrying forward last valid close
  const outEq: number[] = [];
  let eq = 1;
  let lastClose: number | null = null;

  for (let i = 0; i < close.length; i++) {
    const c = Number(close[i]);
    const curClose = Number.isFinite(c) ? c : lastClose;

    if (i === 0) {
      outEq.push(1);
      lastClose = curClose ?? lastClose;
      continue;
    }

    if (lastClose != null && curClose != null && Number.isFinite(lastClose) && Number.isFinite(curClose) && lastClose !== 0) {
      const r = curClose / lastClose - 1;
      eq *= 1 + (Number.isFinite(r) ? r : 0);
    } else {
      // no info => flat
      eq *= 1;
    }

    outEq.push(eq);
    lastClose = curClose ?? lastClose;
  }

  return outEq;
}

function alignByDateFFill(targetDates: string[], sourceDates: string[], sourceValues: number[]) {
  // map date -> value
  const m = new Map<string, number>();
  for (let i = 0; i < Math.min(sourceDates.length, sourceValues.length); i++) {
    const d = sourceDates[i];
    const v = Number(sourceValues[i]);
    if (d && Number.isFinite(v)) m.set(d, v);
  }

  const out: number[] = [];
  let last: number | null = null;

  for (const d of targetDates) {
    if (m.has(d)) {
      last = m.get(d)!;
      out.push(last);
    } else if (last != null) {
      out.push(last);
    } else {
      out.push(NaN);
    }
  }

  // if leading NaNs exist, fill them to the first finite value
  const firstFinite = out.find((x) => Number.isFinite(x));
  if (firstFinite != null && Number.isFinite(firstFinite)) {
    for (let i = 0; i < out.length; i++) {
      if (Number.isFinite(out[i])) break;
      out[i] = firstFinite;
    }
  }
  return out;
}

async function readStrategy(): Promise<PortfolioStrategy | null> {
  const file = path.join(process.cwd(), "public", "data", "portfolio_strategy.json");
  const parsed = await readJsonFile(file);
  if (!parsed?.dates?.length || !parsed?.equity?.length) return null;
  return parsed;
}

async function readTickerSnapshot(dataRoot: string, ticker: string): Promise<{ dates: string[]; close: number[]; ticker: string } | null> {
  const base = path.join(dataRoot, "ticker");

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

  return {
    dates: obj.dates.map((x: any) => String(x)),
    close: obj.price.map((x: any) => Number(x)),
    ticker: obj.ticker ?? ticker,
  };
}

async function readSpxIndexBundle(): Promise<{
  symbol: string;
  dates: string[];
  close: number[];
} | null> {
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
    : [];

  // also include the "extra ^GSPX" you mentioned as fallback
  const fallbackCandidates = ["^GSPC", "^SPX", "^GSPX", "SPY"];
  const candidatesMerged = Array.from(new Set([...priceCandidates, ...fallbackCandidates]));

  const rows: any[] = parsed.daily;

  const dates: string[] = [];
  const close: number[] = [];

  for (const r of rows) {
    const d = r?.date ? String(r.date) : "";
    if (!d) continue;

    let px: number = NaN;
    for (const c of candidatesMerged) {
      const key = `close_${c}`;
      if (r[key] != null) {
        const v = Number(r[key]);
        if (Number.isFinite(v)) {
          px = v;
          break;
        }
      }
    }
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

    dates.push(d);
    close.push(Number.isFinite(px) ? px : NaN);
  }

  const hasAnyClose = close.some((x) => Number.isFinite(x));
  if (!hasAnyClose) return null;

  return { symbol, dates, close };
}

function mergeCloseSeries(
  baseDates: string[],
  baseClose: number[],
  extras: Array<{ dates: string[]; close: number[] }>
): { dates: string[]; close: number[] } {
  // Build maps, later sources used only when base missing
  const baseMap = new Map<string, number>();
  for (let i = 0; i < Math.min(baseDates.length, baseClose.length); i++) {
    const d = baseDates[i];
    const v = Number(baseClose[i]);
    if (d && Number.isFinite(v)) baseMap.set(d, v);
  }

  const extraMaps = extras.map((e) => {
    const m = new Map<string, number>();
    for (let i = 0; i < Math.min(e.dates.length, e.close.length); i++) {
      const d = e.dates[i];
      const v = Number(e.close[i]);
      if (d && Number.isFinite(v)) m.set(d, v);
    }
    return m;
  });

  // union all dates
  const allDates = new Set<string>();
  baseDates.forEach((d) => allDates.add(d));
  for (const e of extras) e.dates.forEach((d) => allDates.add(d));

  const dates = Array.from(allDates).sort(); // YYYY-MM-DD string sorts correctly
  const close: number[] = [];

  for (const d of dates) {
    if (baseMap.has(d)) {
      close.push(baseMap.get(d)!);
      continue;
    }
    let filled: number | null = null;
    for (const m of extraMaps) {
      if (m.has(d)) {
        filled = m.get(d)!;
        break;
      }
    }
    close.push(filled ?? NaN);
  }

  return { dates, close };
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

  // Build SPX price series from data/SPX/sp500_index.json
  const spxBundle = await readSpxIndexBundle();

  // Also load extra snapshots to fill missing dates/prices (your request: "also get extra ^GSPX to get everything")
  const dataRoot = path.join(process.cwd(), "public", "data");

  const [gspc, spxSnap, gspx, spy] = await Promise.all([
    readTickerSnapshot(dataRoot, "^GSPC"),
    readTickerSnapshot(dataRoot, "^SPX"),
    readTickerSnapshot(dataRoot, "^GSPX"), // in case you actually have this file
    readTickerSnapshot(dataRoot, "SPY"),
  ]);

  // 1) Benchmark series (SPY) — align + ffill to strategy dates
  const spySeries: EquitySeries | null = spy
    ? (() => {
        const eq = computeEquityFromClose(spy.close);
        const aligned = alignByDateFFill(strategy.dates, spy.dates, eq);
        return { ticker: spy.ticker ?? "SPY", equity: aligned };
      })()
    : null;

  // 2) SPX series — merge base (bundle) with extras (^GSPC/^SPX/^GSPX/SPY) then align to strategy dates
  let sp500_price_series: EquitySeries | null = null;

  if (spxBundle) {
    const merged = mergeCloseSeries(
      spxBundle.dates,
      spxBundle.close,
      [
        gspc ? { dates: gspc.dates, close: gspc.close } : { dates: [], close: [] },
        spxSnap ? { dates: spxSnap.dates, close: spxSnap.close } : { dates: [], close: [] },
        gspx ? { dates: gspx.dates, close: gspx.close } : { dates: [], close: [] },
        spy ? { dates: spy.dates, close: spy.close } : { dates: [], close: [] },
      ].filter((x) => x.dates.length)
    );

    const eq = computeEquityFromClose(merged.close);
    const alignedEq = alignByDateFFill(strategy.dates, merged.dates, eq);

    sp500_price_series = {
      ticker: spxBundle.symbol ?? "SPX",
      equity: alignedEq,
    };
  } else if (gspc) {
    // fallback: use ^GSPC if bundle missing
    const eq = computeEquityFromClose(gspc.close);
    const alignedEq = alignByDateFFill(strategy.dates, gspc.dates, eq);
    sp500_price_series = { ticker: gspc.ticker ?? "^GSPC", equity: alignedEq };
  }

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
    />
  );
}
