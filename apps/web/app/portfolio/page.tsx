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

function sortByDateAsc(dates: string[]) {
  return [...dates].sort((a, b) => a.localeCompare(b));
}

function mean(xs: number[]) {
  if (!xs.length) return NaN;
  return xs.reduce((a, b) => a + b, 0) / xs.length;
}

function std(xs: number[]) {
  if (xs.length < 2) return NaN;
  const m = mean(xs);
  const v = xs.reduce((a, x) => a + (x - m) * (x - m), 0) / (xs.length - 1);
  return Math.sqrt(v);
}

function maxDrawdown(eq: number[]) {
  let peak = -Infinity;
  let mdd = 0;
  for (const v of eq) {
    if (!Number.isFinite(v)) continue;
    peak = Math.max(peak, v);
    if (Number.isFinite(peak) && peak > 0) {
      const dd = v / peak - 1;
      if (Number.isFinite(dd)) mdd = Math.min(mdd, dd);
    }
  }
  return Number.isFinite(mdd) ? mdd : NaN;
}

function recomputeMetricsFromSeries(eq: number[], rets: number[]) {
  const cleanR = rets.filter((x) => Number.isFinite(x));
  const n = rets.length;

  const lastEq = eq.length ? eq[eq.length - 1] : NaN;
  const cum = Number.isFinite(lastEq) ? lastEq - 1 : NaN;

  // annualization on trading days
  const annRet = Number.isFinite(lastEq) && n > 1 ? Math.pow(lastEq, 252 / n) - 1 : NaN;
  const annVol = cleanR.length ? std(cleanR) * Math.sqrt(252) : NaN;
  const sharpe = Number.isFinite(annRet) && Number.isFinite(annVol) && annVol !== 0 ? annRet / annVol : NaN;

  const hit =
    cleanR.length > 0 ? cleanR.filter((x) => x > 0).length / cleanR.length : NaN;

  return {
    cumulative_return: cum,
    annualized_return: annRet,
    annualized_vol: annVol,
    sharpe,
    max_drawdown: maxDrawdown(eq),
    hit_rate: hit,
    num_days: n,
  };
}

function alignByDateFFill(targetDates: string[], sourceDates: string[], sourceValues: number[]) {
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

  // fill leading NaNs with first finite
  const firstFinite = out.find((x) => Number.isFinite(x));
  if (firstFinite != null && Number.isFinite(firstFinite)) {
    for (let i = 0; i < out.length; i++) {
      if (Number.isFinite(out[i])) break;
      out[i] = firstFinite;
    }
  }
  return out;
}

function computeEquityFromClose(close: number[]) {
  // close can have NaN; treat missing as "no move" (carry last close)
  const eq: number[] = [];
  let curEq = 1;
  let lastClose: number | null = null;

  for (let i = 0; i < close.length; i++) {
    const c = Number(close[i]);
    const curClose = Number.isFinite(c) ? c : lastClose;

    if (i === 0) {
      eq.push(1);
      lastClose = curClose ?? lastClose;
      continue;
    }

    if (lastClose != null && curClose != null && Number.isFinite(lastClose) && Number.isFinite(curClose) && lastClose !== 0) {
      curEq *= 1 + (curClose / lastClose - 1);
    }
    eq.push(curEq);
    lastClose = curClose ?? lastClose;
  }

  return eq;
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

/**
 * Reads repo root file: data/SPX/sp500_index.json
 * daily rows contain date + close_* fields.
 */
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

  // include the extra candidates you requested as fallback
  const fallbackCandidates = ["^GSPC", "^SPX", "^GSPX", "SPY"];
  const candidatesMerged = Array.from(new Set([...priceCandidates, ...fallbackCandidates]));

  const rows: any[] = parsed.daily;

  const tmp: { date: string; close: number }[] = [];
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
    tmp.push({ date: d, close: Number.isFinite(px) ? px : NaN });
  }

  tmp.sort((a, b) => a.date.localeCompare(b.date));

  const dates = tmp.map((x) => x.date);
  const close = tmp.map((x) => x.close);

  if (!close.some((x) => Number.isFinite(x))) return null;
  return { symbol, dates, close };
}

function mergeCloseSeries(
  baseDates: string[],
  baseClose: number[],
  extras: Array<{ dates: string[]; close: number[] }>
): { dates: string[]; close: number[] } {
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

  const allDates = new Set<string>();
  baseDates.forEach((d) => allDates.add(d));
  extras.forEach((e) => e.dates.forEach((d) => allDates.add(d)));

  const dates = Array.from(allDates).sort();
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

function alignStrategyToBaseDates(
  baseDates: string[],
  stratDates: string[],
  stratEq: number[],
  stratRet: number[]
): { equity: number[]; rets: number[] } {
  const eqMap = new Map<string, number>();
  const rMap = new Map<string, number>();

  for (let i = 0; i < Math.min(stratDates.length, stratEq.length); i++) {
    const d = stratDates[i];
    const v = Number(stratEq[i]);
    if (d && Number.isFinite(v)) eqMap.set(d, v);
  }
  for (let i = 0; i < Math.min(stratDates.length, stratRet.length); i++) {
    const d = stratDates[i];
    const v = Number(stratRet[i]);
    if (d && Number.isFinite(v)) rMap.set(d, v);
  }

  const firstStratDate = stratDates.length ? sortByDateAsc(stratDates)[0] : null;

  const outEq: number[] = [];
  const outR: number[] = [];

  let lastEq: number | null = null;

  for (const d of baseDates) {
    // pad before strategy exists: flat at 1
    if (firstStratDate && d < firstStratDate) {
      outEq.push(1);
      outR.push(0);
      continue;
    }

    if (eqMap.has(d)) {
      lastEq = eqMap.get(d)!;
      outEq.push(lastEq);
      outR.push(rMap.get(d) ?? 0);
    } else if (lastEq != null) {
      // missing date in strategy calendar -> flat
      outEq.push(lastEq);
      outR.push(0);
    } else {
      // no info at all yet (shouldn’t happen once we pass firstStratDate, but safe)
      outEq.push(1);
      outR.push(0);
      lastEq = 1;
    }
  }

  return { equity: outEq, rets: outR };
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

  // 1) SPX bundle from data/SPX/sp500_index.json (this defines the required global start date)
  const spxBundle = await readSpxIndexBundle();

  // Determine SPX start date (from the file itself)
  const spxStartDate = spxBundle?.dates?.length ? spxBundle.dates[0] : null;

  // 2) Load snapshots used to fill missing SPX points + build SPY benchmark
  const dataRoot = path.join(process.cwd(), "public", "data");

  const [gspc, spxSnap, gspx, spy] = await Promise.all([
    readTickerSnapshot(dataRoot, "^GSPC"),
    readTickerSnapshot(dataRoot, "^SPX"),
    readTickerSnapshot(dataRoot, "^GSPX"),
    readTickerSnapshot(dataRoot, "SPY"),
  ]);

  // Merge SPX close series with extra sources (only for filling missing values/dates)
  let mergedSpx: { dates: string[]; close: number[] } | null = null;
  if (spxBundle) {
    mergedSpx = mergeCloseSeries(
      spxBundle.dates,
      spxBundle.close,
      [
        gspc ? { dates: gspc.dates, close: gspc.close } : null,
        spxSnap ? { dates: spxSnap.dates, close: spxSnap.close } : null,
        gspx ? { dates: gspx.dates, close: gspx.close } : null,
        spy ? { dates: spy.dates, close: spy.close } : null,
      ].filter(Boolean) as Array<{ dates: string[]; close: number[] }>
    );
  }

  // Strategy end date
  const stratLastDate = strategy.dates?.length ? strategy.dates[strategy.dates.length - 1] : null;

  // 3) Build the portfolio page date axis:
  // Start = SPX start date (required), End = strategy last date (so we don’t show flat strategy beyond its computed horizon)
  let baseDates = strategy.dates;

  if (spxStartDate && mergedSpx?.dates?.length && stratLastDate) {
    baseDates = mergedSpx.dates.filter((d) => d >= spxStartDate && d <= stratLastDate);
    // safety: ensure sorted unique
    baseDates = Array.from(new Set(baseDates)).sort();
  } else if (spxStartDate && stratLastDate) {
    // fallback: use strategy calendar but clipped to SPX start
    baseDates = strategy.dates.filter((d) => d >= spxStartDate && d <= stratLastDate);
  }

  // 4) Align strategy to baseDates (pad early dates to start at SPX start)
  const alignedStrat = alignStrategyToBaseDates(
    baseDates,
    strategy.dates,
    strategy.equity,
    strategy.portfolio_return
  );

  const metrics = recomputeMetricsFromSeries(alignedStrat.equity, alignedStrat.rets);

  // 5) Build SPY benchmark equity aligned to baseDates
  const spySeries: EquitySeries | null = spy
    ? (() => {
        const eq = computeEquityFromClose(spy.close);
        const aligned = alignByDateFFill(baseDates, spy.dates, eq);
        return { ticker: spy.ticker ?? "SPY", equity: aligned };
      })()
    : null;

  // 6) Build SPX equity aligned to baseDates
  const sp500_price_series: EquitySeries | null =
    mergedSpx?.dates?.length && mergedSpx.close?.length
      ? (() => {
          const eq = computeEquityFromClose(mergedSpx!.close);
          const aligned = alignByDateFFill(baseDates, mergedSpx!.dates, eq);
          return { ticker: spxBundle?.symbol ?? "SPX", equity: aligned };
        })()
      : null;

  return (
    <PortfolioClient
      meta={strategy.meta}
      metrics={metrics} // recomputed on the aligned (SPX-start) timeline
      dates={baseDates}
      equity={alignedStrat.equity}
      portfolio_return={alignedStrat.rets}
      holdings={strategy.holdings ?? []}
      benchmark_series={strategy.benchmark_series ?? spySeries ?? null}
      sp500_price_series={sp500_price_series}
    />
  );
}
