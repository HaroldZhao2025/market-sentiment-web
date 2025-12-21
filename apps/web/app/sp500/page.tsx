import fs from "node:fs";
import path from "node:path";
import Link from "next/link";
import Sp500Client from "./Sp500Client";
import Sp500HeatmapClient from "./Sp500HeatmapClient";

export const dynamic = "force-static";

type DailyRow = {
  date: string;
  sentiment_cap_weighted?: number;
  [k: string]: unknown;
};

type Sp500IndexFile = {
  symbol: string;
  name: string;
  price_symbol_candidates?: string[];
  news_symbol_candidates?: string[];
  daily: DailyRow[];
};

type HeatmapRow = {
  symbol: string;
  name: string;
  sector: string;
  industry: string;
  market_cap: number;
  weight: number;
  sentiment: number | null;
  close: number | null;
  prev_close: number | null;
  return_1d: number | null;
};

type HeatmapSnapshot = {
  date: string;
  rows: HeatmapRow[];
  sector_stats: Array<{
    sector: string;
    weight_sum: number;
    market_cap_sum: number;
    sentiment_wavg: number | null;
    return_wavg: number | null;
    contribution_sum: number;
    n: number;
  }>;
};

type Sp500HeatmapFile = {
  symbol: string;
  name: string;
  asof: { latest_trading_day: string; current: string };
  snapshots: {
    latest_trading_day: HeatmapSnapshot;
    current: HeatmapSnapshot;
  };
};

function safeReadJson<T>(absPath: string): T | null {
  try {
    if (!fs.existsSync(absPath)) return null;
    const raw = fs.readFileSync(absPath, "utf-8");
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

function readSp500Index(): Sp500IndexFile | null {
  const candidates = [
    path.resolve(process.cwd(), "../../data/SPX/sp500_index.json"),
    path.resolve(process.cwd(), "public/data/SPX/sp500_index.json"),
    path.resolve(process.cwd(), "public/data/sp500_index.json"),
  ];
  for (const p of candidates) {
    const parsed = safeReadJson<Sp500IndexFile>(p);
    if (parsed && Array.isArray(parsed.daily)) return parsed;
  }
  return null;
}

function readSp500Heatmap(): Sp500HeatmapFile | null {
  const candidates = [
    path.resolve(process.cwd(), "../../data/SPX/sp500_heatmap.json"),
    path.resolve(process.cwd(), "public/data/SPX/sp500_heatmap.json"),
    path.resolve(process.cwd(), "public/data/sp500_heatmap.json"),
  ];
  for (const p of candidates) {
    const parsed = safeReadJson<Sp500HeatmapFile>(p);
    if (parsed?.snapshots?.latest_trading_day?.rows?.length) return parsed;
  }
  return null;
}

function fmtNum(x: unknown, digits = 4): string {
  const n = typeof x === "number" ? x : Number(x);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function fmtMoney(x: unknown, digits = 2): string {
  const n = typeof x === "number" ? x : Number(x);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function avg(arr: number[]): number | null {
  if (!arr.length) return null;
  const s = arr.reduce((a, b) => a + b, 0);
  return s / arr.length;
}

export default function Sp500Page() {
  const data = readSp500Index();
  const heatmap = readSp500Heatmap();

  if (!data) {
    return (
      <div className="space-y-4">
        <h1 className="text-2xl font-semibold">S&amp;P 500 (SPX)</h1>
        <p className="text-gray-700">
          Could not find <code>data/SPX/sp500_index.json</code> at build time.
        </p>
        <p className="text-sm">
          Go back <Link className="underline" href="/">Home</Link>.
        </p>
      </div>
    );
  }

  const daily = [...data.daily].sort((a, b) => a.date.localeCompare(b.date));
  const latest = daily[daily.length - 1];

  const closeKey = Object.keys(latest || {}).find((k) => k.startsWith("close_")) ?? null;
  const latestClose = closeKey ? (latest as any)[closeKey] : null;

  const latestSent = typeof latest?.sentiment_cap_weighted === "number" ? latest.sentiment_cap_weighted : null;

  const last7 = daily
    .slice(Math.max(0, daily.length - 7))
    .map((r) => (typeof r.sentiment_cap_weighted === "number" ? r.sentiment_cap_weighted : null))
    .filter((x): x is number => typeof x === "number");

  const series = {
    date: daily.map((r) => r.date),
    price: daily.map((r) => (closeKey ? Number((r as any)[closeKey]) : NaN)),
    sentiment: daily.map((r) => (typeof r.sentiment_cap_weighted === "number" ? r.sentiment_cap_weighted : NaN)),
  };

  const last30 = daily.slice(Math.max(0, daily.length - 30)).reverse();

  return (
    <div className="space-y-6">
      <div className="flex items-baseline justify-between gap-4 flex-wrap">
        <div>
          <h1 className="text-2xl font-semibold">
            {data.name} ({data.symbol})
          </h1>
        </div>
        <div className="text-sm">
          <Link className="underline" href="/">← Back to Home</Link>
        </div>
      </div>

      <div className="grid gap-3 sm:grid-cols-3">
        <div className="border rounded-lg p-4">
          <div className="text-sm text-gray-500">Latest date</div>
          <div className="text-lg font-medium">{latest?.date ?? "—"}</div>
        </div>

        <div className="border rounded-lg p-4">
          <div className="text-sm text-gray-500">Latest close ({closeKey ?? "close"})</div>
          <div className="text-lg font-medium">{fmtMoney(latestClose, 2)}</div>
        </div>

        <div className="border rounded-lg p-4">
          <div className="text-sm text-gray-500">Cap-weighted sentiment</div>
          <div className="text-lg font-medium">{latestSent === null ? "—" : fmtNum(latestSent, 4)}</div>
          <div className="text-xs text-gray-500 mt-1">7-day avg: {avg(last7) === null ? "—" : fmtNum(avg(last7)!, 4)}</div>
        </div>
      </div>

      <section className="space-y-2">
        <div className="flex items-baseline justify-between gap-4 flex-wrap">
          <h2 className="text-lg font-semibold">Sector / Industry Heatmap</h2>
          <div className="text-xs text-gray-500">
            {heatmap ? (
              <>
                Latest trading day: <span className="font-medium">{heatmap.asof.latest_trading_day}</span> · Current:{" "}
                <span className="font-medium">{heatmap.asof.current}</span>
              </>
            ) : (
              <>Heatmap data not found</>
            )}
          </div>
        </div>

        {heatmap ? (
          <Sp500HeatmapClient file={heatmap} />
        ) : (
          <div className="border rounded-lg p-4 text-sm text-gray-700">
            Heatmap is unavailable because <code>sp500_heatmap.json</code> was not found at build time.
            <div className="mt-2 text-xs text-gray-500">
              Fix: run <code>python -m market_sentiment.cli.build_sp500_heatmap ...</code> in workflow to generate:
              <br />
              <code>data/SPX/sp500_heatmap.json</code> and <code>apps/web/public/data/SPX/sp500_heatmap.json</code>
            </div>
          </div>
        )}
      </section>

      <details className="border rounded-lg p-4">
        <summary className="cursor-pointer select-none text-sm font-medium">❓ How S&amp;P 500 sentiment is calculated?</summary>
        <div className="mt-3 text-sm text-gray-700 space-y-2">
          <p>
            The index sentiment is a <b>market-cap-weighted</b> aggregation of constituent sentiment scores. Larger companies contribute more
            to the final index-level score.
          </p>
          <p>
            For each day, we compute each constituent’s daily news sentiment score, then aggregate across all constituents using their
            market-cap weights to produce a single cap-weighted S&amp;P 500 sentiment number.
          </p>
        </div>
      </details>

      <div className="space-y-2">
        <h2 className="text-lg font-semibold">Price & Sentiment</h2>
        <Sp500Client series={series} />
      </div>

      <div className="space-y-2">
        <h2 className="text-lg font-semibold">Recent daily values</h2>
        <div className="overflow-x-auto border rounded-lg">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b">
              <tr>
                <th className="text-left p-3">Date</th>
                <th className="text-right p-3">Close</th>
                <th className="text-right p-3">Cap-weighted sentiment</th>
              </tr>
            </thead>
            <tbody>
              {last30.map((r) => {
                const close = closeKey ? Number((r as any)[closeKey]) : NaN;
                return (
                  <tr key={r.date} className="border-b last:border-b-0">
                    <td className="p-3">{r.date}</td>
                    <td className="p-3 text-right">{fmtMoney(close, 2)}</td>
                    <td className="p-3 text-right">{fmtNum(r.sentiment_cap_weighted, 4)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        <p className="text-xs text-gray-500">Showing the latest 30 rows from the SPX index file.</p>
      </div>
    </div>
  );
}
