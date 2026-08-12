import fs from "node:fs";
import path from "node:path";
import Link from "next/link";
import Sp500Client from "./Sp500Client";
import Sp500HeatmapClient from "./Sp500HeatmapClient";

export const dynamic = "force-static";

type DailyRow = {
  date: string;
  close?: number;
  sentiment_cap_weighted?: number;
  sentiment_equal_weighted?: number;
  sentiment_coverage_market_cap?: number;
  sentiment_coverage_tickers?: number;
  sentiment_observed_tickers?: number;
  sentiment_unique_news_count?: number;
  [k: string]: unknown;
};

type Sp500IndexFile = {
  symbol: string;
  name: string;
  price_symbol_candidates?: string[];
  news_symbol_candidates?: string[];
  daily: DailyRow[];
};

type HeatmapTile = {
  symbol: string;
  name?: string;
  sector?: string;
  industry?: string;
  market_cap?: number;
  weight?: number;
  date?: string;
  price?: number | null;
  return_1d?: number | null;
  sentiment?: number | null;
  n_total?: number | null;
};

type Sp500HeatmapFile = {
  symbol: string;
  name: string;
  asof: string;
  updated_at_utc?: string;
  stats?: Record<string, unknown>;
  tiles: HeatmapTile[];
};

function safeReadJson<T>(absPath: string): T | null {
  try {
    if (!fs.existsSync(absPath)) return null;
    return JSON.parse(fs.readFileSync(absPath, "utf-8")) as T;
  } catch {
    return null;
  }
}

function readSp500Index(): Sp500IndexFile | null {
  const candidates = [
    path.resolve(process.cwd(), "public/data/SPX/sp500_index.json"),
    path.resolve(process.cwd(), "apps/web/public/data/SPX/sp500_index.json"),
    path.resolve(process.cwd(), "../../apps/web/public/data/SPX/sp500_index.json"),
    path.resolve(process.cwd(), "../../data/SPX/sp500_index.json"),
    path.resolve(process.cwd(), "data/SPX/sp500_index.json"),
    path.resolve(process.cwd(), "public/data/sp500_index.json"),
  ];
  for (const p of candidates) {
    const parsed = safeReadJson<Sp500IndexFile>(p);
    if (parsed?.daily && Array.isArray(parsed.daily)) return parsed;
  }
  return null;
}

function readSp500Heatmap(): Sp500HeatmapFile | null {
  const candidates = [
    path.resolve(process.cwd(), "public/data/SPX/sp500_heatmap.json"),
    path.resolve(process.cwd(), "apps/web/public/data/SPX/sp500_heatmap.json"),
    path.resolve(process.cwd(), "../../apps/web/public/data/SPX/sp500_heatmap.json"),
    path.resolve(process.cwd(), "../../data/SPX/sp500_heatmap.json"),
    path.resolve(process.cwd(), "data/SPX/sp500_heatmap.json"),
  ];
  for (const p of candidates) {
    const parsed = safeReadJson<Sp500HeatmapFile>(p);
    if (parsed?.tiles && Array.isArray(parsed.tiles)) return parsed;
  }
  return null;
}

function finite(x: unknown): number | null {
  if (x === null || x === undefined || x === "") return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function closeOf(row: DailyRow | null | undefined): number | null {
  if (!row) return null;
  const canonical = finite(row.close);
  if (canonical != null) return canonical;
  const key = Object.keys(row).find((k) => k.startsWith("close_"));
  return key ? finite(row[key]) : null;
}

function fmtNum(x: unknown, digits = 4) {
  const n = finite(x);
  return n == null ? "—" : n.toFixed(digits);
}

function fmtMoney(x: unknown, digits = 2) {
  const n = finite(x);
  return n == null ? "—" : n.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

function fmtPct(x: unknown, digits = 1) {
  const n = finite(x);
  return n == null ? "—" : `${(n * 100).toFixed(digits)}%`;
}

function signClass(x: number | null) {
  if (x == null) return "text-neutral-300";
  if (x > 0) return "text-emerald-300";
  if (x < 0) return "text-rose-300";
  return "text-neutral-300";
}

function avg(xs: number[]) {
  return xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null;
}

export default function Sp500Page() {
  const data = readSp500Index();
  const heatmap = readSp500Heatmap();

  if (!data) {
    return (
      <main className="space-y-4">
        <div className="eyebrow">S&P 500 intelligence</div>
        <h1 className="page-title">Index data unavailable</h1>
        <p className="text-sm text-neutral-500">The generated SPX index artifact was not available at build time.</p>
      </main>
    );
  }

  const daily = [...data.daily].sort((a, b) => a.date.localeCompare(b.date));
  const latest = daily.at(-1) ?? null;
  const prior = daily.at(-2) ?? null;
  const latestClose = closeOf(latest);
  const priorClose = closeOf(prior);
  const priceChange = latestClose != null && priorClose != null && priorClose !== 0 ? latestClose / priorClose - 1 : null;
  const latestSent = finite(latest?.sentiment_cap_weighted);
  const priorSent = finite(prior?.sentiment_cap_weighted);
  const sentChange = latestSent != null && priorSent != null ? latestSent - priorSent : null;
  const last7 = daily.slice(-7).map((r) => finite(r.sentiment_cap_weighted)).filter((x): x is number => x != null);
  const last7Avg = avg(last7);

  const series = {
    date: daily.map((r) => r.date),
    price: daily.map((r) => closeOf(r) ?? Number.NaN),
    sentiment: daily.map((r) => finite(r.sentiment_cap_weighted) ?? Number.NaN),
  };

  const observedTiles = (heatmap?.tiles ?? []).filter((t) => finite(t.sentiment) != null);
  const observedWeight = observedTiles.reduce((s, t) => s + Math.max(0, finite(t.weight) ?? 0), 0);
  const uniqueArticles = latest?.sentiment_unique_news_count ?? observedTiles.reduce((s, t) => s + Math.max(0, finite(t.n_total) ?? 0), 0);
  const coverage = finite(latest?.sentiment_coverage_market_cap) ?? (observedWeight > 0 ? observedWeight : null);
  const observedTickers = finite(latest?.sentiment_observed_tickers) ?? observedTiles.length;
  const totalTiles = heatmap?.tiles?.length ?? 0;
  const last30 = daily.slice(-30).reverse();

  return (
    <main className="space-y-8">
      <section className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <div className="eyebrow">Index intelligence</div>
          <h1 className="page-title mt-2">{data.name}</h1>
          <p className="mt-3 max-w-3xl text-sm leading-6 text-neutral-400">
            Price, observed news sentiment, coverage, and constituent attribution in one view. Missing sentiment is excluded rather than treated as neutral.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <span className="pill">Latest trading day · {latest?.date ?? "—"}</span>
          <Link href="/methodology" className="pill">Methodology →</Link>
        </div>
      </section>

      <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
        <div className="kpi">
          <div className="kpi-label">S&P 500 close</div>
          <div className="kpi-value">{fmtMoney(latestClose)}</div>
          <div className={`kpi-sub ${signClass(priceChange)}`}>{priceChange == null ? "1D change unavailable" : `${priceChange >= 0 ? "+" : ""}${fmtPct(priceChange, 2)} 1D`}</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Cap-weighted sentiment</div>
          <div className={`kpi-value ${signClass(latestSent)}`}>{fmtNum(latestSent, 4)}</div>
          <div className={`kpi-sub ${signClass(sentChange)}`}>{sentChange == null ? "Change unavailable" : `${sentChange >= 0 ? "+" : ""}${sentChange.toFixed(4)} vs prior obs`}</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">7D sentiment average</div>
          <div className={`kpi-value ${signClass(last7Avg)}`}>{fmtNum(last7Avg, 4)}</div>
          <div className="kpi-sub">Observed daily index signal</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Market-cap coverage</div>
          <div className="kpi-value">{fmtPct(coverage, 1)}</div>
          <div className="kpi-sub">{Math.round(observedTickers)} / {totalTiles || "—"} constituents observed</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Unique news evidence</div>
          <div className="kpi-value">{Number(uniqueArticles || 0).toLocaleString()}</div>
          <div className="kpi-sub">Articles behind latest observed signal</div>
        </div>
      </section>

      <section className="space-y-3">
        <div>
          <div className="eyebrow">Market structure</div>
          <h2 className="section-title mt-1">Who is driving the index signal?</h2>
          <p className="section-copy">Switch between contribution, raw sentiment, and one-day price return. Contribution combines constituent weight with observed sentiment.</p>
        </div>
        {heatmap ? (
          <Sp500HeatmapClient data={heatmap} />
        ) : (
          <div className="card p-5 text-sm text-neutral-500">Constituent map artifact is unavailable.</div>
        )}
      </section>

      <section className="space-y-3">
        <div>
          <div className="eyebrow">Time series</div>
          <h2 className="section-title mt-1">Price and sentiment through time</h2>
        </div>
        <div className="legacy-dark">
          <Sp500Client series={series} />
        </div>
      </section>

      <section className="space-y-3">
        <div className="flex flex-wrap items-end justify-between gap-3">
          <div>
            <div className="eyebrow">Audit trail</div>
            <h2 className="section-title mt-1">Recent index observations</h2>
          </div>
          <span className="text-xs text-neutral-600">Latest 30 rows</span>
        </div>
        <div className="table-shell overflow-x-auto">
          <table className="w-full min-w-[640px] text-sm">
            <thead className="border-b border-white/10 bg-white/[0.025] text-left text-[11px] uppercase tracking-[0.12em] text-neutral-600">
              <tr>
                <th className="px-4 py-3">Date</th>
                <th className="px-4 py-3 text-right">Close</th>
                <th className="px-4 py-3 text-right">Cap weighted</th>
                <th className="px-4 py-3 text-right">Coverage</th>
              </tr>
            </thead>
            <tbody>
              {last30.map((r) => {
                const s = finite(r.sentiment_cap_weighted);
                return (
                  <tr key={r.date} className="border-b border-white/[0.06] last:border-0 hover:bg-white/[0.025]">
                    <td className="px-4 py-3 font-mono text-xs text-neutral-400">{r.date}</td>
                    <td className="px-4 py-3 text-right font-mono text-neutral-300">{fmtMoney(closeOf(r))}</td>
                    <td className={`px-4 py-3 text-right font-mono ${signClass(s)}`}>{fmtNum(s, 4)}</td>
                    <td className="px-4 py-3 text-right font-mono text-neutral-500">{fmtPct(r.sentiment_coverage_market_cap, 1)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}
