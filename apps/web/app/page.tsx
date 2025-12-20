// apps/web/app/page.tsx
import Link from "next/link";
import fs from "node:fs";
import path from "node:path";
import { loadTickers, loadPortfolio } from "../lib/data";
import { hrefs } from "../lib/paths";

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
  // During GH Actions build, apps/web is often the working dir (process.cwd()).
  // File lives at repo root: data/SPX/sp500_index.json
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

export default async function Home() {
  const [tickers, portfolio] = await Promise.all([loadTickers(), loadPortfolio()]);

  // Read SPX index file (same source as /sp500 page)
  const spxIndex = readSp500Index();

  const daily = spxIndex?.daily ? [...spxIndex.daily].sort((a, b) => a.date.localeCompare(b.date)) : [];
  const latest = daily.length ? daily[daily.length - 1] : null;

  const closeKey = latest ? Object.keys(latest).find((k) => k.startsWith("close_")) ?? null : null;
  const latestClose = latest && closeKey ? (latest as any)[closeKey] : null;

  const latestSent =
    typeof latest?.sentiment_cap_weighted === "number" ? latest.sentiment_cap_weighted : null;

  const last7 = daily
    .slice(Math.max(0, daily.length - 7))
    .map((r) => (typeof r.sentiment_cap_weighted === "number" ? r.sentiment_cap_weighted : null))
    .filter((x): x is number => typeof x === "number");

  const last7Avg = avg(last7);

  return (
    <main className="max-w-6xl mx-auto p-6 space-y-8">
      <h1 className="text-3xl font-bold">Market Sentiment — S&amp;P 500</h1>

      {/* Overview (cards only, like /sp500) */}
      <section className="rounded-2xl p-4 shadow-sm border bg-white">
        <div className="flex items-baseline justify-between gap-4 flex-wrap">
          <h2 className="text-xl font-semibold mb-3">S&amp;P 500 Overview</h2>
          <Link className="text-sm underline" href="/sp500">
            View SPX page →
          </Link>
        </div>

        {!latest ? (
          <p className="text-sm text-neutral-500">No SPX index data yet.</p>
        ) : (
          <div className="grid gap-3 sm:grid-cols-3">
            <div className="border rounded-lg p-4">
              <div className="text-sm text-gray-500">Latest date</div>
              <div className="text-lg font-medium">{latest.date ?? "—"}</div>
            </div>

            <div className="border rounded-lg p-4">
              <div className="text-sm text-gray-500">Latest close ({closeKey ?? "close"})</div>
              <div className="text-lg font-medium">{fmtMoney(latestClose, 2)}</div>
            </div>

            <div className="border rounded-lg p-4">
              <div className="text-sm text-gray-500">Cap-weighted sentiment</div>
              <div className="text-lg font-medium">{latestSent === null ? "—" : fmtNum(latestSent, 4)}</div>
              <div className="text-xs text-gray-500 mt-1">
                7-day avg: {last7Avg === null ? "—" : fmtNum(last7Avg, 4)}
              </div>
            </div>
          </div>
        )}
      </section>

      {/* Portfolio quick stats */}
      <section className="rounded-2xl p-4 shadow-sm border bg-white">
        <h2 className="text-xl font-semibold mb-2">Portfolio</h2>
        {!portfolio ? (
          <p className="text-sm text-neutral-500">No portfolio yet.</p>
        ) : (
          <p className="text-sm text-neutral-600">Points: {portfolio.dates?.length ?? 0}</p>
        )}
      </section>

      {/* Tickers */}
      <section>
        <h2 className="text-xl font-semibold mb-3">Tickers</h2>
        {tickers.length === 0 ? (
          <p className="text-sm text-neutral-500">No data generated yet.</p>
        ) : (
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-2">
            {tickers.map((t) => (
              <Link
                key={t}
                href={hrefs.ticker(t)}
                className="px-3 py-2 rounded-lg bg-white hover:bg-neutral-100 border text-sm"
              >
                {t}
              </Link>
            ))}
          </div>
        )}
      </section>
    </main>
  );
}
