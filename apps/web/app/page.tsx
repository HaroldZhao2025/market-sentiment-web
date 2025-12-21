// apps/web/app/page.tsx
import Link from "next/link";
import fs from "node:fs";
import path from "node:path";
import { loadTickers } from "../lib/data";
import { hrefs } from "../lib/paths";
import TickerGridClient, { type TickerRow } from "../components/TickerGridClient";

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

type PortfolioHolding = {
  date: string;
  long: string[];
  short: string[];
};

type PortfolioStrategyFile = {
  meta?: {
    generated_at?: string;
    rebalance?: "daily" | "weekly";
    signal?: "day" | "ma7";
    lag_days?: number;
    k?: number;
    long_short?: boolean;
    gross_per_side?: number;
    benchmark?: string | null;
    universe_size_used?: number;
  };
  metrics?: {
    cumulative_return?: number;
    annualized_return?: number;
    annualized_vol?: number;
    sharpe?: number;
    max_drawdown?: number;
    hit_rate?: number;
    num_days?: number;
  };
  dates: string[];
  equity: number[];
  portfolio_return: number[];
  holdings?: PortfolioHolding[];
};

type TickerJson = {
  ticker: string;
  dates: string[];
  price: number[];
  // both exist in your snapshots
  S?: number[];
  sentiment?: number[];
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

function readPortfolioStrategy(): PortfolioStrategyFile | null {
  const candidates = [
    path.resolve(process.cwd(), "public/data/portfolio_strategy.json"),
    path.resolve(process.cwd(), "apps/web/public/data/portfolio_strategy.json"),
    path.resolve(process.cwd(), "../../apps/web/public/data/portfolio_strategy.json"),
  ];

  for (const p of candidates) {
    const parsed = safeReadJson<PortfolioStrategyFile>(p);
    if (parsed?.dates?.length && parsed?.equity?.length) return parsed;
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

function fmtPct(x: unknown, digits = 2): string {
  const n = typeof x === "number" ? x : Number(x);
  if (!Number.isFinite(n)) return "—";
  return `${(n * 100).toFixed(digits)}%`;
}

function avg(arr: number[]): number | null {
  if (!arr.length) return null;
  const s = arr.reduce((a, b) => a + b, 0);
  return s / arr.length;
}

function findTickerDir(): string | null {
  const candidates = [
    path.resolve(process.cwd(), "public/data/ticker"),
    path.resolve(process.cwd(), "data/ticker"),
    path.resolve(process.cwd(), "../../data/ticker"),
    path.resolve(process.cwd(), "../../apps/web/public/data/ticker"),
  ];
  for (const d of candidates) {
    try {
      if (fs.existsSync(d) && fs.statSync(d).isDirectory()) return d;
    } catch {
      // ignore
    }
  }
  return null;
}

function readTickerRow(tickerDir: string | null, t: string): TickerRow {
  const tryFiles = (baseDir: string, ticker: string) => {
    const names = [
      `${ticker}.json`,
      `${encodeURIComponent(ticker)}.json`, // for ^GSPC -> %5EGSPC.json
      `${ticker.replace("^", "")}.json`,
      `${encodeURIComponent(ticker).replace("%2E", "-")}.json`,
    ];
    for (const n of names) {
      const p = path.join(baseDir, n);
      const obj = safeReadJson<TickerJson>(p);
      if (obj?.price?.length && obj?.dates?.length) return obj;
    }
    return null;
  };

  const obj = tickerDir ? tryFiles(tickerDir, t) : null;

  const price = obj?.price?.length ? Number(obj.price[obj.price.length - 1]) : null;

  // prefer S (your strategy uses S a lot), else sentiment
  const sArr = (obj?.S?.length ? obj.S : obj?.sentiment) ?? [];
  const sentiment = sArr.length ? Number(sArr[sArr.length - 1]) : null;

  let dailyReturn: number | null = null;
  if (obj?.price?.length && obj.price.length >= 2) {
    const p0 = Number(obj.price[obj.price.length - 2]);
    const p1 = Number(obj.price[obj.price.length - 1]);
    if (Number.isFinite(p0) && Number.isFinite(p1) && p0 !== 0) dailyReturn = p1 / p0 - 1;
  }

  return {
    ticker: t,
    price: Number.isFinite(price as number) ? (price as number) : null,
    sentiment: Number.isFinite(sentiment as number) ? (sentiment as number) : null,
    dailyReturn: Number.isFinite(dailyReturn as number) ? (dailyReturn as number) : null,
  };
}

export default async function Home() {
  const [tickers] = await Promise.all([loadTickers()]);

  // Read SPX index file
  const spxIndex = readSp500Index();
  const daily = spxIndex?.daily ? [...spxIndex.daily].sort((a, b) => a.date.localeCompare(b.date)) : [];
  const latest = daily.length ? daily[daily.length - 1] : null;

  const closeKey = latest ? Object.keys(latest).find((k) => k.startsWith("close_")) ?? null : null;
  const latestClose = latest && closeKey ? (latest as any)[closeKey] : null;

  const latestSent = typeof latest?.sentiment_cap_weighted === "number" ? latest.sentiment_cap_weighted : null;

  const last7 = daily
    .slice(Math.max(0, daily.length - 7))
    .map((r) => (typeof r.sentiment_cap_weighted === "number" ? r.sentiment_cap_weighted : null))
    .filter((x): x is number => typeof x === "number");

  const last7Avg = avg(last7);

  // Read portfolio strategy output
  const strat = readPortfolioStrategy();
  const pLatestDate = strat?.dates?.length ? strat.dates[strat.dates.length - 1] : null;
  const pLatestEq = strat?.equity?.length ? strat.equity[strat.equity.length - 1] : null;
  const pLatestRet = strat?.portfolio_return?.length ? strat.portfolio_return[strat.portfolio_return.length - 1] : null;
  const latestHolding = strat?.holdings?.length ? strat.holdings[strat.holdings.length - 1] : null;

  let p7dRet: number | null = null;
  if (strat?.equity?.length && strat.equity.length >= 7) {
    const a = strat.equity[strat.equity.length - 7];
    const b = strat.equity[strat.equity.length - 1];
    if (Number.isFinite(a) && Number.isFinite(b) && a !== 0) p7dRet = b / a - 1;
  }

  // Build ticker rows (price + sentiment + daily return) for the homepage list
  const tickerDir = findTickerDir();
  const tickerRows: TickerRow[] = tickers.map((t) => readTickerRow(tickerDir, t));

  return (
    <main className="max-w-6xl mx-auto p-6 space-y-8">
      <h1 className="text-3xl font-bold">Market Sentiment — S&amp;P 500</h1>

      {/* SPX Overview */}
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
              <div className="text-xs text-gray-500 mt-1">7-day avg: {last7Avg === null ? "—" : fmtNum(last7Avg, 4)}</div>
            </div>
          </div>
        )}
      </section>

      {/* Portfolio Overview */}
      <section className="rounded-2xl p-4 shadow-sm border bg-white">
        <div className="flex items-baseline justify-between gap-4 flex-wrap">
          <h2 className="text-xl font-semibold mb-3">Portfolio Overview</h2>
          <Link className="text-sm underline" href="/portfolio">
            View Portfolio page →
          </Link>
        </div>

        {!strat ? (
          <p className="text-sm text-neutral-500">
            No portfolio strategy yet. Generate{" "}
            <code className="px-1 py-0.5 rounded bg-neutral-100">public/data/portfolio_strategy.json</code>.
          </p>
        ) : (
          <>
            <div className="mb-3 flex flex-wrap gap-2 text-xs">
              {strat.meta?.rebalance ? (
                <span className="rounded-full bg-indigo-100 text-indigo-800 px-3 py-1 font-semibold">
                  Rebalance: {strat.meta.rebalance}
                </span>
              ) : null}
              {strat.meta?.signal ? (
                <span className="rounded-full bg-emerald-100 text-emerald-800 px-3 py-1 font-semibold">
                  Signal: {strat.meta.signal}
                </span>
              ) : null}
              {typeof strat.meta?.lag_days === "number" ? (
                <span className="rounded-full bg-amber-100 text-amber-800 px-3 py-1 font-semibold">
                  Lag: {strat.meta.lag_days}d
                </span>
              ) : null}
              {typeof strat.meta?.k === "number" ? (
                <span className="rounded-full bg-fuchsia-100 text-fuchsia-800 px-3 py-1 font-semibold">
                  K: {strat.meta.k}
                </span>
              ) : null}
              {typeof strat.meta?.long_short === "boolean" ? (
                <span className="rounded-full bg-sky-100 text-sky-800 px-3 py-1 font-semibold">
                  Long/Short: {strat.meta.long_short ? "Yes" : "No"}
                </span>
              ) : null}
              {typeof strat.meta?.universe_size_used === "number" ? (
                <span className="rounded-full bg-neutral-100 text-neutral-800 px-3 py-1 font-semibold">
                  Universe used: {strat.meta.universe_size_used}
                </span>
              ) : null}
            </div>

            <div className="grid gap-3 sm:grid-cols-4">
              <div className="border rounded-lg p-4">
                <div className="text-sm text-gray-500">Latest date</div>
                <div className="text-lg font-medium">{pLatestDate ?? "—"}</div>
                <div className="text-xs text-gray-500 mt-1">Points: {strat.dates?.length ?? 0}</div>
              </div>

              <div className="border rounded-lg p-4">
                <div className="text-sm text-gray-500">Current equity</div>
                <div className="text-lg font-medium">{pLatestEq == null ? "—" : fmtNum(pLatestEq, 4)}</div>
                <div className="text-xs text-gray-500 mt-1">Last 7d: {p7dRet == null ? "—" : fmtPct(p7dRet, 2)}</div>
              </div>

              <div className="border rounded-lg p-4">
                <div className="text-sm text-gray-500">Cumulative return</div>
                <div className="text-lg font-medium">{fmtPct(strat.metrics?.cumulative_return, 2)}</div>
                <div className="text-xs text-gray-500 mt-1">Latest daily: {pLatestRet == null ? "—" : fmtPct(pLatestRet, 2)}</div>
              </div>

              <div className="border rounded-lg p-4">
                <div className="text-sm text-gray-500">Sharpe / Max DD</div>
                <div className="text-lg font-medium">
                  {fmtNum(strat.metrics?.sharpe, 2)} <span className="text-gray-400">/</span>{" "}
                  {fmtPct(strat.metrics?.max_drawdown, 2)}
                </div>
                <div className="text-xs text-gray-500 mt-1">
                  Ann. ret/vol: {fmtPct(strat.metrics?.annualized_return, 2)} / {fmtPct(strat.metrics?.annualized_vol, 2)}
                </div>
              </div>
            </div>

            <details className="border rounded-lg p-4 mt-4">
              <summary className="cursor-pointer select-none text-sm font-medium">❓ How the portfolio strategy works?</summary>
              <div className="mt-3 text-sm text-gray-700 space-y-2">
                <p>
                  We compute each stock’s daily news sentiment signal (either <b>day</b> or <b>7-day moving average</b>),
                  then <b>rank the universe</b> on each rebalance date.
                </p>
                <p>
                  The portfolio goes <b>long</b> the top-K tickers (and optionally <b>short</b> the bottom-K),
                  with equal weights per side. A <b>lag</b> is applied to reduce look-ahead bias.
                </p>
              </div>
            </details>

            {latestHolding ? (
              <div className="mt-4 grid gap-3 sm:grid-cols-2">
                <div className="border rounded-lg p-4">
                  <div className="flex items-baseline justify-between">
                    <div className="text-sm font-semibold">Latest Longs</div>
                    <div className="text-xs text-gray-500">{latestHolding.date}</div>
                  </div>
                  <div className="mt-2 flex flex-wrap gap-2">
                    {latestHolding.long?.length ? (
                      latestHolding.long.slice(0, 14).map((t) => (
                        <Link
                          key={t}
                          href={hrefs.ticker(t)}
                          className="rounded-full bg-white border px-3 py-1 text-sm hover:bg-neutral-100"
                          title={`Go to ${t}`}
                        >
                          {t}
                        </Link>
                      ))
                    ) : (
                      <span className="text-sm text-neutral-500">—</span>
                    )}
                    {latestHolding.long?.length > 14 ? (
                      <span className="text-xs text-gray-500 self-center">+{latestHolding.long.length - 14} more</span>
                    ) : null}
                  </div>
                </div>

                <div className="border rounded-lg p-4">
                  <div className="flex items-baseline justify-between">
                    <div className="text-sm font-semibold">Latest Shorts</div>
                    <div className="text-xs text-gray-500">{latestHolding.date}</div>
                  </div>
                  <div className="mt-2 flex flex-wrap gap-2">
                    {latestHolding.short?.length ? (
                      latestHolding.short.slice(0, 14).map((t) => (
                        <Link
                          key={t}
                          href={hrefs.ticker(t)}
                          className="rounded-full bg-white border px-3 py-1 text-sm hover:bg-neutral-100"
                          title={`Go to ${t}`}
                        >
                          {t}
                        </Link>
                      ))
                    ) : (
                      <span className="text-sm text-neutral-500">Short leg disabled.</span>
                    )}
                    {latestHolding.short?.length > 14 ? (
                      <span className="text-xs text-gray-500 self-center">+{latestHolding.short.length - 14} more</span>
                    ) : null}
                  </div>
                </div>
              </div>
            ) : null}
          </>
        )}
      </section>

      {/* Tickers (UPDATED) */}
      <section className="rounded-2xl p-4 shadow-sm border bg-white">
        <h2 className="text-xl font-semibold mb-3">Tickers</h2>
        {tickers.length === 0 ? (
          <p className="text-sm text-neutral-500">No data generated yet.</p>
        ) : (
          <TickerGridClient rows={tickerRows} />
        )}
      </section>
    </main>
  );
}
