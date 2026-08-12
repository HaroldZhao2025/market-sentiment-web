import Link from "next/link";
import fs from "node:fs";
import path from "node:path";
import { loadTickers } from "../lib/data";
import { hrefs } from "../lib/paths";
import TickerGridClient, { type TickerRow } from "../components/TickerGridClient";

type DailyRow = {
  date: string;
  close?: number;
  sentiment_cap_weighted?: number;
  sentiment_equal_weighted?: number;
  sentiment_coverage_market_cap?: number;
  sentiment_coverage_tickers?: number;
  sentiment_observed_tickers?: number;
  sentiment_unique_news_count?: number;
  [key: string]: unknown;
};

type Sp500IndexFile = {
  symbol?: string;
  name?: string;
  daily?: DailyRow[];
};

type PortfolioHolding = {
  date: string;
  long: string[];
  short: string[];
};

type PortfolioStrategyFile = {
  meta?: {
    rebalance?: string;
    signal?: string;
    lag_days?: number;
    k?: number;
    long_short?: boolean;
    universe_size_used?: number;
  };
  metrics?: {
    cumulative_return?: number;
    annualized_return?: number;
    annualized_vol?: number;
    sharpe?: number;
    max_drawdown?: number;
    hit_rate?: number;
  };
  dates?: string[];
  equity?: number[];
  portfolio_return?: number[];
  holdings?: PortfolioHolding[];
};

type TickerJson = {
  ticker?: string;
  dates?: string[];
  price?: Array<number | null>;
  S?: Array<number | null>;
  sentiment?: Array<number | null>;
};

function safeReadJson<T>(absolutePath: string): T | null {
  try {
    if (!fs.existsSync(absolutePath)) return null;
    return JSON.parse(fs.readFileSync(absolutePath, "utf8")) as T;
  } catch {
    return null;
  }
}

function readSp500Index(): Sp500IndexFile | null {
  const candidates = [
    path.resolve(process.cwd(), "public/data/SPX/sp500_index.json"),
    path.resolve(process.cwd(), "../../data/SPX/sp500_index.json"),
    path.resolve(process.cwd(), "public/data/sp500_index.json"),
  ];
  for (const candidate of candidates) {
    const parsed = safeReadJson<Sp500IndexFile>(candidate);
    if (parsed?.daily?.length) return parsed;
  }
  return null;
}

function readPortfolioStrategy(): PortfolioStrategyFile | null {
  const candidates = [
    path.resolve(process.cwd(), "public/data/portfolio_strategy.json"),
    path.resolve(process.cwd(), "../../apps/web/public/data/portfolio_strategy.json"),
  ];
  for (const candidate of candidates) {
    const parsed = safeReadJson<PortfolioStrategyFile>(candidate);
    if (parsed?.dates?.length && parsed?.equity?.length) return parsed;
  }
  return null;
}

function findTickerDir(): string | null {
  const candidates = [
    path.resolve(process.cwd(), "public/data/ticker"),
    path.resolve(process.cwd(), "../../apps/web/public/data/ticker"),
    path.resolve(process.cwd(), "../../data/ticker"),
  ];
  for (const candidate of candidates) {
    try {
      if (fs.existsSync(candidate) && fs.statSync(candidate).isDirectory()) return candidate;
    } catch {
      // Keep checking candidates.
    }
  }
  return null;
}

function finiteNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const number = typeof value === "number" ? value : Number(value);
  return Number.isFinite(number) ? number : null;
}

function lastFinite(values: Array<number | null> | undefined, offset = 0): number | null {
  if (!values?.length) return null;
  let seen = 0;
  for (let index = values.length - 1; index >= 0; index -= 1) {
    const value = finiteNumber(values[index]);
    if (value === null) continue;
    if (seen === offset) return value;
    seen += 1;
  }
  return null;
}

function readTickerRow(tickerDir: string | null, ticker: string): TickerRow {
  if (!tickerDir) {
    return { ticker, price: null, sentiment: null, sentimentChange: null, dailyReturn: null };
  }

  const names = [ticker, encodeURIComponent(ticker), ticker.replace("^", "")];
  let obj: TickerJson | null = null;
  for (const name of names) {
    obj = safeReadJson<TickerJson>(path.join(tickerDir, `${name}.json`));
    if (obj) break;
  }

  const prices = obj?.price ?? [];
  const signals = obj?.S?.length ? obj.S : obj?.sentiment ?? [];
  const price = lastFinite(prices);
  const previousPrice = lastFinite(prices, 1);
  const sentiment = lastFinite(signals);
  const previousSentiment = lastFinite(signals, 1);

  const dailyReturn =
    price !== null && previousPrice !== null && previousPrice !== 0 ? price / previousPrice - 1 : null;
  const sentimentChange =
    sentiment !== null && previousSentiment !== null ? sentiment - previousSentiment : null;

  return { ticker, price, sentiment, sentimentChange, dailyReturn };
}

function fmtNumber(value: unknown, digits = 3) {
  const number = finiteNumber(value);
  if (number === null) return "—";
  return `${number > 0 ? "+" : ""}${number.toFixed(digits)}`;
}

function fmtPlain(value: unknown, digits = 2) {
  const number = finiteNumber(value);
  return number === null ? "—" : number.toFixed(digits);
}

function fmtPct(value: unknown, digits = 1) {
  const number = finiteNumber(value);
  if (number === null) return "—";
  return `${number > 0 ? "+" : ""}${(number * 100).toFixed(digits)}%`;
}

function fmtCoverage(value: unknown) {
  const number = finiteNumber(value);
  if (number === null) return "—";
  const normalized = number > 1 ? number / 100 : number;
  return `${(normalized * 100).toFixed(1)}%`;
}

function avg(values: number[]) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : null;
}

function signalClass(value: number | null) {
  if (value === null) return "text-neutral-500";
  if (value > 0.03) return "text-emerald-400";
  if (value < -0.03) return "text-rose-400";
  return "text-neutral-200";
}

function marketRegime(sentiment: number | null, change: number | null) {
  if (sentiment === null) return { label: "Insufficient signal", detail: "Waiting for observed market sentiment." };
  if (sentiment >= 0.08 && (change ?? 0) >= 0) return { label: "Broadly constructive", detail: "Positive market sentiment is holding or improving." };
  if (sentiment >= 0 && (change ?? 0) < -0.04) return { label: "Positive, cooling", detail: "Sentiment remains positive but momentum has weakened." };
  if (sentiment <= -0.08 && (change ?? 0) <= 0) return { label: "Broadly defensive", detail: "Negative sentiment is persistent or deteriorating." };
  if (sentiment < 0 && (change ?? 0) > 0.04) return { label: "Negative, recovering", detail: "Sentiment remains below zero but is improving." };
  return { label: "Mixed / transitional", detail: "The aggregate signal is close to neutral or changing direction." };
}

export default async function Home() {
  const tickers = await loadTickers();
  const tickerDir = findTickerDir();
  const tickerRows = tickers.map((ticker) => readTickerRow(tickerDir, ticker));

  const spx = readSp500Index();
  const daily = [...(spx?.daily ?? [])].sort((a, b) => a.date.localeCompare(b.date));
  const latest = daily.at(-1) ?? null;
  const previous = daily.at(-2) ?? null;

  const latestClose = finiteNumber(latest?.close) ??
    (latest ? finiteNumber(Object.entries(latest).find(([key]) => key.startsWith("close_"))?.[1]) : null);
  const previousClose = finiteNumber(previous?.close) ??
    (previous ? finiteNumber(Object.entries(previous).find(([key]) => key.startsWith("close_"))?.[1]) : null);
  const closeReturn = latestClose !== null && previousClose !== null && previousClose !== 0
    ? latestClose / previousClose - 1
    : null;

  const latestSentiment = finiteNumber(latest?.sentiment_cap_weighted);
  const previousSentiment = finiteNumber(previous?.sentiment_cap_weighted);
  const sentimentChange = latestSentiment !== null && previousSentiment !== null
    ? latestSentiment - previousSentiment
    : null;
  const last7Average = avg(
    daily
      .slice(-7)
      .map((row) => finiteNumber(row.sentiment_cap_weighted))
      .filter((value): value is number => value !== null),
  );

  const regime = marketRegime(latestSentiment, sentimentChange);
  const coverage = finiteNumber(latest?.sentiment_coverage_market_cap);
  const observedTickers = finiteNumber(latest?.sentiment_observed_tickers ?? latest?.sentiment_coverage_tickers);
  const uniqueNews = finiteNumber(latest?.sentiment_unique_news_count);

  const validRows = tickerRows.filter((row) => row.sentiment !== null);
  const strongestImprovers = [...validRows]
    .filter((row) => row.sentimentChange !== null)
    .sort((a, b) => (b.sentimentChange ?? -Infinity) - (a.sentimentChange ?? -Infinity))
    .slice(0, 5);
  const strongestDeteriorators = [...validRows]
    .filter((row) => row.sentimentChange !== null)
    .sort((a, b) => (a.sentimentChange ?? Infinity) - (b.sentimentChange ?? Infinity))
    .slice(0, 5);
  const divergences = [...validRows]
    .filter((row) => row.dailyReturn !== null && row.sentiment !== null && row.sentiment * row.dailyReturn < 0)
    .sort((a, b) => Math.abs((b.sentiment ?? 0) * (b.dailyReturn ?? 0)) - Math.abs((a.sentiment ?? 0) * (a.dailyReturn ?? 0)))
    .slice(0, 5);

  const portfolio = readPortfolioStrategy();
  const latestEquity = portfolio?.equity?.at(-1) ?? null;
  const latestHolding = portfolio?.holdings?.at(-1) ?? null;

  return (
    <main className="space-y-10">
      <section className="grid gap-6 xl:grid-cols-[1.35fr_0.65fr]">
        <div className="card overflow-hidden p-6 md:p-8">
          <div className="eyebrow">Live market state</div>
          <div className="mt-4 flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
            <div>
              <h1 className="max-w-3xl text-4xl font-semibold tracking-[-0.035em] text-white md:text-6xl">
                What changed in the market?
              </h1>
              <p className="mt-4 max-w-2xl text-base leading-7 text-neutral-400">
                News sentiment, price reaction, coverage, and empirical signals in one auditable S&amp;P 500 intelligence layer.
              </p>
            </div>
            <div className="shrink-0 rounded-2xl border border-white/10 bg-black/20 px-5 py-4 lg:text-right">
              <div className="text-xs uppercase tracking-[0.14em] text-neutral-500">Current regime</div>
              <div className="mt-1 text-xl font-semibold text-white">{regime.label}</div>
              <div className="mt-1 max-w-xs text-xs leading-5 text-neutral-500">{regime.detail}</div>
            </div>
          </div>

          <div className="mt-8 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <div className="kpi">
              <div className="kpi-label">S&P 500 close</div>
              <div className="kpi-value">{fmtPlain(latestClose, 2)}</div>
              <div className={`kpi-sub ${signalClass(closeReturn)}`}>{fmtPct(closeReturn)} 1D</div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Cap-weighted sentiment</div>
              <div className={`kpi-value ${signalClass(latestSentiment)}`}>{fmtNumber(latestSentiment)}</div>
              <div className={`kpi-sub ${signalClass(sentimentChange)}`}>Δ {fmtNumber(sentimentChange)}</div>
            </div>
            <div className="kpi">
              <div className="kpi-label">7D sentiment average</div>
              <div className={`kpi-value ${signalClass(last7Average)}`}>{fmtNumber(last7Average)}</div>
              <div className="kpi-sub">Smooths daily news noise</div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Signal coverage</div>
              <div className="kpi-value">{fmtCoverage(coverage)}</div>
              <div className="kpi-sub">
                {observedTickers === null ? "Observed tickers —" : `${Math.round(observedTickers)} observed tickers`}
              </div>
            </div>
          </div>

          <div className="mt-5 flex flex-wrap items-center gap-x-5 gap-y-2 text-xs text-neutral-500">
            <span>As of {latest?.date ?? "—"}</span>
            <span>{uniqueNews === null ? "Unique news —" : `${Math.round(uniqueNews)} unique news items`}</span>
            <Link href="/methodology" className="text-neutral-300 underline decoration-neutral-700 underline-offset-4 hover:text-white">
              How this is calculated
            </Link>
          </div>
        </div>

        <div className="card p-6">
          <div className="eyebrow">Why it matters</div>
          <h2 className="mt-3 text-2xl font-semibold tracking-tight text-white">Evidence before narrative.</h2>
          <p className="mt-3 text-sm leading-6 text-neutral-400">
            Large-language models can summarize news. This project focuses on what they cannot reliably invent: deduplicated observations, market-cap attribution, price divergence, reproducible backtests, and source-level scores.
          </p>
          <div className="mt-6 space-y-3 text-sm">
            {[
              ["Observed signal", "No-news is treated as missing, not neutral zero."],
              ["Market context", "Sentiment is read beside price reaction and coverage."],
              ["Researchable", "The same data layer feeds portfolio and empirical studies."],
            ].map(([title, copy]) => (
              <div key={title} className="rounded-xl border border-white/10 bg-white/[0.025] p-4">
                <div className="font-medium text-neutral-200">{title}</div>
                <div className="mt-1 text-xs leading-5 text-neutral-500">{copy}</div>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="space-y-4">
        <div>
          <div className="eyebrow">Attention map</div>
          <h2 className="section-title mt-2">Where the signal moved</h2>
          <p className="section-copy">Daily sentiment change tells you where the information environment shifted, while divergence highlights where news and price disagree.</p>
        </div>

        <div className="grid gap-4 lg:grid-cols-3">
          <SignalList title="Improving sentiment" rows={strongestImprovers} mode="change" />
          <SignalList title="Deteriorating sentiment" rows={strongestDeteriorators} mode="change" />
          <SignalList title="News / price divergence" rows={divergences} mode="divergence" />
        </div>
      </section>

      <section className="space-y-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <div className="eyebrow">Explore the universe</div>
            <h2 className="section-title mt-2">S&P 500 signal explorer</h2>
            <p className="section-copy">Search and screen the universe instead of reading a static ticker list.</p>
          </div>
          <Link href="/sp500" className="pill self-start sm:self-auto">Open heatmap →</Link>
        </div>
        <TickerGridClient rows={tickerRows} />
      </section>

      <section className="grid gap-4 lg:grid-cols-3">
        <Link href="/portfolio" className="card card-hover block p-6">
          <div className="eyebrow">Strategy layer</div>
          <div className="mt-3 text-xl font-semibold text-white">Portfolio</div>
          <div className="mt-2 text-sm leading-6 text-neutral-400">Track how the signal behaves after execution lag, sizing, turnover, and transaction costs.</div>
          <div className="mt-5 grid grid-cols-2 gap-3 text-sm">
            <MiniMetric label="Equity" value={latestEquity == null ? "—" : fmtPlain(latestEquity, 3)} />
            <MiniMetric label="Sharpe" value={fmtPlain(portfolio?.metrics?.sharpe, 2)} />
          </div>
          <div className="mt-4 text-xs text-neutral-500">
            {latestHolding ? `${latestHolding.long?.length ?? 0} long / ${latestHolding.short?.length ?? 0} short` : "No current holdings snapshot"}
          </div>
        </Link>

        <Link href="/research" className="card card-hover block p-6">
          <div className="eyebrow">Evidence layer</div>
          <div className="mt-3 text-xl font-semibold text-white">Research Lab</div>
          <div className="mt-2 text-sm leading-6 text-neutral-400">Inspect empirical relationships, forward returns, robustness, and signal diagnostics generated from the live dataset.</div>
          <div className="mt-7 text-sm font-medium text-emerald-300">Open research →</div>
        </Link>

        <Link href="/data" className="card card-hover block p-6">
          <div className="eyebrow">Agent-ready layer</div>
          <div className="mt-3 text-xl font-semibold text-white">Machine-readable data</div>
          <div className="mt-2 text-sm leading-6 text-neutral-400">Discover stable JSON endpoints for tickers, the index, portfolio results, and research artifacts.</div>
          <div className="mt-7 text-sm font-medium text-sky-300">Explore data contract →</div>
        </Link>
      </section>
    </main>
  );
}

function SignalList({
  title,
  rows,
  mode,
}: {
  title: string;
  rows: TickerRow[];
  mode: "change" | "divergence";
}) {
  return (
    <div className="card p-5">
      <div className="text-sm font-semibold text-white">{title}</div>
      <div className="mt-4 space-y-1">
        {rows.length ? rows.map((row) => (
          <Link
            key={row.ticker}
            href={hrefs.ticker(row.ticker)}
            className="flex items-center justify-between rounded-xl px-3 py-2.5 transition hover:bg-white/[0.04]"
          >
            <div>
              <div className="font-medium text-neutral-200">{row.ticker}</div>
              {mode === "divergence" ? (
                <div className="mt-0.5 text-[11px] text-neutral-600">S {fmtNumber(row.sentiment, 2)} · P {fmtPct(row.dailyReturn, 2)}</div>
              ) : (
                <div className="mt-0.5 text-[11px] text-neutral-600">Current {fmtNumber(row.sentiment, 2)}</div>
              )}
            </div>
            <div className={`tabular-nums ${signalClass(mode === "change" ? row.sentimentChange : row.sentiment)}`}>
              {mode === "change" ? fmtNumber(row.sentimentChange, 2) : "↔"}
            </div>
          </Link>
        )) : (
          <div className="rounded-xl border border-dashed border-white/10 p-4 text-xs text-neutral-600">No qualifying observations in the latest snapshot.</div>
        )}
      </div>
    </div>
  );
}

function MiniMetric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-xl border border-white/10 bg-black/20 p-3">
      <div className="text-[10px] uppercase tracking-[0.12em] text-neutral-600">{label}</div>
      <div className="mt-1 font-semibold tabular-nums text-neutral-200">{value}</div>
    </div>
  );
}
