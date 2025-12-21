"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import PortfolioChart from "../../components/PortfolioChart";

type Holding = {
  date: string;
  long: string[];
  short: string[];
};

type Metrics = {
  cumulative_return?: number;
  annualized_return?: number;
  annualized_vol?: number;
  sharpe?: number;
  max_drawdown?: number;
  hit_rate?: number;
  num_days?: number;
};

type Meta = {
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

type EquitySeries = {
  ticker: string;
  equity: number[];
};

type Props = {
  meta?: Meta;
  metrics?: Metrics;
  dates: string[];
  equity: number[];
  portfolio_return: number[];
  holdings?: Holding[];
  benchmark_series?: EquitySeries | null; // typically SPY
  sp500_price_series?: EquitySeries | null; // SPX built from sp500_index.json + ^GSPC fill
};

function pct(x?: number) {
  if (x == null || !Number.isFinite(x)) return "—";
  return `${(x * 100).toFixed(2)}%`;
}

function num(x?: number) {
  if (x == null || !Number.isFinite(x)) return "—";
  return x.toFixed(2);
}

function drawdown(eq: number[]) {
  let peak = -Infinity;
  return eq.map((v) => {
    if (!Number.isFinite(v)) return NaN;
    peak = Math.max(peak, v);
    if (!Number.isFinite(peak) || peak === 0) return NaN;
    return v / peak - 1;
  });
}

function chipHref(t: string) {
  return `/ticker/${encodeURIComponent(t)}`;
}

export default function PortfolioClient({
  meta,
  metrics,
  dates,
  equity,
  portfolio_return,
  holdings = [],
  benchmark_series,
  sp500_price_series,
}: Props) {
  const [showHoldings, setShowHoldings] = useState(true);

  const last = useMemo(() => {
    const lastEq = equity.at(-1);
    const lastRet = portfolio_return.at(-1);
    return {
      lastEq: lastEq ?? 1,
      lastRet: lastRet ?? 0,
    };
  }, [equity, portfolio_return]);

  const latestHolding = holdings.at(-1);

  const ddPort = useMemo(() => drawdown(equity), [equity]);
  const ddSpy = useMemo(
    () => (benchmark_series?.equity?.length ? drawdown(benchmark_series.equity) : []),
    [benchmark_series]
  );
  const ddSpx = useMemo(
    () => (sp500_price_series?.equity?.length ? drawdown(sp500_price_series.equity) : []),
    [sp500_price_series]
  );

  const perfSeries = useMemo(() => {
    const s = [
      { label: "Strategy (L/S)", values: equity, strokeClassName: "stroke-neutral-900", dotClassName: "fill-neutral-900" },
    ];
    if (benchmark_series?.equity?.length) {
      s.push({
        label: benchmark_series.ticker ?? "SPY",
        values: benchmark_series.equity,
        strokeClassName: "stroke-blue-600",
        dotClassName: "fill-blue-600",
      });
    }
    if (sp500_price_series?.equity?.length) {
      s.push({
        label: sp500_price_series.ticker ?? "SPX",
        values: sp500_price_series.equity,
        strokeClassName: "stroke-fuchsia-600",
        dotClassName: "fill-fuchsia-600",
      });
    }
    return s;
  }, [equity, benchmark_series, sp500_price_series]);

  const ddSeries = useMemo(() => {
    const s = [
      { label: "Strategy DD", values: ddPort, strokeClassName: "stroke-rose-600", dotClassName: "fill-rose-600" },
    ];
    if (ddSpy.length) {
      s.push({
        label: `${benchmark_series?.ticker ?? "SPY"} DD`,
        values: ddSpy,
        strokeClassName: "stroke-sky-600",
        dotClassName: "fill-sky-600",
      });
    }
    if (ddSpx.length) {
      s.push({
        label: `${sp500_price_series?.ticker ?? "SPX"} DD`,
        values: ddSpx,
        strokeClassName: "stroke-violet-600",
        dotClassName: "fill-violet-600",
      });
    }
    return s;
  }, [ddPort, ddSpy, ddSpx, benchmark_series, sp500_price_series]);

  return (
    <main className="min-h-screen bg-gradient-to-b from-indigo-50 via-white to-emerald-50">
      <div className="mx-auto max-w-7xl px-4 py-10 space-y-10">
        {/* Header */}
        <div className="rounded-3xl border bg-white/80 backdrop-blur p-6 shadow-sm">
          <div className="flex items-start justify-between gap-6">
            <div>
              <h1 className="text-3xl font-bold tracking-tight">Portfolio Strategy</h1>
              <div className="text-sm text-neutral-700 mt-2">
                {meta?.rebalance ? (
                  <div className="flex flex-wrap gap-x-2 gap-y-1">
                    <span className="rounded-full bg-indigo-100 text-indigo-800 px-3 py-1 text-xs font-semibold">
                      Rebalance: {meta.rebalance}
                    </span>
                    <span className="rounded-full bg-emerald-100 text-emerald-800 px-3 py-1 text-xs font-semibold">
                      Signal: {meta.signal}
                    </span>
                    <span className="rounded-full bg-amber-100 text-amber-800 px-3 py-1 text-xs font-semibold">
                      Lag: {meta.lag_days ?? 1}d
                    </span>
                    <span className="rounded-full bg-fuchsia-100 text-fuchsia-800 px-3 py-1 text-xs font-semibold">
                      K: {meta.k}
                    </span>
                    <span className="rounded-full bg-sky-100 text-sky-800 px-3 py-1 text-xs font-semibold">
                      Long/Short: {meta.long_short ? "Yes" : "No"}
                    </span>
                    {meta?.universe_size_used ? (
                      <span className="rounded-full bg-neutral-100 text-neutral-800 px-3 py-1 text-xs font-semibold">
                        Universe used: {meta.universe_size_used}
                      </span>
                    ) : null}
                  </div>
                ) : (
                  "Daily long/short equity curve + holdings"
                )}
              </div>
            </div>

            <button
              className="rounded-2xl border bg-white px-4 py-2 text-sm text-neutral-800 hover:bg-neutral-50 shadow-sm"
              onClick={() => setShowHoldings((v) => !v)}
            >
              {showHoldings ? "Hide Holdings" : "Show Holdings"}
            </button>
          </div>

          {/* explainers */}
          <div className="mt-6 grid grid-cols-1 lg:grid-cols-2 gap-4">
            <details className="border rounded-2xl p-4 bg-white">
              <summary className="cursor-pointer select-none text-sm font-semibold">
               ❓ How this portfolio strategy works (weekly lagged sentiment ranking)
              </summary>
              <div className="mt-3 text-sm text-neutral-700 space-y-2">
                <p>
                  We treat each stock’s news sentiment as a trading signal (either <b>daily</b> sentiment or its <b>7-day moving average</b>).
                </p>
                <p>
                  On each rebalance date (typically <b>weekly</b>), we rank the universe by the signal observed with a <b>lag</b> (e.g., yesterday),
                  then go <b>long</b> the top-K tickers and (optionally) <b>short</b> the bottom-K tickers with equal weights.
                </p>
                <p className="text-neutral-600">
                  The lag + “apply weights from next trading day” rule is used to reduce look-ahead bias.
                </p>
              </div>
            </details>

            <details className="border rounded-2xl p-4 bg-white">
              <summary className="cursor-pointer select-none text-sm font-semibold">
                📌 What lines are shown on the charts?
              </summary>
              <div className="mt-3 text-sm text-neutral-700 space-y-2">
                <p>
                  The performance chart compares <b>Strategy Equity</b> to <b>SPY</b> (if available) and the <b>S&amp;P 500 index (SPX)</b>.
                </p>
                <p>
                  SPX equity is constructed from <code>data/SPX/sp500_index.json</code> and filled using <code>^GSPC/^SPX</code> snapshots if needed,
                  so all series share the same date axis.
                </p>
              </div>
            </details>
          </div>
        </div>

        {/* KPI cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div className="rounded-2xl p-5 shadow-sm border bg-white">
            <div className="text-sm text-neutral-500 mb-1">Current Equity</div>
            <div className="text-2xl font-semibold tabular-nums">{last.lastEq.toFixed(4)}</div>
            <div className="text-sm text-neutral-600 mt-1">Latest daily: {pct(last.lastRet)}</div>
          </div>

          <div className="rounded-2xl p-5 shadow-sm border bg-white">
            <div className="text-sm text-neutral-500 mb-1">Cumulative Return</div>
            <div className="text-2xl font-semibold tabular-nums">{pct(metrics?.cumulative_return)}</div>
            <div className="text-sm text-neutral-600 mt-1">Max DD: {pct(metrics?.max_drawdown)}</div>
          </div>

          <div className="rounded-2xl p-5 shadow-sm border bg-white">
            <div className="text-sm text-neutral-500 mb-1">Annualized</div>
            <div className="text-sm text-neutral-700 mt-1">
              Return: <span className="font-semibold tabular-nums">{pct(metrics?.annualized_return)}</span>
            </div>
            <div className="text-sm text-neutral-700">
              Vol: <span className="font-semibold tabular-nums">{pct(metrics?.annualized_vol)}</span>
            </div>
          </div>

          <div className="rounded-2xl p-5 shadow-sm border bg-white">
            <div className="text-sm text-neutral-500 mb-1">Sharpe & Hit Rate</div>
            <div className="text-sm text-neutral-700 mt-1">
              Sharpe: <span className="font-semibold tabular-nums">{num(metrics?.sharpe)}</span>
            </div>
            <div className="text-sm text-neutral-700">
              Hit: <span className="font-semibold tabular-nums">{pct(metrics?.hit_rate)}</span>
            </div>
          </div>
        </div>

        {/* Performance chart */}
        <section className="rounded-3xl p-6 shadow-sm border bg-white space-y-5">
          <div className="flex items-end justify-between gap-4">
            <div>
              <h3 className="font-semibold text-lg">Performance vs Benchmarks</h3>
              <div className="text-sm text-neutral-600">All lines are normalized to 1.00 at start.</div>
            </div>
            <div className="text-xs text-neutral-500">baseline: 1.00</div>
          </div>

          <PortfolioChart
            dates={dates}
            series={perfSeries}
            height={520}
            baselineValue={1}
            yLabel="Equity (normalized)"
            valueFormat={(v) => v.toFixed(4)}
          />
        </section>

        {/* Drawdown chart */}
        <section className="rounded-3xl p-6 shadow-sm border bg-white space-y-5">
          <div className="flex items-end justify-between gap-4">
            <div>
              <h3 className="font-semibold text-lg">Drawdowns</h3>
              <div className="text-sm text-neutral-600">Peak-to-trough performance comparison.</div>
            </div>
            <div className="text-xs text-neutral-500">baseline: 0.00</div>
          </div>

          <PortfolioChart
            dates={dates}
            series={ddSeries}
            height={360}
            baselineValue={0}
            yLabel="Drawdown"
            valueFormat={(v) => `${(v * 100).toFixed(2)}%`}
          />
        </section>

        {/* Holdings */}
        {showHoldings ? (
          <section className="rounded-3xl p-6 shadow-sm border bg-white space-y-4">
            <div className="flex items-center justify-between">
              <h3 className="font-semibold text-lg">Latest Holdings</h3>
              <div className="text-sm text-neutral-600">
                {latestHolding?.date ? (
                  <>
                    As of <span className="font-medium text-neutral-900">{latestHolding.date}</span>
                  </>
                ) : (
                  "—"
                )}
              </div>
            </div>

            {!latestHolding ? (
              <div className="text-sm text-neutral-600">No holdings found in portfolio_strategy.json.</div>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="rounded-2xl border bg-emerald-50 p-4">
                  <div className="text-sm font-semibold mb-2 text-emerald-900">Long</div>
                  <div className="flex flex-wrap gap-2">
                    {latestHolding.long.map((t) => (
                      <Link
                        key={t}
                        href={chipHref(t)}
                        className="rounded-full bg-white border px-3 py-1 text-sm hover:bg-emerald-100 transition"
                        title={`Go to ${t}`}
                      >
                        {t}
                      </Link>
                    ))}
                  </div>
                </div>

                <div className="rounded-2xl border bg-rose-50 p-4">
                  <div className="text-sm font-semibold mb-2 text-rose-900">Short</div>
                  <div className="flex flex-wrap gap-2">
                    {latestHolding.short.map((t) => (
                      <Link
                        key={t}
                        href={chipHref(t)}
                        className="rounded-full bg-white border px-3 py-1 text-sm hover:bg-rose-100 transition"
                        title={`Go to ${t}`}
                      >
                        {t}
                      </Link>
                    ))}
                    {!latestHolding.short.length ? (
                      <span className="text-sm text-neutral-600">Short leg disabled.</span>
                    ) : null}
                  </div>
                </div>
              </div>
            )}
          </section>
        ) : null}
      </div>
    </main>
  );
}
