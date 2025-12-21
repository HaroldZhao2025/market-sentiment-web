"use client";

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

type Props = {
  meta?: Meta;
  metrics?: Metrics;
  dates: string[];
  equity: number[];
  portfolio_return: number[];
  holdings?: Holding[];
  benchmark_series?: {
    ticker: string;
    equity: number[];
  };
};

function pct(x?: number) {
  if (x == null || !Number.isFinite(x)) return "—";
  return `${(x * 100).toFixed(2)}%`;
}

function num(x?: number) {
  if (x == null || !Number.isFinite(x)) return "—";
  return x.toFixed(2);
}

export default function PortfolioClient({
  meta,
  metrics,
  dates,
  equity,
  portfolio_return,
  holdings = [],
  benchmark_series,
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

  return (
    <div className="mx-auto max-w-7xl px-4 py-8 space-y-8">
      <div className="flex items-start justify-between gap-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Portfolio</h1>
          <div className="text-sm text-neutral-600 mt-1">
            {meta?.rebalance ? (
              <>
                Rebalance: <span className="font-medium text-neutral-800">{meta.rebalance}</span> · Signal:{" "}
                <span className="font-medium text-neutral-800">{meta.signal}</span> · K:{" "}
                <span className="font-medium text-neutral-800">{meta.k}</span> · Long/Short:{" "}
                <span className="font-medium text-neutral-800">{meta.long_short ? "Yes" : "No"}</span>
              </>
            ) : (
              "Daily long/short equity curve + holdings"
            )}
          </div>
        </div>

        <button
          className="rounded-xl border bg-white px-3 py-2 text-sm text-neutral-700 hover:bg-neutral-50"
          onClick={() => setShowHoldings((v) => !v)}
        >
          {showHoldings ? "Hide Holdings" : "Show Holdings"}
        </button>
      </div>

      <div className="rounded-2xl p-6 shadow-sm border bg-white space-y-5">
        <h3 className="font-semibold">Equity Curve</h3>
        <PortfolioChart
          dates={dates}
          portfolioEquity={equity}
          benchmarkEquity={benchmark_series?.equity}
          benchmarkLabel={benchmark_series?.ticker ?? "SPY"}
          height={520}
        />
      </div>

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
          <div className="text-sm text-neutral-700 mt-1">Return: <span className="font-semibold tabular-nums">{pct(metrics?.annualized_return)}</span></div>
          <div className="text-sm text-neutral-700">Vol: <span className="font-semibold tabular-nums">{pct(metrics?.annualized_vol)}</span></div>
        </div>

        <div className="rounded-2xl p-5 shadow-sm border bg-white">
          <div className="text-sm text-neutral-500 mb-1">Sharpe & Hit Rate</div>
          <div className="text-sm text-neutral-700 mt-1">Sharpe: <span className="font-semibold tabular-nums">{num(metrics?.sharpe)}</span></div>
          <div className="text-sm text-neutral-700">Hit: <span className="font-semibold tabular-nums">{pct(metrics?.hit_rate)}</span></div>
        </div>
      </div>

      {showHoldings ? (
        <div className="rounded-2xl p-6 shadow-sm border bg-white space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="font-semibold">Latest Holdings</h3>
            <div className="text-sm text-neutral-600">
              {latestHolding?.date ? <>As of <span className="font-medium text-neutral-800">{latestHolding.date}</span></> : "—"}
            </div>
          </div>

          {!latestHolding ? (
            <div className="text-sm text-neutral-600">No holdings found in portfolio_strategy.json.</div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="rounded-2xl border bg-neutral-50 p-4">
                <div className="text-sm font-semibold mb-2">Long</div>
                <div className="flex flex-wrap gap-2">
                  {latestHolding.long.map((t) => (
                    <span key={t} className="rounded-full bg-white border px-3 py-1 text-sm">
                      {t}
                    </span>
                  ))}
                </div>
              </div>

              <div className="rounded-2xl border bg-neutral-50 p-4">
                <div className="text-sm font-semibold mb-2">Short</div>
                <div className="flex flex-wrap gap-2">
                  {latestHolding.short.map((t) => (
                    <span key={t} className="rounded-full bg-white border px-3 py-1 text-sm">
                      {t}
                    </span>
                  ))}
                  {!latestHolding.short.length ? (
                    <span className="text-sm text-neutral-600">Short leg disabled.</span>
                  ) : null}
                </div>
              </div>
            </div>
          )}
        </div>
      ) : null}
    </div>
  );
}
