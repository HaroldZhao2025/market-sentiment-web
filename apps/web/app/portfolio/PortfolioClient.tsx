"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import PortfolioChart from "../../components/PortfolioChart";

type Holding = { date: string; long: string[]; short: string[] };
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
  signal?: "day" | "ma7" | "blend";
  lag_days?: number;
  k?: number;
  long_short?: boolean;
  gross_per_side?: number;
  benchmark?: string | null;
  universe_size_used?: number;
};
type EquitySeries = { ticker: string; equity: number[] };
type Props = {
  meta?: Meta;
  metrics?: Metrics;
  dates: string[];
  equity: number[];
  portfolio_return: number[];
  holdings?: Holding[];
  benchmark_series?: EquitySeries | null;
  sp500_price_series?: EquitySeries | null;
};

function pct(x?: number) {
  return x == null || !Number.isFinite(x) ? "—" : `${(x * 100).toFixed(2)}%`;
}
function num(x?: number) {
  return x == null || !Number.isFinite(x) ? "—" : x.toFixed(2);
}
function drawdown(eq: number[]) {
  let peak = -Infinity;
  return eq.map((v) => {
    if (!Number.isFinite(v)) return Number.NaN;
    peak = Math.max(peak, v);
    return peak > 0 ? v / peak - 1 : Number.NaN;
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
  const latestHolding = holdings.at(-1);
  const lastEq = equity.at(-1) ?? 1;
  const lastRet = portfolio_return.at(-1) ?? 0;

  const perfSeries = useMemo(() => {
    const rows = [
      { label: "Strategy", values: equity, strokeClassName: "stroke-emerald-400", dotClassName: "fill-emerald-400" },
    ];
    if (benchmark_series?.equity?.length) {
      rows.push({ label: benchmark_series.ticker || "SPY", values: benchmark_series.equity, strokeClassName: "stroke-sky-400", dotClassName: "fill-sky-400" });
    }
    if (sp500_price_series?.equity?.length) {
      rows.push({ label: sp500_price_series.ticker || "SPX", values: sp500_price_series.equity, strokeClassName: "stroke-fuchsia-400", dotClassName: "fill-fuchsia-400" });
    }
    return rows;
  }, [equity, benchmark_series, sp500_price_series]);

  const ddSeries = useMemo(() => {
    const rows = [
      { label: "Strategy DD", values: drawdown(equity), strokeClassName: "stroke-rose-400", dotClassName: "fill-rose-400" },
    ];
    if (benchmark_series?.equity?.length) {
      rows.push({ label: `${benchmark_series.ticker || "SPY"} DD`, values: drawdown(benchmark_series.equity), strokeClassName: "stroke-sky-400", dotClassName: "fill-sky-400" });
    }
    if (sp500_price_series?.equity?.length) {
      rows.push({ label: `${sp500_price_series.ticker || "SPX"} DD`, values: drawdown(sp500_price_series.equity), strokeClassName: "stroke-fuchsia-400", dotClassName: "fill-fuchsia-400" });
    }
    return rows;
  }, [equity, benchmark_series, sp500_price_series]);

  return (
    <main className="space-y-8">
      <section className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <div className="eyebrow">Strategy lab</div>
          <h1 className="page-title mt-2">Portfolio strategy</h1>
          <p className="mt-3 max-w-3xl text-sm leading-6 text-neutral-400">
            Lagged sentiment and price signals translated into a reproducible long/short backtest with explicit benchmark comparison.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          {meta?.rebalance ? <span className="pill">Rebalance · {meta.rebalance}</span> : null}
          {meta?.signal ? <span className="pill">Signal · {meta.signal}</span> : null}
          {typeof meta?.lag_days === "number" ? <span className="pill">Lag · {meta.lag_days}d</span> : null}
          {typeof meta?.k === "number" ? <span className="pill">K · {meta.k}</span> : null}
          <Link href="/methodology" className="pill">Methodology →</Link>
        </div>
      </section>

      <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
        <div className="kpi">
          <div className="kpi-label">Current equity</div>
          <div className="kpi-value">{lastEq.toFixed(4)}</div>
          <div className="kpi-sub">Latest daily return · {pct(lastRet)}</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Cumulative return</div>
          <div className="kpi-value">{pct(metrics?.cumulative_return)}</div>
          <div className="kpi-sub">Max drawdown · {pct(metrics?.max_drawdown)}</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Annualized return</div>
          <div className="kpi-value">{pct(metrics?.annualized_return)}</div>
          <div className="kpi-sub">Volatility · {pct(metrics?.annualized_vol)}</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Sharpe</div>
          <div className="kpi-value">{num(metrics?.sharpe)}</div>
          <div className="kpi-sub">Hit rate · {pct(metrics?.hit_rate)}</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Universe used</div>
          <div className="kpi-value">{meta?.universe_size_used?.toLocaleString() ?? "—"}</div>
          <div className="kpi-sub">Long/short · {meta?.long_short ? "enabled" : "disabled"}</div>
        </div>
      </section>

      <section className="space-y-3">
        <div>
          <div className="eyebrow">Performance</div>
          <h2 className="section-title mt-1">Strategy vs benchmarks</h2>
          <p className="section-copy">All equity lines are normalized to 1.00 at the comparison start.</p>
        </div>
        <div className="legacy-dark ambient-panel p-4 md:p-6">
          <PortfolioChart
            dates={dates}
            series={perfSeries}
            height={520}
            baselineValue={1}
            yLabel="Equity (normalized)"
            valueFormat={(v) => v.toFixed(4)}
          />
        </div>
      </section>

      <section className="space-y-3">
        <div>
          <div className="eyebrow">Risk</div>
          <h2 className="section-title mt-1">Drawdowns</h2>
          <p className="section-copy">Peak-to-trough losses reveal how the strategy behaves when the signal is wrong or market regimes change.</p>
        </div>
        <div className="legacy-dark ambient-panel p-4 md:p-6">
          <PortfolioChart
            dates={dates}
            series={ddSeries}
            height={360}
            baselineValue={0}
            yLabel="Drawdown"
            valueFormat={(v) => `${(v * 100).toFixed(2)}%`}
          />
        </div>
      </section>

      <section className="space-y-3">
        <div className="flex flex-wrap items-end justify-between gap-3">
          <div>
            <div className="eyebrow">Current positioning</div>
            <h2 className="section-title mt-1">Latest holdings</h2>
          </div>
          <button type="button" className="pill" onClick={() => setShowHoldings((v) => !v)}>
            {showHoldings ? "Hide holdings" : "Show holdings"}
          </button>
        </div>

        {showHoldings ? (
          latestHolding ? (
            <div className="grid gap-3 lg:grid-cols-2">
              <div className="rounded-2xl border border-emerald-400/15 bg-emerald-400/[0.045] p-5">
                <div className="flex items-center justify-between gap-3">
                  <div className="text-xs font-semibold uppercase tracking-[0.14em] text-emerald-300">Long book</div>
                  <div className="text-xs text-neutral-600">{latestHolding.date}</div>
                </div>
                <div className="mt-4 flex flex-wrap gap-2">
                  {latestHolding.long.map((t) => (
                    <Link key={t} href={chipHref(t)} className="rounded-full border border-emerald-400/15 bg-black/20 px-3 py-1.5 text-sm text-emerald-100 transition hover:bg-emerald-400/10">{t}</Link>
                  ))}
                </div>
              </div>

              <div className="rounded-2xl border border-rose-400/15 bg-rose-400/[0.045] p-5">
                <div className="flex items-center justify-between gap-3">
                  <div className="text-xs font-semibold uppercase tracking-[0.14em] text-rose-300">Short book</div>
                  <div className="text-xs text-neutral-600">{latestHolding.date}</div>
                </div>
                <div className="mt-4 flex flex-wrap gap-2">
                  {latestHolding.short.length ? latestHolding.short.map((t) => (
                    <Link key={t} href={chipHref(t)} className="rounded-full border border-rose-400/15 bg-black/20 px-3 py-1.5 text-sm text-rose-100 transition hover:bg-rose-400/10">{t}</Link>
                  )) : <span className="text-sm text-neutral-600">Short leg disabled.</span>}
                </div>
              </div>
            </div>
          ) : <div className="card p-5 text-sm text-neutral-500">No holdings found in the generated strategy artifact.</div>
        ) : null}
      </section>

      <section className="card p-5 text-xs leading-6 text-neutral-500">
        The portfolio is a research backtest, not an investment recommendation. Signal construction, lag, transaction costs, and weighting assumptions are documented on the Methodology page.
      </section>
    </main>
  );
}
