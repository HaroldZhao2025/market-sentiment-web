"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import { hrefs } from "../lib/paths";

export type TickerRow = {
  ticker: string;
  price: number | null;
  sentiment: number | null;
  sentimentChange: number | null;
  dailyReturn: number | null;
};

type Props = {
  rows: TickerRow[];
};

type SortKey = "Alphabet" | "Sentiment" | "Change" | "Return" | "Divergence";

type ViewKey = "All" | "Positive" | "Negative" | "Divergence";

function fmtPrice(x: number | null) {
  if (x == null || !Number.isFinite(x)) return "—";
  return x.toFixed(2);
}

function fmtSignal(x: number | null, digits = 2) {
  if (x == null || !Number.isFinite(x)) return "—";
  return `${x > 0 ? "+" : ""}${x.toFixed(digits)}`;
}

function fmtPct(x: number | null) {
  if (x == null || !Number.isFinite(x)) return "—";
  return `${x > 0 ? "+" : ""}${(x * 100).toFixed(2)}%`;
}

function signalClass(v: number | null) {
  if (v == null || !Number.isFinite(v)) return "text-neutral-500";
  if (v > 0.03) return "text-emerald-400";
  if (v < -0.03) return "text-rose-400";
  return "text-neutral-300";
}

function hasDivergence(row: TickerRow) {
  if (row.sentiment == null || row.dailyReturn == null) return false;
  return Math.abs(row.sentiment) >= 0.05 && Math.abs(row.dailyReturn) >= 0.002 && row.sentiment * row.dailyReturn < 0;
}

function divergenceScore(row: TickerRow) {
  if (row.sentiment == null || row.dailyReturn == null) return -Infinity;
  return Math.abs(row.sentiment) * Math.abs(row.dailyReturn);
}

export default function TickerGridClient({ rows }: Props) {
  const [sort, setSort] = useState<SortKey>("Divergence");
  const [view, setView] = useState<ViewKey>("All");
  const [query, setQuery] = useState("");

  const filtered = useMemo(() => {
    const q = query.trim().toUpperCase();
    return rows.filter((row) => {
      if (q && !row.ticker.includes(q)) return false;
      if (view === "Positive" && !(row.sentiment != null && row.sentiment > 0)) return false;
      if (view === "Negative" && !(row.sentiment != null && row.sentiment < 0)) return false;
      if (view === "Divergence" && !hasDivergence(row)) return false;
      return true;
    });
  }, [rows, view, query]);

  const sorted = useMemo(() => {
    const arr = [...filtered];
    const n = (x: number | null) => (x == null || !Number.isFinite(x) ? -Infinity : x);

    if (sort === "Alphabet") arr.sort((a, b) => a.ticker.localeCompare(b.ticker));
    if (sort === "Sentiment") arr.sort((a, b) => n(b.sentiment) - n(a.sentiment));
    if (sort === "Change") arr.sort((a, b) => n(b.sentimentChange) - n(a.sentimentChange));
    if (sort === "Return") arr.sort((a, b) => n(b.dailyReturn) - n(a.dailyReturn));
    if (sort === "Divergence") arr.sort((a, b) => divergenceScore(b) - divergenceScore(a));
    return arr;
  }, [filtered, sort]);

  const divergenceCount = useMemo(() => rows.filter(hasDivergence).length, [rows]);

  const buttonClass = (active: boolean) =>
    `rounded-lg px-3 py-2 text-xs font-medium transition ${
      active
        ? "bg-white text-neutral-950"
        : "text-neutral-400 hover:bg-white/[0.06] hover:text-white"
    }`;

  return (
    <div className="space-y-4">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
        <div className="flex flex-wrap items-center gap-2">
          {(["All", "Positive", "Negative", "Divergence"] as ViewKey[]).map((key) => (
            <button key={key} className={buttonClass(view === key)} onClick={() => setView(key)}>
              {key}{key === "Divergence" ? ` (${divergenceCount})` : ""}
            </button>
          ))}
        </div>

        <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
          <label className="relative">
            <span className="sr-only">Search ticker</span>
            <input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Search ticker…"
              className="w-full rounded-xl border border-white/10 bg-black/20 px-3 py-2 text-sm text-white outline-none placeholder:text-neutral-600 focus:border-emerald-400/40 sm:w-44"
            />
          </label>
          <select
            value={sort}
            onChange={(event) => setSort(event.target.value as SortKey)}
            className="rounded-xl border border-white/10 bg-neutral-900 px-3 py-2 text-sm text-neutral-300 outline-none focus:border-emerald-400/40"
          >
            <option value="Divergence">Sort: divergence</option>
            <option value="Sentiment">Sort: sentiment</option>
            <option value="Change">Sort: sentiment change</option>
            <option value="Return">Sort: daily return</option>
            <option value="Alphabet">Sort: ticker</option>
          </select>
        </div>
      </div>

      <div className="text-xs text-neutral-500">
        Showing {sorted.length} of {rows.length} tickers. Divergence flags opposite-signed news sentiment and price reaction.
      </div>

      <div className="table-shell overflow-x-auto">
        <table className="w-full min-w-[720px] text-left text-sm">
          <thead className="border-b border-white/10 bg-white/[0.025] text-[11px] uppercase tracking-[0.12em] text-neutral-500">
            <tr>
              <th className="px-4 py-3 font-medium">Ticker</th>
              <th className="px-4 py-3 font-medium">Price</th>
              <th className="px-4 py-3 font-medium">1D return</th>
              <th className="px-4 py-3 font-medium">Sentiment</th>
              <th className="px-4 py-3 font-medium">Δ sentiment</th>
              <th className="px-4 py-3 font-medium">Signal</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/5">
            {sorted.map((row) => {
              const divergent = hasDivergence(row);
              return (
                <tr key={row.ticker} className="transition hover:bg-white/[0.035]">
                  <td className="px-4 py-3">
                    <Link href={hrefs.ticker(row.ticker)} className="font-semibold text-white hover:text-emerald-300">
                      {row.ticker}
                    </Link>
                  </td>
                  <td className="px-4 py-3 tabular-nums text-neutral-300">{fmtPrice(row.price)}</td>
                  <td className={`px-4 py-3 tabular-nums ${signalClass(row.dailyReturn)}`}>{fmtPct(row.dailyReturn)}</td>
                  <td className={`px-4 py-3 tabular-nums ${signalClass(row.sentiment)}`}>{fmtSignal(row.sentiment)}</td>
                  <td className={`px-4 py-3 tabular-nums ${signalClass(row.sentimentChange)}`}>{fmtSignal(row.sentimentChange)}</td>
                  <td className="px-4 py-3">
                    {divergent ? (
                      <span className="rounded-full border border-amber-400/20 bg-amber-400/10 px-2.5 py-1 text-xs font-medium text-amber-300">
                        Divergence
                      </span>
                    ) : (
                      <span className="text-xs text-neutral-600">—</span>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
