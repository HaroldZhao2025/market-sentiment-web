"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import type { ScreenerRow } from "../../lib/intelligence";

type SortKey = "weight" | "sentiment" | "sentiment_change" | "return_1d" | "divergence" | "n_total" | "novelty";

type Props = { rows: ScreenerRow[] };

function finite(x: unknown): number | null {
  if (x === null || x === undefined || x === "") return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function tone(v: number | null) {
  if (v == null) return "text-neutral-500";
  if (v > 0) return "text-emerald-300";
  if (v < 0) return "text-rose-300";
  return "text-neutral-300";
}

function fmt(v: number | null, d = 3) {
  return v == null ? "—" : `${v > 0 ? "+" : ""}${v.toFixed(d)}`;
}

function pct(v: number | null, d = 2) {
  return v == null ? "—" : `${v > 0 ? "+" : ""}${(v * 100).toFixed(d)}%`;
}

export default function ScreenerClient({ rows }: Props) {
  const [q, setQ] = useState("");
  const [sector, setSector] = useState("All");
  const [evidenceOnly, setEvidenceOnly] = useState(true);
  const [sort, setSort] = useState<SortKey>("divergence");
  const [direction, setDirection] = useState<"desc" | "asc">("desc");

  const sectors = useMemo(() => ["All", ...Array.from(new Set(rows.map((r) => r.sector || "Unknown"))).sort()], [rows]);

  const filtered = useMemo(() => {
    const query = q.trim().toLowerCase();
    return rows
      .filter((r) => sector === "All" || (r.sector || "Unknown") === sector)
      .filter((r) => !evidenceOnly || finite(r.sentiment) != null)
      .filter((r) => !query || [r.symbol, r.name, r.sector, r.industry, r.event_theme].some((x) => String(x || "").toLowerCase().includes(query)))
      .slice()
      .sort((a, b) => {
        const av = finite(a[sort]);
        const bv = finite(b[sort]);
        if (av == null && bv == null) return a.symbol.localeCompare(b.symbol);
        if (av == null) return 1;
        if (bv == null) return -1;
        return direction === "desc" ? bv - av : av - bv;
      });
  }, [rows, q, sector, evidenceOnly, sort, direction]);

  return (
    <div className="space-y-5">
      <section className="ambient-panel p-4 md:p-5">
        <div className="grid gap-3 lg:grid-cols-[minmax(0,1fr)_220px_220px_auto]">
          <input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Search ticker, company, sector, industry, event…"
            className="rounded-xl border border-white/10 bg-black/30 px-4 py-2.5 text-sm text-white outline-none placeholder:text-neutral-600 focus:border-white/20"
          />
          <select value={sector} onChange={(e) => setSector(e.target.value)} className="rounded-xl border border-white/10 bg-neutral-900 px-3 py-2.5 text-sm text-neutral-300 outline-none">
            {sectors.map((s) => <option key={s}>{s}</option>)}
          </select>
          <select value={sort} onChange={(e) => setSort(e.target.value as SortKey)} className="rounded-xl border border-white/10 bg-neutral-900 px-3 py-2.5 text-sm text-neutral-300 outline-none">
            <option value="divergence">Sentiment-price divergence</option>
            <option value="sentiment">Sentiment</option>
            <option value="sentiment_change">Sentiment change</option>
            <option value="return_1d">1D return</option>
            <option value="weight">Index weight</option>
            <option value="n_total">News evidence</option>
            <option value="novelty">Event novelty</option>
          </select>
          <button type="button" onClick={() => setDirection((x) => x === "desc" ? "asc" : "desc")} className="rounded-xl border border-white/10 bg-white/[0.04] px-4 py-2.5 text-sm text-neutral-300 hover:bg-white/[0.07]">
            {direction === "desc" ? "High → Low" : "Low → High"}
          </button>
        </div>
        <label className="mt-3 inline-flex items-center gap-2 text-xs text-neutral-500">
          <input type="checkbox" checked={evidenceOnly} onChange={(e) => setEvidenceOnly(e.target.checked)} />
          Only tickers with observed sentiment evidence
        </label>
      </section>

      <div className="flex items-center justify-between text-xs text-neutral-600">
        <span>{filtered.length} constituents</span>
        <span>Missing sentiment remains missing — never coerced to zero.</span>
      </div>

      <div className="table-shell overflow-x-auto">
        <table className="w-full min-w-[1180px] text-sm">
          <thead className="border-b border-white/10 bg-white/[0.025] text-left text-[11px] uppercase tracking-[0.1em] text-neutral-600">
            <tr>
              <th className="px-4 py-3">Ticker</th>
              <th className="px-4 py-3">Sector / industry</th>
              <th className="px-4 py-3 text-right">Weight</th>
              <th className="px-4 py-3 text-right">Price</th>
              <th className="px-4 py-3 text-right">1D return</th>
              <th className="px-4 py-3 text-right">Sentiment</th>
              <th className="px-4 py-3 text-right">Δ sentiment</th>
              <th className="px-4 py-3 text-right">Divergence</th>
              <th className="px-4 py-3">Dominant event</th>
              <th className="px-4 py-3 text-right">Novelty</th>
              <th className="px-4 py-3 text-right">News</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((r) => {
              const s = finite(r.sentiment);
              const ds = finite(r.sentiment_change);
              const ret = finite(r.return_1d);
              const div = finite(r.divergence);
              return (
                <tr key={r.symbol} className="border-b border-white/[0.06] last:border-0 hover:bg-white/[0.025]">
                  <td className="px-4 py-3">
                    <Link href={`/ticker/${r.symbol}`} className="font-semibold text-white hover:text-emerald-300">{r.symbol}</Link>
                    <div className="max-w-[190px] truncate text-[11px] text-neutral-600">{r.name || "—"}</div>
                  </td>
                  <td className="px-4 py-3">
                    <div className="text-xs text-neutral-300">{r.sector || "Unknown"}</div>
                    <div className="max-w-[220px] truncate text-[11px] text-neutral-600">{r.industry || "—"}</div>
                  </td>
                  <td className="px-4 py-3 text-right font-mono text-xs text-neutral-400">{pct(finite(r.weight), 2)}</td>
                  <td className={`px-4 py-3 text-right font-mono ${tone(ret)}`}>{finite(r.price)?.toFixed(2) ?? "—"}</td>
                  <td className={`px-4 py-3 text-right font-mono ${tone(ret)}`}>{pct(ret)}</td>
                  <td className={`px-4 py-3 text-right font-mono ${tone(s)}`}>{fmt(s)}</td>
                  <td className={`px-4 py-3 text-right font-mono ${tone(ds)}`}>{fmt(ds)}</td>
                  <td className={`px-4 py-3 text-right font-mono ${tone(div)}`}>{fmt(div)}</td>
                  <td className="px-4 py-3">
                    <div className="max-w-[190px] truncate text-xs text-neutral-300">{r.event_theme || "No scored event"}</div>
                    <div className="mt-1 text-[10px] text-neutral-600">{r.source_count} sources · disagreement {finite(r.disagreement)?.toFixed(3) ?? "—"}</div>
                  </td>
                  <td className="px-4 py-3 text-right font-mono text-xs text-neutral-400">{finite(r.novelty)?.toFixed(2) ?? "—"}</td>
                  <td className="px-4 py-3 text-right font-mono text-xs text-neutral-400">{finite(r.n_total)?.toFixed(0) ?? "—"}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
