"use client";

import { useMemo, useState } from "react";
import type { AttributionRow } from "../../lib/intelligence";

type Props = { rows: AttributionRow[] };

function tone(v: number | null) {
  if (v == null) return "text-neutral-500";
  if (v > 0) return "text-emerald-300";
  if (v < 0) return "text-rose-300";
  return "text-neutral-300";
}

function pct(v: number | null, d = 2) {
  return v == null ? "—" : `${(v * 100).toFixed(d)}%`;
}

export default function AttributionExplorer({ rows }: Props) {
  const [level, setLevel] = useState<"sector" | "industry">("sector");
  const [sector, setSector] = useState("All");
  const sectors = useMemo(() => ["All", ...Array.from(new Set(rows.filter((r) => r.level === "sector").map((r) => r.sector))).sort()], [rows]);

  const visible = useMemo(() => rows
    .filter((r) => r.level === level)
    .filter((r) => level === "sector" || sector === "All" || r.sector === sector)
    .slice()
    .sort((a, b) => Math.abs(b.contribution) - Math.abs(a.contribution)), [rows, level, sector]);

  const maxAbs = Math.max(...visible.map((r) => Math.abs(r.contribution)), 1e-9);

  return (
    <div className="ambient-panel overflow-hidden">
      <div className="flex flex-col gap-3 border-b border-white/10 px-5 py-4 md:flex-row md:items-center md:justify-between">
        <div>
          <div className="eyebrow">Attribution V2</div>
          <h3 className="mt-1 text-lg font-semibold text-white">Company → industry → sector → index</h3>
        </div>
        <div className="flex flex-wrap gap-2">
          {level === "industry" ? (
            <select value={sector} onChange={(e) => setSector(e.target.value)} className="rounded-xl border border-white/10 bg-neutral-900 px-3 py-2 text-xs text-neutral-300 outline-none">
              {sectors.map((s) => <option key={s}>{s}</option>)}
            </select>
          ) : null}
          <div className="flex rounded-xl border border-white/10 bg-black/30 p-1">
            <button type="button" onClick={() => setLevel("sector")} className={`rounded-lg px-3 py-1.5 text-xs ${level === "sector" ? "bg-white/10 text-white" : "text-neutral-500"}`}>Sector</button>
            <button type="button" onClick={() => setLevel("industry")} className={`rounded-lg px-3 py-1.5 text-xs ${level === "industry" ? "bg-white/10 text-white" : "text-neutral-500"}`}>Industry</button>
          </div>
        </div>
      </div>

      <div className="divide-y divide-white/[0.06]">
        {visible.map((r) => {
          const width = Math.max(2, (Math.abs(r.contribution) / maxAbs) * 100);
          return (
            <div key={r.key} className="grid gap-3 px-5 py-3 md:grid-cols-[minmax(180px,1.5fr)_minmax(180px,2fr)_100px_110px_110px] md:items-center">
              <div>
                <div className="text-sm font-medium text-neutral-200">{r.label}</div>
                <div className="mt-0.5 text-[10px] text-neutral-600">{r.observed_tickers}/{r.total_tickers} tickers observed · {r.news_count} news</div>
              </div>
              <div className="h-2 overflow-hidden rounded-full bg-white/[0.05]">
                <div className={`h-full rounded-full ${r.contribution >= 0 ? "bg-emerald-400" : "bg-rose-400"}`} style={{ width: `${width}%` }} />
              </div>
              <div className={`text-right font-mono text-xs ${tone(r.contribution)}`}>{r.contribution >= 0 ? "+" : ""}{(r.contribution * 10000).toFixed(2)} bps</div>
              <div className={`text-right font-mono text-xs ${tone(r.sentiment)}`}>{r.sentiment == null ? "—" : `${r.sentiment >= 0 ? "+" : ""}${r.sentiment.toFixed(3)}`}</div>
              <div className="text-right font-mono text-xs text-neutral-500">{pct(r.observed_weight, 2)} obs wt</div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
