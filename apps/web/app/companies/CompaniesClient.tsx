"use client";

import Link from "next/link";
import { useMemo, useState } from "react";

export type CompanyRow = {
  ticker: string;
  name?: string;
  sector?: string;
  industry?: string;
  universe?: string;
  latest_price?: number | null;
  return_1d?: number | null;
  sentiment?: number | null;
  news_count?: number | null;
  earnings_available?: boolean;
};

type Props = { rows: CompanyRow[]; generatedAt?: string | null };

function finite(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}
function tone(value: number | null) {
  if (value == null) return "text-neutral-500";
  if (value > 0) return "text-emerald-300";
  if (value < 0) return "text-rose-300";
  return "text-neutral-300";
}
function pct(value: number | null) {
  return value == null ? "—" : `${value > 0 ? "+" : ""}${(value * 100).toFixed(2)}%`;
}

export default function CompaniesClient({ rows, generatedAt }: Props) {
  const [query, setQuery] = useState("");
  const [universe, setUniverse] = useState("All");
  const [sector, setSector] = useState("All");
  const [sort, setSort] = useState<"ticker" | "return" | "sentiment" | "news">("ticker");

  const universes = useMemo(() => ["All", ...Array.from(new Set(rows.map((r) => r.universe || "Other"))).sort()], [rows]);
  const sectors = useMemo(() => ["All", ...Array.from(new Set(rows.map((r) => r.sector || "Unknown"))).sort()], [rows]);
  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    const result = rows.filter((row) => {
      if (universe !== "All" && (row.universe || "Other") !== universe) return false;
      if (sector !== "All" && (row.sector || "Unknown") !== sector) return false;
      return !q || [row.ticker, row.name, row.sector, row.industry].some((value) => String(value || "").toLowerCase().includes(q));
    });
    return result.slice().sort((a, b) => {
      if (sort === "ticker") return a.ticker.localeCompare(b.ticker);
      const av = sort === "return" ? finite(a.return_1d) : sort === "sentiment" ? finite(a.sentiment) : finite(a.news_count);
      const bv = sort === "return" ? finite(b.return_1d) : sort === "sentiment" ? finite(b.sentiment) : finite(b.news_count);
      if (av == null && bv == null) return a.ticker.localeCompare(b.ticker);
      if (av == null) return 1;
      if (bv == null) return -1;
      return bv - av;
    });
  }, [rows, query, universe, sector, sort]);

  return (
    <div className="space-y-5">
      <section className="ambient-panel p-4 md:p-5">
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Search ticker, company, sector…" className="rounded-xl border border-white/10 bg-black/30 px-4 py-2.5 text-sm text-white outline-none placeholder:text-neutral-600" />
          <select value={universe} onChange={(e) => setUniverse(e.target.value)} className="rounded-xl border border-white/10 bg-neutral-900 px-3 py-2.5 text-sm text-neutral-300 outline-none">{universes.map((value) => <option key={value}>{value}</option>)}</select>
          <select value={sector} onChange={(e) => setSector(e.target.value)} className="rounded-xl border border-white/10 bg-neutral-900 px-3 py-2.5 text-sm text-neutral-300 outline-none">{sectors.map((value) => <option key={value}>{value}</option>)}</select>
          <select value={sort} onChange={(e) => setSort(e.target.value as typeof sort)} className="rounded-xl border border-white/10 bg-neutral-900 px-3 py-2.5 text-sm text-neutral-300 outline-none"><option value="ticker">Alphabetical</option><option value="return">1D return</option><option value="sentiment">Sentiment</option><option value="news">News attention</option></select>
        </div>
      </section>
      <div className="flex flex-wrap items-center justify-between gap-3 text-xs text-neutral-600"><span>{filtered.length} of {rows.length} companies</span><span>{generatedAt ? `Generated ${generatedAt.slice(0, 19).replace("T", " ")} UTC` : "V5 extended artifact"}</span></div>
      <div className="table-shell overflow-x-auto">
        <table className="w-full min-w-[1120px] text-sm">
          <thead className="border-b border-white/10 bg-white/[0.025] text-left text-[11px] uppercase tracking-[0.1em] text-neutral-600"><tr><th className="px-4 py-3">Company</th><th className="px-4 py-3">Universe</th><th className="px-4 py-3">Sector / industry</th><th className="px-4 py-3 text-right">Price</th><th className="px-4 py-3 text-right">1D</th><th className="px-4 py-3 text-right">Sentiment</th><th className="px-4 py-3 text-right">News</th><th className="px-4 py-3">Explore</th></tr></thead>
          <tbody>{filtered.map((row) => { const ret = finite(row.return_1d); const sent = finite(row.sentiment); return <tr key={row.ticker} className="border-b border-white/[0.06] last:border-0 hover:bg-white/[0.025]"><td className="px-4 py-3"><div className="font-semibold text-white">{row.ticker}</div><div className="max-w-[230px] truncate text-xs text-neutral-500">{row.name || "Company name unavailable"}</div></td><td className="px-4 py-3 text-xs text-neutral-400">{row.universe || "Other"}</td><td className="px-4 py-3"><div className="text-xs text-neutral-300">{row.sector || "Unknown"}</div><div className="max-w-[240px] truncate text-[11px] text-neutral-600">{row.industry || "—"}</div></td><td className={`px-4 py-3 text-right font-mono ${tone(ret)}`}>{finite(row.latest_price)?.toFixed(2) ?? "—"}</td><td className={`px-4 py-3 text-right font-mono ${tone(ret)}`}>{pct(ret)}</td><td className={`px-4 py-3 text-right font-mono ${tone(sent)}`}>{sent == null ? "—" : `${sent > 0 ? "+" : ""}${sent.toFixed(3)}`}</td><td className="px-4 py-3 text-right font-mono text-neutral-400">{finite(row.news_count)?.toFixed(0) ?? "—"}</td><td className="px-4 py-3"><div className="flex flex-wrap gap-3"><Link href={`/ticker/${row.ticker}`} className="text-xs text-emerald-300 hover:underline">Ticker</Link><Link href={`/earnings/${row.ticker}`} className="text-xs text-sky-300 hover:underline">Earnings</Link></div></td></tr>; })}</tbody>
        </table>
      </div>
    </div>
  );
}
