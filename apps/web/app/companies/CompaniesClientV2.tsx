"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import CompanyVisual from "../../components/CompanyVisual";

export type CompanyRowV2 = {
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

type Props = { rows: CompanyRowV2[]; generatedAt?: string | null };
type SortKey = "ticker" | "return" | "sentiment" | "news";

const PAGE_SIZE = 60;

function finite(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function tone(value: number | null) {
  if (value == null) return "text-neutral-500";
  return value > 0 ? "text-emerald-300" : value < 0 ? "text-rose-300" : "text-neutral-300";
}

function pct(value: number | null) {
  return value == null ? "—" : `${value > 0 ? "+" : ""}${(value * 100).toFixed(2)}%`;
}

function sentiment(value: number | null) {
  return value == null ? "—" : `${value > 0 ? "+" : ""}${value.toFixed(3)}`;
}

export default function CompaniesClientV2({ rows, generatedAt }: Props) {
  const [query, setQuery] = useState("");
  const [universe, setUniverse] = useState("All");
  const [sector, setSector] = useState("All");
  const [sort, setSort] = useState<SortKey>("ticker");
  const [view, setView] = useState<"table" | "cards">("table");
  const [page, setPage] = useState(1);

  const universes = useMemo(() => ["All", ...Array.from(new Set(rows.map((row) => row.universe || "Other"))).sort()], [rows]);
  const sectors = useMemo(() => ["All", ...Array.from(new Set(rows.map((row) => row.sector || "Unknown"))).sort()], [rows]);

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

  useEffect(() => setPage(1), [query, universe, sector, sort, view]);

  const pageCount = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const safePage = Math.min(page, pageCount);
  const visible = filtered.slice((safePage - 1) * PAGE_SIZE, safePage * PAGE_SIZE);

  return (
    <div className="space-y-5">
      <section className="ambient-panel p-4 md:p-5">
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
          <input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Search company or ticker" className="rounded-xl border border-white/10 bg-black/30 px-4 py-2.5 text-sm text-white outline-none placeholder:text-neutral-600" />
          <select value={universe} onChange={(e) => setUniverse(e.target.value)} className="rounded-xl border border-white/10 bg-neutral-900 px-3 py-2.5 text-sm text-neutral-300 outline-none">{universes.map((value) => <option key={value}>{value}</option>)}</select>
          <select value={sector} onChange={(e) => setSector(e.target.value)} className="rounded-xl border border-white/10 bg-neutral-900 px-3 py-2.5 text-sm text-neutral-300 outline-none">{sectors.map((value) => <option key={value}>{value}</option>)}</select>
          <select value={sort} onChange={(e) => setSort(e.target.value as SortKey)} className="rounded-xl border border-white/10 bg-neutral-900 px-3 py-2.5 text-sm text-neutral-300 outline-none"><option value="ticker">Alphabetical</option><option value="return">1D return</option><option value="sentiment">Sentiment</option><option value="news">News attention</option></select>
          <div className="grid grid-cols-2 rounded-xl border border-white/10 bg-neutral-900 p-1 text-xs">
            <button type="button" onClick={() => setView("table")} className={`rounded-lg px-3 py-2 transition ${view === "table" ? "bg-white/10 text-white" : "text-neutral-500 hover:text-neutral-300"}`}>Table</button>
            <button type="button" onClick={() => setView("cards")} className={`rounded-lg px-3 py-2 transition ${view === "cards" ? "bg-white/10 text-white" : "text-neutral-500 hover:text-neutral-300"}`}>Cards</button>
          </div>
        </div>
      </section>

      <div className="flex flex-wrap items-center justify-between gap-3 text-xs text-neutral-600">
        <span>{filtered.length.toLocaleString()} companies · page {safePage} of {pageCount}</span>
        <span>{generatedAt ? `Updated ${generatedAt.slice(0, 16).replace("T", " ")} UTC` : "Latest extended snapshot"}</span>
      </div>

      {view === "cards" ? (
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
          {visible.map((row) => {
            const ret = finite(row.return_1d);
            const sent = finite(row.sentiment);
            return (
              <article key={row.ticker} className="rounded-2xl border border-white/10 bg-white/[0.025] p-4 transition hover:border-white/20 hover:bg-white/[0.04]">
                <div className="flex items-start gap-4">
                  <CompanyVisual ticker={row.ticker} name={row.name} sector={row.sector} size="md" />
                  <div className="min-w-0 flex-1">
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0"><Link href={`/ticker/${row.ticker}`} className="text-lg font-semibold text-white hover:text-emerald-300">{row.ticker}</Link><div className="truncate text-xs text-neutral-500">{row.name || row.ticker}</div></div>
                      <span className="rounded-lg border border-white/[0.07] px-2 py-1 text-[10px] text-neutral-500">{row.universe?.replace("S&P ", "") || "US"}</span>
                    </div>
                    <div className="mt-3 text-xs text-neutral-400">{row.sector || "Unknown"}</div>
                    <div className="mt-0.5 truncate text-[11px] text-neutral-600">{row.industry || "—"}</div>
                  </div>
                </div>
                <div className="mt-4 grid grid-cols-4 gap-2 border-t border-white/[0.06] pt-3 text-right">
                  <div><div className="text-[9px] uppercase tracking-wider text-neutral-700">Price</div><div className={`mt-1 font-mono text-xs ${tone(ret)}`}>{finite(row.latest_price)?.toFixed(2) ?? "—"}</div></div>
                  <div><div className="text-[9px] uppercase tracking-wider text-neutral-700">1D</div><div className={`mt-1 font-mono text-xs ${tone(ret)}`}>{pct(ret)}</div></div>
                  <div><div className="text-[9px] uppercase tracking-wider text-neutral-700">Sent.</div><div className={`mt-1 font-mono text-xs ${tone(sent)}`}>{sentiment(sent)}</div></div>
                  <div><div className="text-[9px] uppercase tracking-wider text-neutral-700">News</div><div className="mt-1 font-mono text-xs text-neutral-400">{finite(row.news_count)?.toFixed(0) ?? "—"}</div></div>
                </div>
                <div className="mt-3 flex gap-4 text-xs"><Link href={`/ticker/${row.ticker}`} className="text-emerald-300 hover:underline">Company</Link><Link href={`/earnings/${row.ticker}`} className="text-sky-300 hover:underline">Earnings</Link></div>
              </article>
            );
          })}
        </div>
      ) : (
        <div className="table-shell overflow-x-auto">
          <table className="w-full min-w-[1120px] text-sm">
            <thead className="border-b border-white/10 bg-white/[0.025] text-left text-[11px] uppercase tracking-[0.1em] text-neutral-600"><tr><th className="px-4 py-3">Company</th><th className="px-4 py-3">Universe</th><th className="px-4 py-3">Sector</th><th className="px-4 py-3 text-right">Price</th><th className="px-4 py-3 text-right">1D</th><th className="px-4 py-3 text-right">Sentiment</th><th className="px-4 py-3 text-right">News</th><th className="px-4 py-3">Explore</th></tr></thead>
            <tbody>{visible.map((row) => { const ret = finite(row.return_1d); const sent = finite(row.sentiment); return <tr key={row.ticker} className="border-b border-white/[0.06] last:border-0 hover:bg-white/[0.025]"><td className="px-4 py-3"><div className="flex items-center gap-3"><CompanyVisual ticker={row.ticker} name={row.name} sector={row.sector} size="sm" /><div className="min-w-0"><div className="font-semibold text-white">{row.ticker}</div><div className="max-w-[220px] truncate text-xs text-neutral-500">{row.name || row.ticker}</div></div></div></td><td className="px-4 py-3 text-xs text-neutral-400">{row.universe || "Other"}</td><td className="px-4 py-3"><div className="text-xs text-neutral-300">{row.sector || "Unknown"}</div><div className="max-w-[220px] truncate text-[11px] text-neutral-600">{row.industry || "—"}</div></td><td className={`px-4 py-3 text-right font-mono ${tone(ret)}`}>{finite(row.latest_price)?.toFixed(2) ?? "—"}</td><td className={`px-4 py-3 text-right font-mono ${tone(ret)}`}>{pct(ret)}</td><td className={`px-4 py-3 text-right font-mono ${tone(sent)}`}>{sentiment(sent)}</td><td className="px-4 py-3 text-right font-mono text-neutral-400">{finite(row.news_count)?.toFixed(0) ?? "—"}</td><td className="px-4 py-3"><div className="flex gap-3"><Link href={`/ticker/${row.ticker}`} className="text-xs text-emerald-300 hover:underline">Company</Link><Link href={`/earnings/${row.ticker}`} className="text-xs text-sky-300 hover:underline">Earnings</Link></div></td></tr>; })}</tbody>
          </table>
        </div>
      )}

      <div className="flex items-center justify-center gap-3 pt-1">
        <button type="button" disabled={safePage <= 1} onClick={() => setPage((value) => Math.max(1, value - 1))} className="rounded-xl border border-white/10 px-4 py-2 text-xs text-neutral-300 transition hover:bg-white/[0.05] disabled:cursor-not-allowed disabled:opacity-30">Previous</button>
        <span className="text-xs text-neutral-600">{safePage} / {pageCount}</span>
        <button type="button" disabled={safePage >= pageCount} onClick={() => setPage((value) => Math.min(pageCount, value + 1))} className="rounded-xl border border-white/10 px-4 py-2 text-xs text-neutral-300 transition hover:bg-white/[0.05] disabled:cursor-not-allowed disabled:opacity-30">Next</button>
      </div>
    </div>
  );
}
