"use client";

import { useRouter } from "next/navigation";
import { useEffect, useMemo, useState } from "react";
import CompanyVisual from "../../components/CompanyVisual";

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
type View = "cards" | "table";

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
  const router = useRouter();
  const [query, setQuery] = useState("");
  const [universe, setUniverse] = useState("All");
  const [sector, setSector] = useState("All");
  const [sort, setSort] = useState<"ticker" | "return" | "sentiment" | "news">("ticker");
  const [view, setView] = useState<View>("cards");
  const [page, setPage] = useState(1);
  const pageSize = 60;

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

  useEffect(() => setPage(1), [query, universe, sector, sort]);
  const pages = Math.max(1, Math.ceil(filtered.length / pageSize));
  const visible = filtered.slice((page - 1) * pageSize, page * pageSize);
  const open = (ticker: string) => router.push(`/ticker/${ticker}`);
  const keyOpen = (event: React.KeyboardEvent, ticker: string) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      open(ticker);
    }
  };

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

      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="text-xs text-neutral-600">{filtered.length} of {rows.length} companies · page {page}/{pages}{generatedAt ? ` · ${generatedAt.slice(0, 10)}` : ""}</div>
        <div className="flex rounded-xl border border-white/10 bg-black/30 p-1">
          {(["cards", "table"] as const).map((value) => <button key={value} type="button" onClick={() => setView(value)} className={`rounded-lg px-3 py-1.5 text-xs ${view === value ? "bg-white/10 text-white" : "text-neutral-500"}`}>{value === "cards" ? "Cards" : "Table"}</button>)}
        </div>
      </div>

      {view === "cards" ? (
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
          {visible.map((row) => {
            const ret = finite(row.return_1d);
            const sent = finite(row.sentiment);
            return (
              <article key={row.ticker} role="link" tabIndex={0} onClick={() => open(row.ticker)} onKeyDown={(event) => keyOpen(event, row.ticker)} className="group cursor-pointer rounded-2xl border border-white/10 bg-white/[0.025] p-4 transition hover:-translate-y-0.5 hover:border-emerald-400/25 hover:bg-white/[0.045] focus:outline-none focus:ring-2 focus:ring-emerald-400/30">
                <div className="flex items-start gap-4">
                  <CompanyVisual ticker={row.ticker} name={row.name} sector={row.sector} size="md" />
                  <div className="min-w-0 flex-1">
                    <div className="flex items-start justify-between gap-3"><div><div className="font-semibold text-white">{row.ticker}</div><div className="mt-0.5 truncate text-sm text-neutral-400">{row.name || "Company name unavailable"}</div></div><span className="text-neutral-700 transition group-hover:translate-x-0.5 group-hover:text-emerald-300">→</span></div>
                    <div className="mt-2 truncate text-xs text-neutral-600">{row.sector || "Unknown"} · {row.industry || "—"}</div>
                  </div>
                </div>
                <div className="mt-4 grid grid-cols-4 gap-2 border-t border-white/[0.06] pt-3 text-right">
                  <div><div className="text-[10px] uppercase tracking-wider text-neutral-700">Price</div><div className="mt-1 font-mono text-xs text-neutral-300">{finite(row.latest_price)?.toFixed(2) ?? "—"}</div></div>
                  <div><div className="text-[10px] uppercase tracking-wider text-neutral-700">1D</div><div className={`mt-1 font-mono text-xs ${tone(ret)}`}>{pct(ret)}</div></div>
                  <div><div className="text-[10px] uppercase tracking-wider text-neutral-700">Sent.</div><div className={`mt-1 font-mono text-xs ${tone(sent)}`}>{sent == null ? "—" : `${sent > 0 ? "+" : ""}${sent.toFixed(3)}`}</div></div>
                  <div><div className="text-[10px] uppercase tracking-wider text-neutral-700">News</div><div className="mt-1 font-mono text-xs text-neutral-400">{finite(row.news_count)?.toFixed(0) ?? "—"}</div></div>
                </div>
              </article>
            );
          })}
        </div>
      ) : (
        <div className="table-shell overflow-x-auto">
          <table className="w-full min-w-[1020px] text-sm">
            <thead className="border-b border-white/10 bg-white/[0.025] text-left text-[11px] uppercase tracking-[0.1em] text-neutral-600"><tr><th className="px-4 py-3">Company</th><th className="px-4 py-3">Universe</th><th className="px-4 py-3">Sector / industry</th><th className="px-4 py-3 text-right">Price</th><th className="px-4 py-3 text-right">1D</th><th className="px-4 py-3 text-right">Sentiment</th><th className="px-4 py-3 text-right">News</th></tr></thead>
            <tbody>{visible.map((row) => { const ret = finite(row.return_1d); const sent = finite(row.sentiment); return <tr key={row.ticker} role="link" tabIndex={0} onClick={() => open(row.ticker)} onKeyDown={(event) => keyOpen(event, row.ticker)} className="cursor-pointer border-b border-white/[0.06] last:border-0 hover:bg-white/[0.04] focus:bg-white/[0.04] focus:outline-none"><td className="px-4 py-3"><div className="flex items-center gap-3"><CompanyVisual ticker={row.ticker} name={row.name} sector={row.sector} size="sm" /><div><div className="font-semibold text-white">{row.ticker}</div><div className="max-w-[230px] truncate text-xs text-neutral-500">{row.name || "Company name unavailable"}</div></div></div></td><td className="px-4 py-3 text-xs text-neutral-400">{row.universe || "Other"}</td><td className="px-4 py-3"><div className="text-xs text-neutral-300">{row.sector || "Unknown"}</div><div className="max-w-[240px] truncate text-[11px] text-neutral-600">{row.industry || "—"}</div></td><td className="px-4 py-3 text-right font-mono text-neutral-300">{finite(row.latest_price)?.toFixed(2) ?? "—"}</td><td className={`px-4 py-3 text-right font-mono ${tone(ret)}`}>{pct(ret)}</td><td className={`px-4 py-3 text-right font-mono ${tone(sent)}`}>{sent == null ? "—" : `${sent > 0 ? "+" : ""}${sent.toFixed(3)}`}</td><td className="px-4 py-3 text-right font-mono text-neutral-400">{finite(row.news_count)?.toFixed(0) ?? "—"}</td></tr>; })}</tbody>
          </table>
        </div>
      )}

      {pages > 1 ? <div className="flex items-center justify-center gap-3 pt-2"><button type="button" disabled={page <= 1} onClick={() => setPage((value) => Math.max(1, value - 1))} className="rounded-xl border border-white/10 px-4 py-2 text-xs text-neutral-400 disabled:opacity-30">Previous</button><span className="text-xs text-neutral-600">{page} / {pages}</span><button type="button" disabled={page >= pages} onClick={() => setPage((value) => Math.min(pages, value + 1))} className="rounded-xl border border-white/10 px-4 py-2 text-xs text-neutral-400 disabled:opacity-30">Next</button></div> : null}
    </div>
  );
}
