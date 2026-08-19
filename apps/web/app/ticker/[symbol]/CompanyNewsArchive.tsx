"use client";

import { useMemo, useState } from "react";
import type { NewsItem } from "./TickerClientV2";

function finite(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}
function scoreClass(value: number | null) { return value == null ? "text-neutral-500" : value > 0 ? "text-emerald-300" : value < 0 ? "text-rose-300" : "text-neutral-300"; }
function sourceOf(item: NewsItem) { if (item.source) return item.source; if (item.provider) return item.provider; try { return new URL(item.url).host.replace(/^www\./, ""); } catch { return ""; } }
function dateOnly(value: string) { const parsed = new Date(value); return Number.isNaN(parsed.getTime()) ? String(value).slice(0, 10) : parsed.toISOString().slice(0, 10); }

export default function CompanyNewsArchive({ news }: { news: NewsItem[] }) {
  const [limit, setLimit] = useState(25);
  const [query, setQuery] = useState("");
  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return news;
    return news.filter((item) => `${item.title} ${sourceOf(item)} ${item.summary || ""}`.toLowerCase().includes(q));
  }, [news, query]);
  const visible = filtered.slice(0, limit);

  if (!news.length) return <section className="card p-5 text-sm text-neutral-500">No free-public company news has been retained yet.</section>;

  return <section className="space-y-3">
    <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between"><div><div className="eyebrow">News archive</div><h2 className="section-title mt-1">Retained headlines</h2><p className="section-copy">Free-public company news, newest first.</p></div><input value={query} onChange={(event) => { setQuery(event.target.value); setLimit(25); }} placeholder="Search headlines" className="w-full rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-xs text-white outline-none placeholder:text-neutral-600 md:w-64" /></div>
    <div className="table-shell overflow-x-auto"><table className="w-full min-w-[760px] text-sm"><thead className="border-b border-white/10 bg-white/[0.025] text-left text-[11px] uppercase tracking-[0.12em] text-neutral-600"><tr><th className="px-4 py-3">Date</th><th className="px-4 py-3">Headline</th><th className="px-4 py-3">Source</th><th className="px-4 py-3 text-right">Score</th></tr></thead><tbody>{visible.map((item, index) => { const score = finite(item.s); return <tr key={`${item.url}-${index}`} className="border-b border-white/[0.06] last:border-0 hover:bg-white/[0.025]"><td className="whitespace-nowrap px-4 py-3 font-mono text-xs text-neutral-600">{dateOnly(item.ts)}</td><td className="max-w-2xl px-4 py-3"><a href={item.url} target="_blank" rel="noreferrer" className="font-medium leading-6 text-neutral-200 hover:text-white hover:underline">{item.title}</a></td><td className="px-4 py-3 text-xs text-neutral-500">{sourceOf(item)}</td><td className={`px-4 py-3 text-right font-mono text-xs ${scoreClass(score)}`}>{score == null ? "—" : `${score > 0 ? "+" : ""}${score.toFixed(4)}`}</td></tr>; })}</tbody></table></div>
    <div className="flex items-center justify-between text-xs text-neutral-600"><span>{visible.length} of {filtered.length} retained headlines</span>{limit < filtered.length ? <button type="button" onClick={() => setLimit((value) => Math.min(filtered.length, value + 25))} className="rounded-xl border border-white/10 px-4 py-2 text-neutral-300 transition hover:bg-white/[0.05]">Show 25 more</button> : null}</div>
  </section>;
}