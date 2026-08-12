import Link from "next/link";
import type { ReactNode } from "react";
import { loadResearchIndex, loadResearchOverview } from "../../lib/research";
import ResearchIndexClient from "./ResearchIndexClient";

type IndexItem = {
  slug: string;
  title: string;
  summary: string;
  updated_at: string;
  status?: string;
  tags?: string[];
  key_stats?: { label: string; value: string }[];
  highlight?: string;
  category?: string;
};

type Overview = {
  meta?: {
    updated_at?: string;
    n_studies?: number;
    n_tickers?: number;
    n_obs_panel?: number;
    date_range?: [string, string];
  };
  sections?: {
    id: string;
    title: string;
    description?: string;
    conclusions?: string[];
    slugs: string[];
  }[];
};

const Badge = ({ children }: { children: ReactNode }) => (
  <span className="rounded-full border border-emerald-400/20 bg-emerald-400/10 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.14em] text-emerald-300">{children}</span>
);

const StatPill = ({ label, value }: { label: string; value: string }) => (
  <div className="rounded-xl border border-white/10 bg-black/20 px-3 py-3">
    <div className="text-[10px] font-semibold uppercase tracking-[0.12em] text-neutral-600">{label}</div>
    <div className="mt-1 text-sm font-semibold text-neutral-200">{value}</div>
  </div>
);

function deriveSections(items: IndexItem[]) {
  const byCat = new Map<string, IndexItem[]>();
  for (const it of items) {
    const cat = it.category?.trim() || "Other";
    if (!byCat.has(cat)) byCat.set(cat, []);
    byCat.get(cat)!.push(it);
  }
  return Array.from(byCat.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([cat, arr]) => ({
      id: cat.toLowerCase().replace(/\s+/g, "-"),
      title: cat,
      description: "Empirical results generated from the current market-intelligence dataset.",
      conclusions: arr.map((x) => x.highlight).filter(Boolean).slice(0, 3) as string[],
      slugs: arr.slice().sort((a, b) => (b.updated_at || "").localeCompare(a.updated_at || "")).map((x) => x.slug),
    }));
}

export default async function ResearchPage() {
  const [itemsRaw, overviewRaw] = await Promise.all([loadResearchIndex(), loadResearchOverview()]);
  const items = (itemsRaw ?? []) as IndexItem[];
  const overview = (overviewRaw ?? { sections: [] }) as Overview;
  const sections = overview.sections?.length ? overview.sections : deriveSections(items);
  const meta = overview.meta ?? {};

  return (
    <main className="research-dark space-y-10">
      <section className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <div className="eyebrow">Research library</div>
          <h1 className="mt-2 max-w-4xl text-4xl font-semibold tracking-[-0.035em] text-white md:text-5xl">Empirical evidence behind the signals.</h1>
          <p className="mt-4 max-w-3xl text-[15px] leading-7 text-neutral-400">Reproducible studies built from the same generated artifacts that power the market, ticker, portfolio, and intelligence-engine views.</p>
          <p className="mt-2 text-xs leading-5 text-neutral-600">Descriptive research only — not investment advice.</p>
        </div>
        <div className="flex gap-2"><Link href="/lab" className="pill">Interactive Lab →</Link><Link href="/methodology" className="pill">Methodology →</Link><Link href="/data" className="pill">Data →</Link></div>
      </section>

      {(meta.n_studies || meta.date_range || meta.n_tickers || meta.n_obs_panel) ? (
        <section className="ambient-panel p-5 md:p-6">
          <div className="flex items-start justify-between gap-4">
            <div><div className="text-xl font-semibold tracking-tight text-white">Dataset snapshot</div><div className="mt-1 text-sm leading-6 text-neutral-500">Latest generated research universe and panel coverage.</div></div>
            <Badge>Live artifacts</Badge>
          </div>
          <div className="mt-5 grid grid-cols-2 gap-3 md:grid-cols-4">
            <StatPill label="Updated" value={meta.updated_at ?? "—"} />
            <StatPill label="Date range" value={Array.isArray(meta.date_range) ? `${meta.date_range[0]} → ${meta.date_range[1]}` : "—"} />
            <StatPill label="Studies" value={meta.n_studies?.toString?.() ?? "—"} />
            <StatPill label="Tickers" value={meta.n_tickers?.toString?.() ?? "—"} />
            <StatPill label="Panel observations" value={meta.n_obs_panel?.toLocaleString?.() ?? "—"} />
            <StatPill label="Frequency" value={Array.isArray(meta.date_range) ? "Daily" : "—"} />
            <StatPill label="Scope" value="S&P 500 snapshot" />
            <StatPill label="Output" value="Reproducible JSON" />
          </div>
        </section>
      ) : null}

      {items.length === 0 ? (
        <section className="card p-6"><div className="text-lg font-semibold text-white">No research artifacts yet</div><p className="mt-2 text-sm text-neutral-500">Research outputs were not generated for this deployment.</p></section>
      ) : <ResearchIndexClient items={items} sections={sections as any} />}
    </main>
  );
}
