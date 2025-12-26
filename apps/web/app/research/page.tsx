// apps/web/app/research/page.tsx
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
  <span className="text-[11px] px-2 py-1 rounded-full border border-zinc-200 bg-white">
    {children}
  </span>
);

const StatPill = ({ label, value }: { label: string; value: string }) => (
  <div className="rounded-xl border border-zinc-200 bg-white px-3 py-2">
    <div className="text-[11px] text-zinc-500">{label}</div>
    <div className="text-sm font-semibold text-zinc-900">{value}</div>
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
      description: "Empirical notes generated from the live Sentiment Live snapshot.",
      conclusions: arr
        .map((x) => x.highlight)
        .filter(Boolean)
        .slice(0, 3) as string[],
      slugs: arr
        .slice()
        .sort((a, b) => (b.updated_at || "").localeCompare(a.updated_at || ""))
        .map((x) => x.slug),
    }));
}

export default async function ResearchPage() {
  const [itemsRaw, overviewRaw] = await Promise.all([loadResearchIndex(), loadResearchOverview()]);
  const items = (itemsRaw ?? []) as IndexItem[];
  const overview = (overviewRaw ?? { sections: [] }) as Overview;

  const sections =
    overview.sections && overview.sections.length ? overview.sections : deriveSections(items);

  const meta = overview.meta ?? {};

  return (
    <main className="max-w-6xl mx-auto p-6 space-y-8">
      {/* Header */}
      <div className="flex items-end justify-between gap-4">
        <div className="space-y-2">
          <h1 className="text-3xl font-bold">Research</h1>
          <p className="text-zinc-600">
            Live empirical notes built on the same dataset powering Sentiment Live.
          </p>
          <p className="text-xs text-zinc-500">
            Descriptive analytics only — not investment advice. Results may change as data updates.
          </p>
        </div>

        <Link href="/" className="text-sm underline text-zinc-700 hover:text-zinc-900">
          Home →
        </Link>
      </div>

      {/* Dataset snapshot */}
      {(meta.n_studies || meta.date_range || meta.n_tickers || meta.n_obs_panel) ? (
        <section className="rounded-2xl border border-zinc-200 bg-white p-5">
          <div className="flex items-start justify-between gap-4">
            <div className="space-y-1">
              <div className="text-lg font-semibold">Dataset snapshot</div>
              <div className="text-sm text-zinc-600">
                Research artifacts are generated from your latest scheduled pipeline output.
              </div>
            </div>
            <Badge>LIVE</Badge>
          </div>

          <div className="mt-4 grid grid-cols-2 md:grid-cols-4 gap-3">
            <StatPill label="Updated" value={meta.updated_at ?? "—"} />
            <StatPill
              label="Date range"
              value={
                Array.isArray(meta.date_range)
                  ? `${meta.date_range[0]} .. ${meta.date_range[1]}`
                  : "—"
              }
            />
            <StatPill label="Studies" value={meta.n_studies?.toString?.() ?? "—"} />
            <StatPill label="Tickers" value={meta.n_tickers?.toString?.() ?? "—"} />
            <StatPill label="Obs (panel)" value={meta.n_obs_panel?.toString?.() ?? "—"} />
            <StatPill label="Frequency" value={Array.isArray(meta.date_range) ? "Daily" : "—"} />
            <StatPill label="Scope" value="S&P 500 snapshot" />
            <StatPill label="Outputs" value="Reproducible JSON" />
          </div>
        </section>
      ) : null}

      {/* Empty state */}
      {items.length === 0 ? (
        <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
          <div className="text-lg font-semibold">No research artifacts yet</div>
          <p className="text-sm text-zinc-600">
            The research builder has not generated{" "}
            <code className="px-1 py-0.5 rounded bg-zinc-100">
              apps/web/public/research/index.json
            </code>{" "}
            for this deployment.
          </p>
          <pre className="text-xs overflow-auto rounded-xl bg-zinc-50 border border-zinc-100 p-4">
python src/market_sentiment/cli/build_research.py --data-root data --out-dir apps/web/public/research
          </pre>
        </section>
      ) : (
        <ResearchIndexClient items={items} sections={sections as any} />
      )}
    </main>
  );
}
