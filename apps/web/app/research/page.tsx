// apps/web/app/research/page.tsx
import Link from "next/link";
import type { ReactNode } from "react";
import { loadResearchIndex, loadResearchOverview } from "../../lib/research";

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
    overview.sections && overview.sections.length
      ? overview.sections
      : deriveSections(items);

  const meta = overview.meta ?? {};
  const bySlug = new Map(items.map((x) => [x.slug, x]));

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
                Current research artifacts are generated from your latest scheduled pipeline output.
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
            <StatPill label="Style" value="Reproducible JSON" />
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
        <div className="space-y-10">
          {sections.map((sec) => {
            const secItems = sec.slugs
              .map((s) => bySlug.get(s))
              .filter(Boolean) as IndexItem[];

            if (!secItems.length) return null;

            return (
              <section key={sec.id} className="space-y-4">
                <div className="space-y-2">
                  <div className="flex items-center justify-between gap-3">
                    <h2 className="text-xl font-semibold">{sec.title}</h2>
                    <div className="text-xs text-zinc-500">{secItems.length} studies</div>
                  </div>

                  {sec.description ? (
                    <div className="text-sm text-zinc-600">{sec.description}</div>
                  ) : null}

                  {sec.conclusions?.length ? (
                    <div className="rounded-2xl border border-zinc-200 bg-white p-4">
                      <div className="text-sm font-semibold">Section takeaways</div>
                      <ul className="mt-2 list-disc pl-5 text-sm text-zinc-700 space-y-1">
                        {sec.conclusions.slice(0, 3).map((c, i) => (
                          <li key={i}>{c}</li>
                        ))}
                      </ul>
                    </div>
                  ) : null}
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {secItems.map((it) => (
                    <Link
                      key={it.slug}
                      href={`/research/${it.slug}`}
                      className="group rounded-2xl border border-zinc-200 bg-white p-5 hover:shadow-sm transition"
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div className="space-y-1">
                          <div className="text-lg font-semibold leading-snug group-hover:underline">
                            {it.title}
                          </div>
                          <div className="text-sm text-zinc-600">{it.summary}</div>

                          {it.highlight ? (
                            <div className="text-xs text-zinc-600 mt-2 line-clamp-2">
                              <span className="font-semibold text-zinc-700">Key finding:</span>{" "}
                              {it.highlight}
                            </div>
                          ) : null}
                        </div>

                        <div className="text-right space-y-2 shrink-0">
                          <Badge>{(it.status ?? "draft").toUpperCase()}</Badge>
                          <div className="text-xs text-zinc-500">{it.updated_at}</div>
                        </div>
                      </div>

                      {(it.tags ?? []).length ? (
                        <div className="mt-3 flex flex-wrap gap-2">
                          {(it.tags ?? []).slice(0, 10).map((t) => (
                            <span
                              key={t}
                              className="text-[11px] text-zinc-700 px-2 py-1 rounded-full bg-zinc-100"
                            >
                              {t}
                            </span>
                          ))}
                        </div>
                      ) : null}

                      {it.key_stats?.length ? (
                        <div className="mt-4 grid grid-cols-2 gap-2">
                          {it.key_stats.slice(0, 6).map((s) => (
                            <div
                              key={s.label}
                              className="rounded-xl bg-zinc-50 p-3 border border-zinc-100"
                            >
                              <div className="text-[11px] text-zinc-500">{s.label}</div>
                              <div className="text-sm font-semibold text-zinc-900">{s.value}</div>
                            </div>
                          ))}
                        </div>
                      ) : null}
                    </Link>
                  ))}
                </div>
              </section>
            );
          })}
        </div>
      )}
    </main>
  );
}
