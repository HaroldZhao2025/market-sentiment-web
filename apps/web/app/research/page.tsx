// apps/web/app/research/page.tsx
import Link from "next/link";
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

type Section = {
  id: string;
  title: string;
  description?: string;
  conclusions?: string[];
  slugs: string[];
};

const Badge = ({ children }: { children: React.ReactNode }) => (
  <span className="text-xs px-2 py-1 rounded-full border border-zinc-200 bg-white">
    {children}
  </span>
);

const Stat = ({ label, value }: { label: string; value: string }) => (
  <div className="rounded-2xl border border-zinc-200 bg-white p-4">
    <div className="text-xs text-zinc-500">{label}</div>
    <div className="text-lg font-semibold mt-1">{value}</div>
  </div>
);

const Chip = ({ children }: { children: React.ReactNode }) => (
  <span className="text-xs text-zinc-700 px-2 py-1 rounded-full bg-zinc-100 border border-zinc-200">
    {children}
  </span>
);

function safeUpper(x?: string) {
  return (x ?? "draft").toUpperCase();
}

function pickLastUpdated(items: IndexItem[]) {
  const dates = items.map((x) => x.updated_at).filter(Boolean).sort();
  return dates.length ? dates[dates.length - 1] : "—";
}

function topTags(items: IndexItem[], k = 10) {
  const m = new Map<string, number>();
  for (const it of items) {
    for (const t of it.tags ?? []) m.set(t, (m.get(t) ?? 0) + 1);
  }
  return Array.from(m.entries())
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, k);
}

function sortByUpdatedDesc(items: IndexItem[]) {
  return [...items].sort((a, b) => (b.updated_at ?? "").localeCompare(a.updated_at ?? ""));
}

function StudyCard({ it }: { it: IndexItem }) {
  return (
    <Link
      key={it.slug}
      href={`/research/${it.slug}`}
      className="group rounded-2xl border border-zinc-200 bg-white p-5 hover:shadow-sm hover:border-zinc-300 transition"
    >
      <div className="flex items-start justify-between gap-4">
        <div className="space-y-1 min-w-0">
          <div className="flex items-center gap-2">
            <div className="text-lg font-semibold leading-snug truncate">{it.title}</div>
            {it.category ? (
              <span className="hidden md:inline text-xs text-zinc-500 border border-zinc-200 rounded-full px-2 py-0.5 bg-white">
                {it.category}
              </span>
            ) : null}
          </div>

          <div className="text-sm text-zinc-600 line-clamp-2">{it.summary}</div>

          {it.highlight ? (
            <div className="text-xs text-zinc-600 mt-2 line-clamp-2">
              <span className="font-semibold text-zinc-700">Key finding:</span>{" "}
              <span className="text-zinc-600">{it.highlight}</span>
            </div>
          ) : null}
        </div>

        <div className="text-right space-y-2 shrink-0">
          <Badge>{safeUpper(it.status)}</Badge>
          <div className="text-xs text-zinc-500">{it.updated_at}</div>
        </div>
      </div>

      {(it.tags ?? []).length ? (
        <div className="mt-3 flex flex-wrap gap-2">
          {(it.tags ?? []).slice(0, 7).map((t) => (
            <span
              key={t}
              className="text-xs text-zinc-700 px-2 py-1 rounded-full bg-zinc-50 border border-zinc-200 group-hover:border-zinc-300"
            >
              {t}
            </span>
          ))}
        </div>
      ) : null}

      {it.key_stats?.length ? (
        <div className="mt-4 grid grid-cols-2 gap-2">
          {it.key_stats.slice(0, 4).map((s) => (
            <div key={s.label} className="rounded-xl bg-zinc-50 p-3 border border-zinc-100">
              <div className="text-xs text-zinc-500">{s.label}</div>
              <div className="text-sm font-semibold">{s.value}</div>
            </div>
          ))}
        </div>
      ) : null}
    </Link>
  );
}

export default async function ResearchPage() {
  const [rawItems, rawSections] = await Promise.all([loadResearchIndex(), loadResearchOverview()]);

  const items = (rawItems ?? []) as IndexItem[];
  const sections = (rawSections ?? []) as Section[];

  const bySlug = new Map(items.map((x) => [x.slug, x]));
  const hasSections = sections.length > 0;

  const nStudies = items.length;
  const nLive = items.filter((x) => (x.status ?? "draft").toLowerCase() === "live").length;
  const nDraft = nStudies - nLive;
  const lastUpdated = pickLastUpdated(items);

  const tagTop = topTags(items, 10);
  const featured = sortByUpdatedDesc(items).slice(0, Math.min(3, items.length));

  return (
    <main className="max-w-6xl mx-auto p-6 space-y-8">
      {/* header */}
      <header className="space-y-3">
        <div className="flex items-end justify-between gap-4">
          <div>
            <h1 className="text-3xl font-bold">Research</h1>
            <p className="text-zinc-600 mt-1">
              Academic-style empirical notes built on the same dataset powering Sentiment Live.
            </p>
            <div className="text-xs text-zinc-500 mt-2">
              Descriptive analytics only — not investment advice. Results may change as data updates.
            </div>
          </div>

          <div className="flex items-center gap-3">
            <Link href="/" className="text-sm underline text-zinc-700 hover:text-zinc-900">
              Home →
            </Link>
            <a
              href="/research/index.json"
              className="text-sm underline text-zinc-700 hover:text-zinc-900"
            >
              index.json
            </a>
            <a
              href="/research/overview.json"
              className="text-sm underline text-zinc-700 hover:text-zinc-900"
            >
              overview.json
            </a>
          </div>
        </div>

        {/* quick stats */}
        <section className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <Stat label="Studies" value={String(nStudies)} />
          <Stat label="Live / Draft" value={`${nLive} / ${nDraft}`} />
          <Stat label="Last updated" value={lastUpdated} />
          <Stat label="Sections" value={String(sections.length)} />
        </section>

        {/* top tags */}
        {tagTop.length ? (
          <section className="rounded-2xl border border-zinc-200 bg-white p-4">
            <div className="text-sm font-semibold">Top tags</div>
            <div className="mt-2 flex flex-wrap gap-2">
              {tagTop.map(([t, c]) => (
                <Chip key={t}>
                  {t} <span className="text-zinc-500">({c})</span>
                </Chip>
              ))}
            </div>
          </section>
        ) : null}
      </header>

      {/* empty state */}
      {items.length === 0 ? (
        <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
          <div className="text-lg font-semibold">No research artifacts yet</div>
          <p className="text-sm text-zinc-600">
            The research builder has not generated{" "}
            <code className="px-1 py-0.5 rounded bg-zinc-100">apps/web/public/research/index.json</code>{" "}
            for this deployment.
          </p>
          <pre className="text-xs overflow-auto rounded-xl bg-zinc-50 border border-zinc-100 p-4">
python src/market_sentiment/cli/build_research.py --data-root data --out-dir apps/web/public/research
          </pre>
        </section>
      ) : (
        <div className="lg:grid lg:grid-cols-[260px_1fr] lg:gap-8 space-y-8 lg:space-y-0">
          {/* left nav / toc */}
          <aside className="space-y-4 lg:sticky lg:top-6 h-fit">
            <section className="rounded-2xl border border-zinc-200 bg-white p-4 space-y-3">
              <div className="text-sm font-semibold">Contents</div>
              <div className="text-sm">
                <a href="#featured" className="text-zinc-700 hover:underline">
                  Featured
                </a>
              </div>
              {hasSections ? (
                <div className="space-y-2">
                  {sections.map((s) => (
                    <div key={s.id} className="text-sm">
                      <a href={`#${s.id}`} className="text-zinc-700 hover:underline">
                        {s.title}
                      </a>
                      <div className="text-xs text-zinc-500">{s.slugs?.length ?? 0} studies</div>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="text-xs text-zinc-500">No overview sections available.</div>
              )}
            </section>

            <section className="rounded-2xl border border-zinc-200 bg-white p-4 space-y-2">
              <div className="text-sm font-semibold">Reproducibility</div>
              <div className="text-xs text-zinc-600">
                Studies are generated by the CLI and exported as static JSON.
              </div>
              <pre className="text-xs overflow-auto rounded-xl bg-zinc-50 border border-zinc-100 p-3">
python src/market_sentiment/cli/build_research.py --data-root data --out-dir apps/web/public/research
              </pre>
              <div className="text-xs text-zinc-500">
                Tip: run the builder <span className="font-semibold">before</span> Next.js export in your workflow.
              </div>
            </section>
          </aside>

          {/* main content */}
          <div className="space-y-10">
            {/* “academic” framing */}
            <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-3">
              <h2 className="text-lg font-semibold">Data & definitions</h2>
              <ul className="list-disc pl-5 text-sm text-zinc-700 space-y-1">
                <li>
                  <span className="font-semibold">Returns:</span> log returns computed from the price series in each
                  ticker JSON.
                </li>
                <li>
                  <span className="font-semibold">Sentiment:</span> <code className="px-1 py-0.5 rounded bg-zinc-100">score_mean</code>{" "}
                  from your pipeline (per ticker per day).
                </li>
                <li>
                  <span className="font-semibold">Inference:</span> Time-series HAC / ticker-clustered SE in panel FE.
                  Results are descriptive and can be sensitive to timing and sample construction.
                </li>
              </ul>
            </section>

            {/* featured */}
            <section id="featured" className="space-y-4">
              <div className="flex items-baseline justify-between">
                <h2 className="text-xl font-semibold">Featured (most recently updated)</h2>
                <div className="text-xs text-zinc-500">auto-selected by updated_at</div>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {featured.map((it) => (
                  <StudyCard key={it.slug} it={it} />
                ))}
              </div>
            </section>

            {/* sectioned rendering */}
            {hasSections ? (
              <div className="space-y-12">
                {sections.map((sec) => {
                  const secItems = sec.slugs.map((s) => bySlug.get(s)).filter(Boolean) as IndexItem[];
                  if (!secItems.length) return null;

                  return (
                    <section key={sec.id} id={sec.id} className="space-y-4 scroll-mt-24">
                      <div className="space-y-2">
                        <div className="flex items-baseline justify-between gap-3">
                          <h2 className="text-xl font-semibold">{sec.title}</h2>
                          <div className="text-xs text-zinc-500">{secItems.length} studies</div>
                        </div>
                        {sec.description ? (
                          <div className="text-sm text-zinc-600">{sec.description}</div>
                        ) : null}

                        {sec.conclusions?.length ? (
                          <div className="rounded-2xl border border-zinc-200 bg-white p-4">
                            <div className="text-sm font-semibold">Section conclusions</div>
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
                          <StudyCard key={it.slug} it={it} />
                        ))}
                      </div>
                    </section>
                  );
                })}
              </div>
            ) : (
              // fallback if overview.json isn't present
              <section className="space-y-4">
                <h2 className="text-xl font-semibold">All studies</h2>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {items.map((it) => (
                    <StudyCard key={it.slug} it={it} />
                  ))}
                </div>
              </section>
            )}
          </div>
        </div>
      )}
    </main>
  );
}
