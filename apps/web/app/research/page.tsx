// apps/web/app/research/page.tsx
import Link from "next/link";
import { loadResearchIndex, loadResearchOverviewFull } from "../../lib/research";

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

export default async function ResearchPage() {
  const [itemsRaw, sectionsRaw] = await Promise.all([
    loadResearchIndex(),
    loadResearchOverviewFull(), // ✅ correct export name
  ]);

  const items = (itemsRaw ?? []) as IndexItem[];
  const sections = (sectionsRaw ?? []) as Section[];

  const bySlug = new Map(items.map((x) => [x.slug, x]));
  const hasSections = sections.length > 0;

  return (
    <main className="max-w-6xl mx-auto p-6 space-y-8">
      <div className="flex items-end justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold">Research</h1>
          <p className="text-zinc-600 mt-1">
            Live empirical notes built on the same dataset powering Sentiment Live.
          </p>
          <div className="text-xs text-zinc-500 mt-2">
            Descriptive analytics only — not investment advice. Results may change as data updates.
          </div>
        </div>

        <Link href="/" className="text-sm underline text-zinc-700 hover:text-zinc-900">
          Home →
        </Link>
      </div>

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
      ) : hasSections ? (
        <div className="space-y-10">
          {sections.map((sec) => {
            const secItems = sec.slugs.map((s) => bySlug.get(s)).filter(Boolean) as IndexItem[];
            if (!secItems.length) return null;

            return (
              <section key={sec.id} className="space-y-4">
                <div className="space-y-2">
                  <h2 className="text-xl font-semibold">{sec.title}</h2>
                  {sec.description ? (
                    <div className="text-sm text-zinc-600">{sec.description}</div>
                  ) : null}
                  {sec.conclusions?.length ? (
                    <ul className="list-disc pl-5 text-sm text-zinc-700 space-y-1">
                      {sec.conclusions.slice(0, 3).map((c, i) => (
                        <li key={i}>{c}</li>
                      ))}
                    </ul>
                  ) : null}
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {secItems.map((it) => (
                    <Link
                      key={it.slug}
                      href={`/research/${it.slug}`}
                      className="rounded-2xl border border-zinc-200 bg-white p-5 hover:shadow-sm transition"
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div className="space-y-1">
                          <div className="text-lg font-semibold leading-snug">{it.title}</div>
                          <div className="text-sm text-zinc-600">{it.summary}</div>
                          {it.highlight ? (
                            <div className="text-xs text-zinc-500 mt-2 line-clamp-2">
                              <span className="font-semibold text-zinc-600">Key finding:</span>{" "}
                              {it.highlight}
                            </div>
                          ) : null}
                        </div>
                        <div className="text-right space-y-2 shrink-0">
                          <Badge>{(it.status ?? "draft").toUpperCase()}</Badge>
                          <div className="text-xs text-zinc-500">{it.updated_at}</div>
                        </div>
                      </div>

                      <div className="mt-3 flex flex-wrap gap-2">
                        {(it.tags ?? []).slice(0, 6).map((t) => (
                          <span
                            key={t}
                            className="text-xs text-zinc-600 px-2 py-1 rounded-full bg-zinc-100"
                          >
                            {t}
                          </span>
                        ))}
                      </div>

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
                  ))}
                </div>
              </section>
            );
          })}
        </div>
      ) : (
        // fallback if overview isn't present
        <section className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {items.map((it) => (
            <Link
              key={it.slug}
              href={`/research/${it.slug}`}
              className="rounded-2xl border border-zinc-200 bg-white p-5 hover:shadow-sm transition"
            >
              <div className="flex items-start justify-between gap-3">
                <div className="space-y-1">
                  <div className="text-lg font-semibold leading-snug">{it.title}</div>
                  <div className="text-sm text-zinc-600">{it.summary}</div>
                </div>
                <div className="text-right space-y-2 shrink-0">
                  <Badge>{(it.status ?? "draft").toUpperCase()}</Badge>
                  <div className="text-xs text-zinc-500">{it.updated_at}</div>
                </div>
              </div>
            </Link>
          ))}
        </section>
      )}
    </main>
  );
}
