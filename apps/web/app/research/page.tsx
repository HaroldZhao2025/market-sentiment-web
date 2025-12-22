// apps/web/app/research/page.tsx
import Link from "next/link";
import { loadResearchIndex } from "../../lib/research";

const Badge = ({ children }: { children: React.ReactNode }) => (
  <span className="text-xs px-2 py-1 rounded-full border border-zinc-200 bg-white">
    {children}
  </span>
);

export default async function ResearchPage() {
  const items = await loadResearchIndex();

  return (
    <main className="max-w-6xl mx-auto p-6 space-y-6">
      <div className="flex items-end justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold">Research</h1>
          <p className="text-zinc-600 mt-1">
            Small empirical studies built on the same dataset powering Sentiment Live.
          </p>
        </div>
      </div>

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

            <div className="mt-3 flex flex-wrap gap-2">
              {(it.tags ?? []).slice(0, 6).map((t) => (
                <span key={t} className="text-xs text-zinc-600 px-2 py-1 rounded-full bg-zinc-100">
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
      </section>
    </main>
  );
}
