// apps/web/app/research/[slug]/page.tsx
import Link from "next/link";
import { loadResearchIndex, loadResearchStudy } from "../../../lib/research";
import ResearchStudyClient from "../ResearchStudyClient";

export async function generateStaticParams() {
  const idx = await loadResearchIndex();
  return idx.map((x) => ({ slug: x.slug }));
}

export default async function ResearchStudyPage({
  params,
}: {
  params: Promise<{ slug: string }>;
}) {
  const { slug } = await params;
  const study = await loadResearchStudy(slug);

  return (
    <main className="max-w-6xl mx-auto p-6 space-y-6">
      <div className="flex items-center justify-between gap-4">
        <div className="space-y-1">
          <Link href="/research" className="text-sm text-zinc-600 hover:underline">
            ← Back to Research
          </Link>
          <h1 className="text-3xl font-bold">{study.title}</h1>
          <p className="text-zinc-600">{study.summary}</p>
          <div className="text-xs text-zinc-500">Updated: {study.updated_at}</div>
        </div>
      </div>

      <ResearchStudyClient study={study} />

      {study.notes?.length ? (
        <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
          <h2 className="text-lg font-semibold">Notes</h2>
          <ul className="list-disc pl-5 text-sm text-zinc-700 space-y-1">
            {study.notes.map((n, i) => (
              <li key={i}>{n}</li>
            ))}
          </ul>
        </section>
      ) : null}
    </main>
  );
}
