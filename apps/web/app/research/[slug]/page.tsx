import Link from "next/link";
import { loadResearchIndex, loadResearchStudy } from "../../../lib/research";
import ResearchStudyClient from "../ResearchStudyClient";

export async function generateStaticParams() {
  const idx = await loadResearchIndex();
  const slugs = idx.map((x) => String(x?.slug ?? "").trim()).filter((slug) => slug.length > 0);
  return slugs.length > 0 ? slugs.map((slug) => ({ slug })) : [{ slug: "__build_check__" }];
}

export default async function ResearchStudyPage({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params;
  const study = await loadResearchStudy(slug);
  return (
    <main className="research-dark mx-auto max-w-6xl space-y-8 py-2">
      <header className="border-b border-white/10 pb-7">
        <Link href="/research" className="text-xs font-medium uppercase tracking-[0.12em] text-neutral-500 transition hover:text-emerald-300">← Research library</Link>
        <div className="mt-5 max-w-4xl">
          <div className="eyebrow">Empirical study</div>
          <h1 className="mt-2 text-3xl font-semibold leading-tight tracking-[-0.03em] text-white md:text-5xl">{study.title}</h1>
          <p className="mt-4 max-w-3xl text-[15px] leading-7 text-neutral-400">{study.summary}</p>
          <div className="mt-4 flex flex-wrap gap-2 text-xs text-neutral-600">
            {study.category ? <span className="pill">{study.category}</span> : null}
            {study.status ? <span className="pill">{String(study.status).toUpperCase()}</span> : null}
            <span className="pill">Updated · {study.updated_at}</span>
          </div>
        </div>
      </header>

      <ResearchStudyClient study={study} />

      {study.notes?.length ? (
        <section className="rounded-2xl border border-white/10 bg-white/[0.03] p-6">
          <h2 className="text-xl font-semibold tracking-tight text-white">Notes</h2>
          <ul className="mt-4 list-disc space-y-2 pl-5 text-sm leading-6 text-neutral-400">
            {study.notes.map((n, i) => <li key={i}>{n}</li>)}
          </ul>
        </section>
      ) : null}
    </main>
  );
}
