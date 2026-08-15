import fs from "node:fs";
import path from "node:path";
import Link from "next/link";
import CompanyVisual from "../../components/CompanyVisual";
import { buildEventMemory } from "../../lib/intelligence";

export const dynamic = "force-static";
export const metadata = { title: "Events" };

type EventArticle = { date?: string; title?: string; url?: string; source?: string; sentiment?: number | null };
type EventInstance = {
  event_instance_id?: string;
  symbol?: string;
  name?: string;
  sector?: string;
  theme?: string;
  start_date?: string;
  end_date?: string;
  article_count?: number;
  source_count?: number;
  sentiment_mean?: number | null;
  sentiment_disagreement?: number | null;
  articles?: EventArticle[];
};

function loadPersistentEvents(): EventInstance[] {
  try {
    const file = path.join(process.cwd(), "public", "data", "v5", "event_instances.json");
    const parsed = JSON.parse(fs.readFileSync(file, "utf8"));
    return Array.isArray(parsed?.event_instances) ? parsed.event_instances : [];
  } catch {
    return [];
  }
}

function tone(value: number | null | undefined) {
  if (value == null) return "text-neutral-500";
  return value > 0 ? "text-emerald-300" : value < 0 ? "text-rose-300" : "text-neutral-300";
}

export default function EventsPage() {
  const instances = loadPersistentEvents().slice().sort((a, b) => String(b.end_date || b.start_date || "").localeCompare(String(a.end_date || a.start_date || "")));
  const fallback = buildEventMemory();
  const recent = instances.slice(0, 120);
  const companies = new Set(recent.map((item) => item.symbol).filter(Boolean)).size;
  const themes = new Set(recent.map((item) => item.theme).filter(Boolean)).size;

  return (
    <main className="space-y-8">
      <section>
        <div className="eyebrow">Company events</div>
        <h1 className="page-title mt-2">Event Stream</h1>
        <p className="mt-3 max-w-2xl text-sm leading-6 text-neutral-400">Related headlines are grouped into company-level event instances, newest first.</p>
        <div className="mt-4 flex flex-wrap gap-2"><span className="pill">{instances.length.toLocaleString()} stored events</span><span className="pill">{companies} companies in view</span><span className="pill">{themes} themes</span></div>
      </section>

      {recent.length ? (
        <section className="grid gap-3 lg:grid-cols-2">
          {recent.map((event, index) => {
            const symbol = String(event.symbol || "");
            const articles = Array.isArray(event.articles) ? event.articles.slice().sort((a, b) => String(b.date || "").localeCompare(String(a.date || ""))) : [];
            const latestArticle = articles[0];
            return (
              <article key={event.event_instance_id || `${symbol}-${event.start_date}-${index}`} className="rounded-2xl border border-white/10 bg-white/[0.025] p-5">
                <div className="flex gap-4">
                  <CompanyVisual ticker={symbol || "CO"} name={event.name} sector={event.sector} size="md" />
                  <div className="min-w-0 flex-1">
                    <div className="flex items-start justify-between gap-3">
                      <div><div className="eyebrow">{event.theme || "Company news"}</div><h2 className="mt-1 text-lg font-semibold text-white">{event.name || symbol}</h2><div className="mt-1 text-xs text-neutral-600">{symbol} · {event.start_date || "—"}{event.end_date && event.end_date !== event.start_date ? ` → ${event.end_date}` : ""}</div></div>
                      <div className={`font-mono text-sm ${tone(event.sentiment_mean)}`}>{event.sentiment_mean == null ? "—" : `${event.sentiment_mean > 0 ? "+" : ""}${event.sentiment_mean.toFixed(3)}`}</div>
                    </div>
                    {latestArticle?.title ? <div className="mt-3 text-sm leading-6 text-neutral-300">{latestArticle.url ? <a href={latestArticle.url} target="_blank" rel="noreferrer" className="hover:underline">{latestArticle.title}</a> : latestArticle.title}</div> : null}
                    <div className="mt-4 flex flex-wrap gap-x-4 gap-y-2 text-[11px] text-neutral-600"><span>{event.article_count ?? 0} article{event.article_count === 1 ? "" : "s"}</span><span>{event.source_count ?? 0} source{event.source_count === 1 ? "" : "s"}</span><span>Disagreement {event.sentiment_disagreement == null ? "—" : event.sentiment_disagreement.toFixed(3)}</span>{symbol ? <Link href={`/ticker/${symbol}`} className="text-emerald-300 hover:underline">Company →</Link> : null}</div>
                  </div>
                </div>
              </article>
            );
          })}
        </section>
      ) : (
        <section className="grid gap-4 lg:grid-cols-2">{fallback.map((theme) => <div key={theme.theme} className="card p-5"><div className="eyebrow">{theme.count} retained articles</div><h2 className="mt-2 text-lg font-semibold text-white">{theme.theme}</h2><div className="mt-3 text-sm text-neutral-500">Persistent event data is refreshing; this summary uses the current core ticker history.</div></div>)}</section>
      )}
    </main>
  );
}
