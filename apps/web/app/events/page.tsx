import Link from "next/link";
import { buildEventMemory, finite } from "../../lib/intelligence";

export const dynamic = "force-static";

export const metadata = {
  title: "Event Memory",
};

function tone(v: number | null) {
  if (v == null) return "text-neutral-500";
  if (v > 0) return "text-emerald-300";
  if (v < 0) return "text-rose-300";
  return "text-neutral-300";
}

function pct(v: number | null, d = 2) {
  return v == null ? "—" : `${v > 0 ? "+" : ""}${(v * 100).toFixed(d)}%`;
}

export default function EventsPage() {
  const themes = buildEventMemory();
  const total = themes.reduce((s, x) => s + x.count, 0);

  return (
    <main className="space-y-8">
      <section>
        <div className="eyebrow">Historical event memory</div>
        <h1 className="page-title mt-2">Event Intelligence</h1>
        <p className="mt-3 max-w-3xl text-sm leading-6 text-neutral-400">
          A deterministic memory layer over the article history retained in current ticker artifacts. Events are classified by rules and linked to observed price reactions; no generative narrative is used.
        </p>
        <div className="mt-4 flex flex-wrap gap-2">
          <span className="pill">{total.toLocaleString()} retained article events</span>
          <span className="pill">{themes.length} event themes</span>
          <span className="pill">Novelty + source disagreement</span>
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        {themes.map((theme) => (
          <article key={theme.theme} className="ambient-panel p-5">
            <div className="flex items-start justify-between gap-4">
              <div>
                <div className="eyebrow">{theme.count} events · {theme.ticker_count} tickers</div>
                <h2 className="mt-2 text-xl font-semibold text-white">{theme.theme}</h2>
              </div>
              <div className={`font-mono text-sm ${tone(theme.avg_sentiment)}`}>{theme.avg_sentiment == null ? "—" : `${theme.avg_sentiment > 0 ? "+" : ""}${theme.avg_sentiment.toFixed(3)}`}</div>
            </div>

            <div className="mt-5 grid grid-cols-2 gap-3 md:grid-cols-4">
              <div className="rounded-xl border border-white/[0.07] bg-black/20 p-3">
                <div className="text-[10px] uppercase tracking-wider text-neutral-600">Avg 1D reaction</div>
                <div className={`mt-1 font-mono text-sm ${tone(theme.avg_return_1d)}`}>{pct(theme.avg_return_1d)}</div>
              </div>
              <div className="rounded-xl border border-white/[0.07] bg-black/20 p-3">
                <div className="text-[10px] uppercase tracking-wider text-neutral-600">Avg 5D reaction</div>
                <div className={`mt-1 font-mono text-sm ${tone(theme.avg_return_5d)}`}>{pct(theme.avg_return_5d)}</div>
              </div>
              <div className="rounded-xl border border-white/[0.07] bg-black/20 p-3">
                <div className="text-[10px] uppercase tracking-wider text-neutral-600">Positive 1D</div>
                <div className="mt-1 font-mono text-sm text-neutral-300">{theme.positive_1d_rate == null ? "—" : `${(theme.positive_1d_rate * 100).toFixed(1)}%`}</div>
              </div>
              <div className="rounded-xl border border-white/[0.07] bg-black/20 p-3">
                <div className="text-[10px] uppercase tracking-wider text-neutral-600">Novelty</div>
                <div className="mt-1 font-mono text-sm text-neutral-300">{theme.avg_novelty == null ? "—" : theme.avg_novelty.toFixed(2)}</div>
              </div>
            </div>

            <div className="mt-4 flex flex-wrap gap-3 text-[11px] text-neutral-600">
              <span>{theme.source_count} distinct sources</span>
              <span>Sentiment disagreement: {finite(theme.disagreement)?.toFixed(3) ?? "—"}</span>
            </div>

            {theme.recent_examples.length ? (
              <div className="mt-5 space-y-2 border-t border-white/[0.07] pt-4">
                <div className="text-[11px] font-semibold uppercase tracking-[0.12em] text-neutral-600">Recent retained examples</div>
                {theme.recent_examples.slice(0, 3).map((item, i) => (
                  <div key={`${item.symbol}-${item.date}-${i}`} className="rounded-xl bg-white/[0.025] p-3">
                    <div className="flex items-center justify-between gap-3">
                      <Link href={`/ticker/${item.symbol}`} className="text-xs font-semibold text-white hover:text-emerald-300">{item.symbol}</Link>
                      <div className="font-mono text-[10px] text-neutral-600">{item.date}</div>
                    </div>
                    <div className="mt-1 text-xs leading-5 text-neutral-300">{item.title}</div>
                    <div className="mt-2 flex flex-wrap gap-3 text-[10px] text-neutral-600">
                      <span>{item.source || "Unknown source"}</span>
                      <span className={tone(item.sentiment)}>S {item.sentiment == null ? "—" : item.sentiment.toFixed(3)}</span>
                      <span className={tone(item.return_1d)}>1D {pct(item.return_1d)}</span>
                    </div>
                  </div>
                ))}
              </div>
            ) : null}
          </article>
        ))}
      </section>

      <section className="card p-5 text-sm leading-6 text-neutral-500">
        Event Memory is bounded by the article history retained in generated ticker JSON. It should not be interpreted as a complete historical news database. Price reactions use the first available trading observation on or after the article date.
      </section>
    </main>
  );
}
