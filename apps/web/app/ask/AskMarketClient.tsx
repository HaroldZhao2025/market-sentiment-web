"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import type { ScreenerRow } from "../../lib/intelligence";
import { rowContribution, runMarketQuestion } from "../../lib/marketQuery";

type Props = { rows: ScreenerRow[] };

const examples = [
  "Top 10 technology stocks with positive sentiment but price down today",
  "Show the most negative sentiment stocks in Financials",
  "Find 15 stocks with the largest sentiment-price divergence",
  "Top contributors with earnings events",
  "Which stocks have the most news attention?",
];

function finite(v: unknown) {
  const n = Number(v);
  return v === null || v === undefined || v === "" || !Number.isFinite(n) ? null : n;
}
function tone(v: number | null) {
  if (v == null) return "text-neutral-500";
  if (v > 0) return "text-emerald-300";
  if (v < 0) return "text-rose-300";
  return "text-neutral-300";
}
function pct(v: number | null, d = 2) {
  return v == null ? "—" : `${v > 0 ? "+" : ""}${(v * 100).toFixed(d)}%`;
}
function num(v: number | null, d = 3) {
  return v == null ? "—" : `${v > 0 ? "+" : ""}${v.toFixed(d)}`;
}

export default function AskMarketClient({ rows }: Props) {
  const [question, setQuestion] = useState(examples[0]);
  const [submitted, setSubmitted] = useState(examples[0]);
  const result = useMemo(() => runMarketQuestion(submitted, rows), [submitted, rows]);

  return (
    <div className="space-y-6">
      <section className="ambient-panel p-5 md:p-6">
        <div className="flex flex-col gap-3 lg:flex-row">
          <input
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter" && question.trim()) setSubmitted(question.trim()); }}
            className="min-w-0 flex-1 rounded-xl border border-white/10 bg-black/30 px-4 py-3 text-sm text-white outline-none placeholder:text-neutral-600 focus:border-emerald-400/30"
            placeholder="Ask a market question…"
          />
          <button type="button" onClick={() => question.trim() && setSubmitted(question.trim())} className="rounded-xl border border-emerald-400/25 bg-emerald-400/10 px-5 py-3 text-sm font-semibold text-emerald-200 transition hover:bg-emerald-400/15">
            Run query
          </button>
        </div>
        <div className="mt-4 flex flex-wrap gap-2">
          {examples.map((example) => (
            <button key={example} type="button" onClick={() => { setQuestion(example); setSubmitted(example); }} className="pill text-left">
              {example}
            </button>
          ))}
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_320px]">
        <div className="space-y-4">
          <div className="flex flex-wrap items-end justify-between gap-3">
            <div>
              <div className="eyebrow">Deterministic answer</div>
              <h2 className="section-title mt-1">{result.matched} matching constituents</h2>
              <p className="section-copy">Showing the top {result.rows.length}. Every row comes from the same screener and attribution inputs used elsewhere on the site.</p>
            </div>
          </div>

          <div className="table-shell overflow-x-auto">
            <table className="w-full min-w-[980px] text-sm">
              <thead className="border-b border-white/10 bg-white/[0.025] text-left text-[11px] uppercase tracking-[0.1em] text-neutral-600">
                <tr>
                  <th className="px-4 py-3">Ticker / company</th>
                  <th className="px-4 py-3">Sector / event</th>
                  <th className="px-4 py-3 text-right">Price</th>
                  <th className="px-4 py-3 text-right">1D</th>
                  <th className="px-4 py-3 text-right">Sentiment</th>
                  <th className="px-4 py-3 text-right">Δ sentiment</th>
                  <th className="px-4 py-3 text-right">Divergence</th>
                  <th className="px-4 py-3 text-right">Contribution</th>
                  <th className="px-4 py-3 text-right">News</th>
                </tr>
              </thead>
              <tbody>
                {result.rows.map((r) => {
                  const ret = finite(r.return_1d), s = finite(r.sentiment), ds = finite(r.sentiment_change), div = finite(r.divergence), c = rowContribution(r);
                  return (
                    <tr key={r.symbol} className="border-b border-white/[0.06] last:border-0 hover:bg-white/[0.025]">
                      <td className="px-4 py-3">
                        <Link href={`/ticker/${r.symbol}`} className="font-semibold text-white hover:text-emerald-300">{r.symbol}</Link>
                        <div className="max-w-[230px] truncate text-xs text-neutral-500">{r.name || "Company name unavailable"}</div>
                      </td>
                      <td className="px-4 py-3"><div className="text-xs text-neutral-300">{r.sector || "Unknown"}</div><div className="max-w-[210px] truncate text-[11px] text-neutral-600">{r.event_theme || r.industry || "—"}</div></td>
                      <td className={`px-4 py-3 text-right font-mono ${tone(ret)}`}>{finite(r.price)?.toFixed(2) ?? "—"}</td>
                      <td className={`px-4 py-3 text-right font-mono ${tone(ret)}`}>{pct(ret)}</td>
                      <td className={`px-4 py-3 text-right font-mono ${tone(s)}`}>{num(s)}</td>
                      <td className={`px-4 py-3 text-right font-mono ${tone(ds)}`}>{num(ds)}</td>
                      <td className={`px-4 py-3 text-right font-mono ${tone(div)}`}>{num(div)}</td>
                      <td className={`px-4 py-3 text-right font-mono ${tone(c)}`}>{c == null ? "—" : `${c >= 0 ? "+" : ""}${(c * 10000).toFixed(2)} bps`}</td>
                      <td className="px-4 py-3 text-right font-mono text-neutral-400">{finite(r.n_total)?.toFixed(0) ?? "—"}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>

        <aside className="space-y-4">
          <div className="card p-5">
            <div className="eyebrow">Query plan</div>
            <h3 className="mt-2 font-semibold text-white">How your question was interpreted</h3>
            <div className="mt-4 space-y-2">
              {result.plan.interpretation.map((line) => <div key={line} className="rounded-xl border border-white/10 bg-black/20 px-3 py-2 text-xs text-neutral-300">{line}</div>)}
            </div>
          </div>
          <div className="card p-5 text-sm leading-6 text-neutral-500">
            <div className="font-semibold text-neutral-300">Why this is auditable</div>
            <p className="mt-2">The question parser only maps language to explicit filters and rankings. It does not invent an explanation or forecast. Missing sentiment remains missing and is excluded by default.</p>
          </div>
        </aside>
      </section>
    </div>
  );
}
