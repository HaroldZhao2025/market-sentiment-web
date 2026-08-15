"use client";

import { useMemo, useState } from "react";

type EarningsHistory = { date?: string; eps_estimate?: number | null; reported_eps?: number | null; surprise_pct?: number | null };
type Topic = { topic: string; mentions: number; sentiment: number | null };
type Turn = {
  turn?: number;
  speaker?: string;
  role?: string;
  section?: "prepared" | "qa" | string;
  sentiment?: number | null;
  word_count?: number | null;
  topic_hits?: string[];
  uncertainty_hits?: number;
  forward_looking_hits?: number;
};
type Call = {
  quarter?: string | null;
  date?: string | null;
  source?: string;
  source_url?: string;
  source_type?: string;
  transcript_word_count?: number;
  transcript_text_redistributed?: boolean;
  summary?: {
    turn_count?: number;
    scored_turn_count?: number;
    overall_sentiment?: number | null;
    prepared_sentiment?: number | null;
    qa_sentiment?: number | null;
    qa_tone_shift?: number | null;
    uncertainty_turn_rate?: number | null;
    forward_looking_turn_rate?: number | null;
    topics?: Topic[];
  };
  price_reaction?: { return_1d?: number | null; return_5d?: number | null };
  turns?: Turn[];
};
type Filing = { ts?: string; title?: string; url?: string; source?: string; document_type?: string; S?: number };
type CallLink = { title?: string; url?: string; source?: string; provider?: string; ts?: string };
export type EarningsArtifact = {
  schema_version?: number;
  symbol?: string;
  earnings_history?: EarningsHistory[];
  calls?: Call[];
  call_links?: CallLink[];
  filing_fallback?: Filing[];
  methodology?: Record<string, unknown>;
};

function finite(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}
function tone(value: number | null) {
  if (value == null) return "text-neutral-500";
  if (value > 0) return "text-emerald-300";
  if (value < 0) return "text-rose-300";
  return "text-neutral-300";
}
function signed(value: number | null, digits = 3) {
  return value == null ? "—" : `${value > 0 ? "+" : ""}${value.toFixed(digits)}`;
}
function pct(value: number | null, digits = 1) {
  return value == null ? "—" : `${value > 0 ? "+" : ""}${(value * 100).toFixed(digits)}%`;
}
function dateOnly(value?: string | null) {
  return value ? value.slice(0, 10) : "—";
}

export default function EarningsIntelligenceClient({ symbol, data }: { symbol: string; data: EarningsArtifact }) {
  const calls = Array.isArray(data.calls) ? data.calls : [];
  const history = Array.isArray(data.earnings_history) ? data.earnings_history : [];
  const filings = Array.isArray(data.filing_fallback) ? data.filing_fallback : [];
  const [callIndex, setCallIndex] = useState(0);
  const [section, setSection] = useState<"all" | "prepared" | "qa">("all");
  const [turnLimit, setTurnLimit] = useState(20);
  const call = calls[Math.min(callIndex, Math.max(0, calls.length - 1))] ?? null;
  const summary = call?.summary ?? {};
  const turns = useMemo(() => {
    const rows = Array.isArray(call?.turns) ? call!.turns! : [];
    return rows.filter((turn) => section === "all" || turn.section === section).slice(0, turnLimit);
  }, [call, section, turnLimit]);

  return (
    <div className="space-y-7">
      <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <div className="kpi"><div className="kpi-label">Overall call tone</div><div className={`kpi-value ${tone(finite(summary.overall_sentiment))}`}>{signed(finite(summary.overall_sentiment))}</div><div className="kpi-sub">FinBERT across transcript turns</div></div>
        <div className="kpi"><div className="kpi-label">Prepared remarks</div><div className={`kpi-value ${tone(finite(summary.prepared_sentiment))}`}>{signed(finite(summary.prepared_sentiment))}</div><div className="kpi-sub">Management prepared section</div></div>
        <div className="kpi"><div className="kpi-label">Q&amp;A tone</div><div className={`kpi-value ${tone(finite(summary.qa_sentiment))}`}>{signed(finite(summary.qa_sentiment))}</div><div className="kpi-sub">Analyst Q&amp;A section</div></div>
        <div className="kpi"><div className="kpi-label">Q&amp;A tone shift</div><div className={`kpi-value ${tone(finite(summary.qa_tone_shift))}`}>{signed(finite(summary.qa_tone_shift))}</div><div className="kpi-sub">Q&amp;A minus prepared remarks</div></div>
      </section>

      <section className="grid gap-4 xl:grid-cols-[minmax(0,1.35fr)_minmax(320px,.65fr)]">
        <div className="card p-5">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div><div className="eyebrow">Call diagnostics</div><h2 className="section-title mt-1">Management tone under questioning</h2><p className="section-copy">Prepared remarks and Q&amp;A are scored separately when a free public transcript is available.</p></div>
            {calls.length > 1 ? <select value={callIndex} onChange={(e) => { setCallIndex(Number(e.target.value)); setTurnLimit(20); }} className="rounded-xl border border-white/10 bg-neutral-900 px-3 py-2 text-xs text-neutral-300 outline-none">{calls.map((item, index) => <option key={`${item.quarter}-${index}`} value={index}>{item.quarter || `Call ${index + 1}`} · {dateOnly(item.date)}</option>)}</select> : null}
          </div>
          {call ? <div className="mt-5 grid gap-3 sm:grid-cols-2 lg:grid-cols-4"><div className="rounded-xl border border-white/10 bg-black/20 p-3"><div className="kpi-label">Uncertainty rate</div><div className="mt-1 font-mono text-lg text-neutral-200">{pct(finite(summary.uncertainty_turn_rate))}</div></div><div className="rounded-xl border border-white/10 bg-black/20 p-3"><div className="kpi-label">Forward-looking rate</div><div className="mt-1 font-mono text-lg text-neutral-200">{pct(finite(summary.forward_looking_turn_rate))}</div></div><div className="rounded-xl border border-white/10 bg-black/20 p-3"><div className="kpi-label">Transcript words</div><div className="mt-1 font-mono text-lg text-neutral-200">{call.transcript_word_count?.toLocaleString() ?? "—"}</div></div><div className="rounded-xl border border-white/10 bg-black/20 p-3"><div className="kpi-label">Source</div>{call.source_url ? <a href={call.source_url} target="_blank" rel="noreferrer" className="mt-1 block truncate text-sm text-sky-300 hover:underline">{call.source || "Public transcript"}</a> : <div className="mt-1 text-sm text-neutral-500">{call.source || "—"}</div>}</div></div> : <div className="mt-5 text-sm leading-6 text-neutral-500">No structured free-public transcript has been fulfilled for this company yet.</div>}
        </div>

        <div className="card p-5">
          <div className="eyebrow">Topics</div><h2 className="section-title mt-1">What management discussed</h2>
          <div className="mt-4 space-y-3">{(summary.topics || []).length ? (summary.topics || []).map((topic) => <div key={topic.topic} className="rounded-xl border border-white/10 bg-black/20 p-3"><div className="flex items-center justify-between gap-3"><div><div className="text-sm font-medium text-neutral-200">{topic.topic}</div><div className="mt-1 text-[11px] text-neutral-600">{topic.mentions} scored turn{topic.mentions === 1 ? "" : "s"}</div></div><div className={`font-mono text-xs ${tone(finite(topic.sentiment))}`}>{signed(finite(topic.sentiment))}</div></div></div>) : <div className="text-sm text-neutral-500">No transcript topic diagnostics yet.</div>}</div>
        </div>
      </section>

      <section className="space-y-3">
        <div><div className="eyebrow">Results</div><h2 className="section-title mt-1">Earnings surprise history</h2></div>
        {history.length ? <div className="table-shell overflow-x-auto"><table className="w-full min-w-[680px] text-sm"><thead className="border-b border-white/10 bg-white/[0.025] text-left text-[11px] uppercase tracking-[0.1em] text-neutral-600"><tr><th className="px-4 py-3">Date</th><th className="px-4 py-3 text-right">EPS estimate</th><th className="px-4 py-3 text-right">Reported EPS</th><th className="px-4 py-3 text-right">Surprise</th></tr></thead><tbody>{history.map((row, index) => { const surprise = finite(row.surprise_pct); return <tr key={`${row.date}-${index}`} className="border-b border-white/[0.06] last:border-0"><td className="px-4 py-3 font-mono text-xs text-neutral-500">{dateOnly(row.date)}</td><td className="px-4 py-3 text-right font-mono text-neutral-300">{finite(row.eps_estimate)?.toFixed(3) ?? "—"}</td><td className="px-4 py-3 text-right font-mono text-neutral-300">{finite(row.reported_eps)?.toFixed(3) ?? "—"}</td><td className={`px-4 py-3 text-right font-mono ${tone(surprise)}`}>{surprise == null ? "—" : `${surprise > 0 ? "+" : ""}${surprise.toFixed(2)}%`}</td></tr>; })}</tbody></table></div> : <div className="card p-4 text-sm text-neutral-500">No EPS surprise history is available.</div>}
      </section>

      {call ? <section className="space-y-3"><div className="flex flex-wrap items-end justify-between gap-4"><div><div className="eyebrow">Call turns</div><h2 className="section-title mt-1">Speaker-level diagnostics</h2><p className="section-copy">We retain derived speaker/section metrics and source links, not third-party transcript body text.</p></div><div className="flex rounded-xl border border-white/10 bg-black/30 p-1">{(["all", "prepared", "qa"] as const).map((value) => <button key={value} type="button" onClick={() => { setSection(value); setTurnLimit(20); }} className={`rounded-lg px-3 py-2 text-xs ${section === value ? "bg-white/10 text-white" : "text-neutral-500"}`}>{value === "all" ? "All" : value === "prepared" ? "Prepared" : "Q&A"}</button>)}</div></div><div className="grid gap-2 md:grid-cols-2">{turns.map((turn, index) => <div key={`${turn.turn}-${index}`} className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-4"><div className="flex items-start justify-between gap-3"><div><div className="font-medium text-neutral-200">{turn.speaker || "Unknown speaker"}</div><div className="mt-1 text-[11px] text-neutral-600">{turn.section || "—"} · {turn.word_count ?? 0} words</div></div><span className={`font-mono text-xs ${tone(finite(turn.sentiment))}`}>{signed(finite(turn.sentiment))}</span></div>{turn.topic_hits?.length ? <div className="mt-3 flex flex-wrap gap-1.5">{turn.topic_hits.map((topic) => <span key={topic} className="rounded-full border border-white/10 px-2 py-1 text-[10px] text-neutral-500">{topic}</span>)}</div> : null}</div>)}</div>{(call.turns?.length || 0) > turnLimit ? <button type="button" onClick={() => setTurnLimit((value) => value + 20)} className="rounded-xl border border-white/10 bg-white/[0.04] px-4 py-2 text-sm text-neutral-300 hover:bg-white/[0.07]">Show more turns</button> : null}</section> : null}

      <section className="space-y-3"><div><div className="eyebrow">SEC &amp; IR</div><h2 className="section-title mt-1">Supporting earnings material</h2></div>{filings.length ? <div className="grid gap-3 md:grid-cols-2">{filings.map((filing, index) => <a key={`${filing.url}-${index}`} href={filing.url} target="_blank" rel="noreferrer" className="card card-hover p-4"><div className="text-[11px] text-neutral-600">{dateOnly(filing.ts)} · {filing.source || "SEC EDGAR"}</div><div className="mt-2 text-sm font-medium leading-6 text-neutral-200">{filing.title || "Untitled filing"}</div><div className="mt-1 text-[10px] uppercase tracking-[0.1em] text-neutral-700">{filing.document_type || "filing"}</div></a>)}</div> : <div className="card p-4 text-sm text-neutral-500">No supporting filing material is available.</div>}</section>
    </div>
  );
}
