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
type CallLink = { title?: string; url?: string; source?: string; ts?: string };
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
  if (!value) return "—";
  const date = new Date(value);
  if (!Number.isNaN(date.getTime())) return date.toISOString().slice(0, 10);
  return value.slice(0, 10);
}

export default function EarningsIntelligenceClient({ symbol, data }: { symbol: string; data: EarningsArtifact }) {
  const calls = Array.isArray(data.calls) ? data.calls : [];
  const callLinks = Array.isArray(data.call_links) ? data.call_links : [];
  const history = Array.isArray(data.earnings_history) ? data.earnings_history : [];
  const filings = Array.isArray(data.filing_fallback) ? data.filing_fallback : [];
  const [callIndex, setCallIndex] = useState(0);
  const [section, setSection] = useState<"all" | "prepared" | "qa">("all");
  const [turnLimit, setTurnLimit] = useState(24);
  const call = calls[Math.min(callIndex, Math.max(0, calls.length - 1))] ?? null;
  const summary = call?.summary ?? {};
  const turns = useMemo(() => {
    const rows = Array.isArray(call?.turns) ? call!.turns! : [];
    return rows.filter((turn) => section === "all" || turn.section === section).slice(0, turnLimit);
  }, [call, section, turnLimit]);

  return (
    <div className="space-y-7">
      <section className="flex flex-wrap items-center justify-between gap-3 rounded-2xl border border-white/10 bg-white/[0.025] p-4">
        <div>
          <div className="eyebrow">Call coverage</div>
          <div className="mt-1 text-sm text-neutral-300">{calls.length ? `${calls.length} structured free-public transcript${calls.length === 1 ? "" : "s"}` : "Structured transcript pending"}</div>
        </div>
        <div className="flex flex-wrap gap-2 text-xs">
          <span className="pill">{history.length} results</span>
          <span className="pill">{callLinks.length} public source{callLinks.length === 1 ? "" : "s"}</span>
          <span className="pill">{filings.length} SEC item{filings.length === 1 ? "" : "s"}</span>
        </div>
      </section>

      {callLinks.length ? (
        <section className="space-y-3">
          <div><div className="eyebrow">Public sources</div><h2 className="section-title mt-1">Earnings-call sources</h2><p className="section-copy">Free public source pages used for discovery. Structured metrics appear only after a transcript is successfully parsed.</p></div>
          <div className="grid gap-3 md:grid-cols-2">
            {callLinks.slice(0, 8).map((link, index) => <a key={`${link.url}-${index}`} href={link.url} target="_blank" rel="noreferrer" className="card card-hover p-4"><div className="text-[11px] text-neutral-600">{dateOnly(link.ts)} · {link.source || "Public source"}</div><div className="mt-2 text-sm font-medium leading-6 text-neutral-200">{link.title || "Earnings-call source"}</div></a>)}
          </div>
        </section>
      ) : null}

      <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <div className="kpi"><div className="kpi-label">Overall call tone</div><div className={`kpi-value ${tone(finite(summary.overall_sentiment))}`}>{signed(finite(summary.overall_sentiment))}</div><div className="kpi-sub">FinBERT across parsed turns</div></div>
        <div className="kpi"><div className="kpi-label">Prepared remarks</div><div className={`kpi-value ${tone(finite(summary.prepared_sentiment))}`}>{signed(finite(summary.prepared_sentiment))}</div><div className="kpi-sub">Management prepared section</div></div>
        <div className="kpi"><div className="kpi-label">Q&amp;A tone</div><div className={`kpi-value ${tone(finite(summary.qa_sentiment))}`}>{signed(finite(summary.qa_sentiment))}</div><div className="kpi-sub">Question-and-answer section</div></div>
        <div className="kpi"><div className="kpi-label">Q&amp;A tone shift</div><div className={`kpi-value ${tone(finite(summary.qa_tone_shift))}`}>{signed(finite(summary.qa_tone_shift))}</div><div className="kpi-sub">Q&amp;A minus prepared</div></div>
      </section>

      <section className="grid gap-4 xl:grid-cols-[minmax(0,1.35fr)_minmax(320px,.65fr)]">
        <div className="card p-5">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div><div className="eyebrow">Call diagnostics</div><h2 className="section-title mt-1">Management tone under questioning</h2><p className="section-copy">Prepared remarks and Q&amp;A are scored separately when a free public transcript is available.</p></div>
            {calls.length > 1 ? <select value={callIndex} onChange={(e) => { setCallIndex(Number(e.target.value)); setTurnLimit(24); }} className="rounded-xl border border-white/10 bg-neutral-900 px-3 py-2 text-xs text-neutral-300 outline-none">{calls.map((item, index) => <option key={`${item.source_url}-${index}`} value={index}>{item.quarter || `Call ${index + 1}`} · {dateOnly(item.date)}</option>)}</select> : null}
          </div>
          {call ? <>
            <div className="mt-4 flex flex-wrap items-center gap-2 text-xs"><span className="rounded-full border border-emerald-400/20 bg-emerald-400/10 px-2.5 py-1 text-emerald-300">Structured transcript</span><span className="text-neutral-600">{call.source || "Free public transcript"}</span>{call.transcript_word_count ? <span className="text-neutral-700">· {call.transcript_word_count.toLocaleString()} words analyzed</span> : null}{call.source_url ? <a href={call.source_url} target="_blank" rel="noreferrer" className="text-sky-300 hover:underline">Open source ↗</a> : null}</div>
            <div className="mt-5 grid gap-3 sm:grid-cols-2 lg:grid-cols-4"><div className="rounded-xl border border-white/10 bg-black/20 p-3"><div className="kpi-label">Uncertainty rate</div><div className="mt-1 font-mono text-lg text-neutral-200">{pct(finite(summary.uncertainty_turn_rate))}</div></div><div className="rounded-xl border border-white/10 bg-black/20 p-3"><div className="kpi-label">Forward-looking rate</div><div className="mt-1 font-mono text-lg text-neutral-200">{pct(finite(summary.forward_looking_turn_rate))}</div></div><div className="rounded-xl border border-white/10 bg-black/20 p-3"><div className="kpi-label">Parsed turns</div><div className="mt-1 font-mono text-lg text-neutral-200">{Number(summary.turn_count || 0).toLocaleString()}</div></div><div className="rounded-xl border border-white/10 bg-black/20 p-3"><div className="kpi-label">Source policy</div><div className="mt-1 text-sm font-medium text-emerald-300">Free public only</div></div></div>
          </> : <div className="mt-5 rounded-xl border border-amber-400/15 bg-amber-400/[0.04] p-4 text-sm leading-6 text-neutral-400">No structured transcript has been fulfilled yet for {symbol}. Public call links and SEC evidence remain visible, but they do not populate tone metrics until transcript text is actually parsed.</div>}
        </div>

        <div className="card p-5">
          <div className="eyebrow">Topics</div><h2 className="section-title mt-1">What management discussed</h2>
          <div className="mt-4 space-y-3">{(summary.topics || []).length ? (summary.topics || []).map((topic) => <div key={topic.topic} className="rounded-xl border border-white/10 bg-black/20 p-3"><div className="flex items-center justify-between gap-3"><div><div className="text-sm font-medium text-neutral-200">{topic.topic}</div><div className="mt-1 text-[11px] text-neutral-600">{topic.mentions} turn{topic.mentions === 1 ? "" : "s"}</div></div><div className={`font-mono text-xs ${tone(finite(topic.sentiment))}`}>{signed(finite(topic.sentiment))}</div></div></div>) : <div className="text-sm text-neutral-500">No transcript topic diagnostics yet.</div>}</div>
        </div>
      </section>

      <section className="space-y-3">
        <div><div className="eyebrow">Reported fundamentals</div><h2 className="section-title mt-1">Earnings surprise history</h2></div>
        {history.length ? <div className="table-shell overflow-x-auto"><table className="w-full min-w-[680px] text-sm"><thead className="border-b border-white/10 bg-white/[0.025] text-left text-[11px] uppercase tracking-[0.1em] text-neutral-600"><tr><th className="px-4 py-3">Date</th><th className="px-4 py-3 text-right">EPS estimate</th><th className="px-4 py-3 text-right">Reported EPS</th><th className="px-4 py-3 text-right">Surprise</th></tr></thead><tbody>{history.map((row, index) => { const surprise = finite(row.surprise_pct); return <tr key={`${row.date}-${index}`} className="border-b border-white/[0.06] last:border-0"><td className="px-4 py-3 font-mono text-xs text-neutral-500">{dateOnly(row.date)}</td><td className="px-4 py-3 text-right font-mono text-neutral-300">{finite(row.eps_estimate)?.toFixed(3) ?? "—"}</td><td className="px-4 py-3 text-right font-mono text-neutral-300">{finite(row.reported_eps)?.toFixed(3) ?? "—"}</td><td className={`px-4 py-3 text-right font-mono ${tone(surprise)}`}>{surprise == null ? "—" : `${surprise > 0 ? "+" : ""}${surprise.toFixed(2)}%`}</td></tr>; })}</tbody></table></div> : <div className="card p-4 text-sm text-neutral-500">No structured EPS surprise history in this artifact.</div>}
      </section>

      {call ? <section className="space-y-3"><div className="flex flex-wrap items-end justify-between gap-4"><div><div className="eyebrow">Speaker diagnostics</div><h2 className="section-title mt-1">Turn-level call signals</h2><p className="section-copy">We retain derived diagnostics, not third-party transcript text. Use the source link above to read the original transcript.</p></div><div className="flex rounded-xl border border-white/10 bg-black/30 p-1">{(["all", "prepared", "qa"] as const).map((value) => <button key={value} type="button" onClick={() => { setSection(value); setTurnLimit(24); }} className={`rounded-lg px-3 py-2 text-xs ${section === value ? "bg-white/10 text-white" : "text-neutral-500"}`}>{value === "all" ? "All" : value === "prepared" ? "Prepared" : "Q&A"}</button>)}</div></div><div className="grid gap-2 md:grid-cols-2">{turns.map((turn, index) => <div key={`${turn.turn}-${index}`} className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-4"><div className="flex items-start justify-between gap-3"><div><span className="font-medium text-neutral-200">{turn.speaker || "Unknown speaker"}</span><span className="ml-2 rounded-full border border-white/10 px-2 py-0.5 text-[10px] uppercase text-neutral-500">{turn.section || "—"}</span></div><span className={`font-mono text-xs ${tone(finite(turn.sentiment))}`}>{signed(finite(turn.sentiment))}</span></div><div className="mt-3 flex flex-wrap gap-2 text-[11px] text-neutral-600"><span>{Number(turn.word_count || 0)} words</span>{(turn.topic_hits || []).map((topic) => <span key={topic} className="rounded-full border border-white/10 px-2 py-0.5">{topic}</span>)}{Number(turn.uncertainty_hits || 0) > 0 ? <span>uncertainty ×{turn.uncertainty_hits}</span> : null}{Number(turn.forward_looking_hits || 0) > 0 ? <span>forward ×{turn.forward_looking_hits}</span> : null}</div></div>)}</div>{(call.turns?.length || 0) > turnLimit ? <button type="button" onClick={() => setTurnLimit((value) => value + 24)} className="rounded-xl border border-white/10 bg-white/[0.04] px-4 py-2 text-sm text-neutral-300 hover:bg-white/[0.07]">Show more turns</button> : null}</section> : null}

      <section className="space-y-3"><div><div className="eyebrow">SEC &amp; issuer evidence</div><h2 className="section-title mt-1">Earnings documents</h2><p className="section-copy">Transcript exhibits, releases, presentations and filing evidence are classified separately.</p></div>{filings.length ? <div className="grid gap-3 md:grid-cols-2">{filings.map((filing, index) => <a key={`${filing.url}-${index}`} href={filing.url} target="_blank" rel="noreferrer" className="card card-hover p-4"><div className="text-[11px] text-neutral-600">{dateOnly(filing.ts)} · {filing.source || "SEC EDGAR"} · {filing.document_type || "filing"}</div><div className="mt-2 text-sm font-medium leading-6 text-neutral-200">{filing.title || "Untitled filing"}</div></a>)}</div> : <div className="card p-4 text-sm text-neutral-500">No recent earnings-related SEC evidence in this artifact.</div>}</section>
    </div>
  );
}
