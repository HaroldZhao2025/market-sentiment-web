"use client";

import { useMemo, useState } from "react";

/* --------- Props (unchanged) --------- */
export type SeriesIn = { date: string[]; price: number[]; sentiment: number[] };
export type NewsItem = {
  ts: string;
  title: string;
  url: string;
  text?: string;

  // already present, keep:
  source?: string;
  provider?: string;

  // headline-level scalar (any key name we might emit)
  s?: number | string;
  score?: number | string;
  sent?: number | string;

  // per-class probabilities as an object
  probs?: { pos?: number | string; neu?: number | string; neg?: number | string } | null;

  // sometimes stored as 'scores' (object OR array of {label, score})
  scores?: any;

  // sometimes stored as 'sentiment' (object with positive/neutral/negative)
  sentiment?: any;
};


type View = "overlay" | "price" | "sentiment" | "separate";

export default function TickerClient({
  symbol,
  series,
  news,
  newsTotal = 0,
}: {
  symbol: string;
  series: SeriesIn;
  news: NewsItem[];
  newsTotal?: number;
}) {
  const [mode, setMode] = useState<View>("overlay");

  /* ---------- Align series safely ---------- */
  const aligned = useMemo(() => {
    const n = Math.min(
      series.date?.length ?? 0,
      series.price?.length || Infinity,
      series.sentiment?.length || Infinity
    );
    const date = (series.date || []).slice(0, n);
    const price = (series.price || []).slice(0, n);
    const sentiment = (series.sentiment || []).slice(0, n);
    return { date, price, sentiment, n };
  }, [series]);

  const hasPrice = aligned.price.length > 0 && aligned.price.length === aligned.date.length;
  const hasSent  = aligned.sentiment.length > 0 && aligned.sentiment.length === aligned.date.length;

  /* ---------- KPIs ---------- */
  const sMA7 = useMemo(() => ma7(aligned.sentiment), [aligned.sentiment]);
  const lastS  = aligned.n ? Number(aligned.sentiment.at(-1) ?? 0) : 0;
  const lastMA = aligned.n ? Number(sMA7.at(-1) ?? 0) : 0;

  return (
    <div className="mx-auto max-w-6xl px-4 py-8 space-y-8">
      {/* Header + View selector */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-3xl font-bold tracking-tight">Market Sentiment for {symbol}</h1>
        <div className="inline-flex items-center rounded-xl border bg-white p-1" role="tablist" aria-label="Chart view">
          <SegButton active={mode === "separate"} onClick={() => setMode("separate")}  label="Separate View" />
          <SegButton active={mode === "overlay"}  onClick={() => setMode("overlay")}   label="Overlayed View" />
          {hasPrice && <SegButton active={mode === "price"} onClick={() => setMode("price")} label="Price Only" />}
          {hasSent  && <SegButton active={mode === "sentiment"} onClick={() => setMode("sentiment")} label="Sentiment Only" />}
        </div>
      </div>

      {/* Chart card(s) */}
      <div className="rounded-2xl p-6 shadow-sm border bg-white space-y-6">
        <h3 className="font-semibold">Sentiment and Price Analysis</h3>

        {mode === "separate" ? (
          <div className="space-y-6">
            <SentimentBars dates={aligned.date} values={aligned.sentiment} height={300} />
            <PriceLine     dates={aligned.date} values={aligned.price}     height={300} />
          </div>
        ) : (
          <OverlayChart
            dates={aligned.date}
            price={mode !== "sentiment" && hasPrice ? aligned.price : undefined}
            sentiment={mode !== "price"     && hasSent  ? aligned.sentiment : undefined}
            height={520}
          />
        )}
      </div>

      {/* Live Market Insights */}
      <section className="rounded-2xl p-6 shadow-sm border bg-white space-y-4">
        <h3 className="font-semibold">Live Market Insights</h3>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <KpiCard title="Live Market Sentiment" value={label(lastS)} sub="(Latest daily score)" bigValue={lastS.toFixed(4)} />
          <KpiCard title="Predicted Return" value={`${(lastMA * 100).toFixed(2)}%`} sub="7-day sentiment average" />
          <KpiCard title="Advisory Opinion" value={recommendation(lastMA)} sub="Derived from sentiment trend" />
          <KpiCard title="News Items (period)" value={newsTotal.toLocaleString()} />
        </div>
      </section>

      {/* Headlines with date-only + Sentiment column */}
      <section className="rounded-2xl p-6 shadow-sm border bg-white">
        <h3 className="font-semibold mb-2">Recent Headlines for {symbol}</h3>
        <p className="text-xs text-neutral-500 mb-3">
          The table below gives each of the most recent headlines of the stock and the negative, neutral, positive aggregated sentiment score.
        </p>
        {news?.length ? (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="text-left text-neutral-600 border-b">
                <tr>
                  <th className="py-2 pr-3">Date</th>
                  <th className="py-2 pr-3">Headline</th>
                  <th className="py-2 pr-3">Source</th>
                  <th className="py-2 pr-3">Sentiment</th>
                </tr>
              </thead>
              <tbody>
                {news.slice(0, 10).map((n, i) => {
                  const host = n.source || n.provider || extractHost(n.url);
                  const S = fmtHeadlineSentiment(n);
                  return (
                    <tr key={i} className="border-b last:border-b-0">
                      <td className="py-2 pr-3 text-neutral-600">{toDateOnly(n.ts)}</td>
                      <td className="py-2 pr-3">
                        <a className="underline decoration-dotted underline-offset-2" href={n.url} target="_blank" rel="noreferrer">
                          {n.title}
                        </a>
                      </td>
                      <td className="py-2 pr-3 text-neutral-500">{host}</td>
                      <td className="py-2 pr-3">
                        {S.s !== null ? (
                          <>
                            <span className="font-medium">{S.label}</span>{" "}
                            <span className="text-neutral-500">
                              ({S.s.toFixed(4)})
                            </span>
                            {S.hasProbs ? (
                              <span className="ml-2 text-xs text-neutral-500">
                                neg {S.neg!.toFixed(2)} / neu {S.neu!.toFixed(2)} / pos {S.pos!.toFixed(2)}
                              </span>
                            ) : null}
                          </>
                        ) : (
                          <span className="text-neutral-400">–</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="text-neutral-500">No recent headlines found.</div>
        )}
      </section>
    </div>
  );
}

/* ============ UI atoms ============ */
function SegButton({ active, onClick, label }:{ active:boolean; onClick:()=>void; label:string }) {
  return (
    <button
      role="tab"
      aria-selected={active}
      className={[
        "px-3 py-1.5 text-sm rounded-lg transition",
        active ? "bg-black text-white shadow-sm" : "text-neutral-700 hover:bg-neutral-50",
      ].join(" ")}
      onClick={onClick}
    >
      {label}
    </button>
  );
}

function KpiCard({ title, value, sub, bigValue }:{
  title:string; value:string; sub?:string; bigValue?:string;
}) {
  return (
    <div className="rounded-2xl p-5 shadow-sm border bg-white">
      <div className="text-sm text-neutral-500 mb-1">{title}</div>
      <div className="text-2xl md:text-3xl font-semibold">
        {value} {bigValue ? <span className="text-neutral-500 text-lg">({bigValue})</span> : null}
      </div>
      {sub ? <div className="text-xs text-neutral-500 mt-1">{sub}</div> : null}
    </div>
  );
}

/* ============ Shared axis helpers ============ */
function parseISO(s: string): Date {
  const d = new Date(s);
  if (!isNaN(d.getTime())) return d;
  const parts = String(s).split(/[-/]/).map((x) => +x);
  const dd = new Date(parts[0] || 1970, (parts[1] || 1) - 1, parts[2] || 1);
  return isNaN(dd.getTime()) ? new Date() : dd;
}
const MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];

/** Compute monthly tick indices + labels (keeps at most ~8 labels) */
function monthTicks(dates: string[]) {
  if (!dates.length) return [] as { i: number; label: string }[];
  const marks: { i: number; label: string }[] = [];
  let prevM = -1, prevY = -1;
  for (let i = 0; i < dates.length; i++) {
    const dt = parseISO(dates[i]), m = dt.getUTCMonth(), y = dt.getUTCFullYear();
    if (m !== prevM || y !== prevY) {
      marks.push({ i, label: `${MONTHS[m]}` });
      prevM = m; prevY = y;
    }
  }
  const maxLabels = 8;
  if (marks.length > maxLabels) {
    const stride = Math.ceil(marks.length / maxLabels);
    return marks.filter((_, idx) => idx % stride === 0);
  }
  return marks;
}

/* ============ Overlay chart (with month ticks & y labels) ============ */
function OverlayChart({
  dates, price, sentiment, height = 520, width = 980,
}:{
  dates:string[]; price?:number[]; sentiment?:number[]; height?:number; width?:number;
}) {
  const pad = { t: 28, r: 78, b: 44, l: 70 };
  const W = width, H = height;
  const innerW = W - pad.l - pad.r, innerH = H - pad.t - pad.b;
  const n = dates.length, step = n > 1 ? innerW / (n - 1) : innerW;

  const sMax = sentiment && sentiment.length ? Math.max(0.5, ...sentiment.map((x)=>Math.abs(x))) : 1;
  const sY   = (v:number) => pad.t + innerH/2 - (v / sMax) * (innerH/2);
  const sTicks = [-sMax, -sMax/2, 0, sMax/2, sMax];

  const pMin = price && price.length ? Math.min(...price) : 0;
  const pMax = price && price.length ? Math.max(...price) : 1;
  const pY   = (v:number) => pad.t + (1 - (v - pMin) / Math.max(1e-9, pMax - pMin)) * innerH;
  const pTicks = price && price.length ? [pMin, (pMin+pMax)/2, pMax] : [];

  const baselineY = sY(0);
  const monthMarks = monthTicks(dates);

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full rounded-xl border bg-white">
      <rect x={pad.l} y={pad.t} width={innerW} height={innerH} fill="none" stroke="#e5e7eb" />
      {sTicks.map((v, i) => {
        const y = sY(v);
        return (
          <g key={`s${i}`}>
            <line x1={pad.l - 6} x2={pad.l} y1={y} y2={y} stroke="#e5e7eb" />
            <text x={pad.l - 8} y={y + 3} fontSize="11" fill="#6b7280" textAnchor="end">
              {v.toFixed(2)}
            </text>
            {Math.abs(v) > 1e-10 && (
              <line x1={pad.l} x2={pad.l + innerW} y1={y} y2={y} stroke="#f1f5f9" />
            )}
          </g>
        );
      })}
      <text
        x={16}
        y={pad.t + innerH / 2}
        fontSize="12"
        fill="#374151"
        transform={`rotate(-90, 16, ${pad.t + innerH / 2})`}
        textAnchor="middle"
      >
        Sentiment Score
      </text>

      {pTicks.map((v, i) => {
        const y = pY(v);
        return (
          <g key={`p${i}`}>
            <line x1={pad.l+innerW} x2={pad.l+innerW+6} y1={y} y2={y} stroke="#e5e7eb" />
            <text x={pad.l+innerW+8} y={y + 3} fontSize="11" fill="#6b7280">{v.toFixed(2)}</text>
          </g>
        );
      })}
      <text
        x={W - 18}
        y={pad.t + innerH / 2}
        fontSize="12"
        fill="#374151"
        transform={`rotate(90, ${W - 18}, ${pad.t + innerH / 2})`}
        textAnchor="middle"
      >
        Stock Price
      </text>

      {sentiment && sentiment.length ? (
        <line x1={pad.l} x2={pad.l+innerW} y1={sY(0)} y2={sY(0)} stroke="#e5e7eb" />
      ) : null}

      {sentiment?.map((v, i) => {
        const x = pad.l + i * step;
        const y = Math.min(sY(0), sY(v));
        const h = Math.abs(sY(v) - sY(0));
        return <rect key={i} x={x - 1} y={y} width={2} height={Math.max(1, h)} fill="#6b47dc" opacity={0.7} />;
      })}

      {price && price.length > 1
        ? price.map((v, i) => {
            if (i === 0) return null;
            const x1 = pad.l + (i - 1) * step, y1 = pY(price[i - 1]);
            const x2 = pad.l + i * step,       y2 = pY(v);
            return <line key={`l${i}`} x1={x1} y1={y1} x2={x2} y2={y2} stroke="#10b981" strokeWidth={2.5} />;
          })
        : null}
      {price?.map((v, i) => {
        const cx = pad.l + i * step, cy = pY(v);
        return <circle key={`c${i}`} cx={cx} cy={cy} r={2.2} fill="#10b981" />;
      })}

      {monthMarks.map(({ i, label }, k) => {
        const x = pad.l + i * step;
        return (
          <g key={`m${k}`}>
            <line x1={x} x2={x} y1={pad.t + innerH} y2={pad.t + innerH + 5} stroke="#e5e7eb" />
            <text x={x} y={H - 10} fontSize="11" fill="#6b7280" textAnchor="middle">{label}</text>
          </g>
        );
      })}
    </svg>
  );
}

/* ============ Separate charts ============ */
function SentimentBars({ dates, values, height = 300, width = 980 }:{
  dates:string[]; values:number[]; height?:number; width?:number;
}) {
  const pad = { t: 28, r: 24, b: 44, l: 70 };
  const W = width, H = height;
  const innerW = W - pad.l - pad.r, innerH = H - pad.t - pad.b;
  const n = dates.length, step = n > 1 ? innerW / (n - 1) : innerW;

  const sMax = values.length ? Math.max(0.5, ...values.map((x)=>Math.abs(x))) : 1;
  const sY   = (v:number) => pad.t + innerH/2 - (v / sMax) * (innerH/2);
  const baselineY = sY(0);
  const sTicks = [-sMax, -sMax/2, 0, sMax/2, sMax];
  const monthMarks = monthTicks(dates);

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full rounded-xl border bg-white">
      <rect x={pad.l} y={pad.t} width={innerW} height={innerH} fill="none" stroke="#e5e7eb" />
      {sTicks.map((v, i) => {
        const y = sY(v);
        return (
          <g key={i}>
            <line x1={pad.l - 6} x2={pad.l} y1={y} y2={y} stroke="#e5e7eb" />
            <text x={pad.l - 8} y={y + 3} fontSize="11" fill="#6b7280" textAnchor="end">{v.toFixed(2)}</text>
            {Math.abs(v) > 1e-10 && <line x1={pad.l} x2={pad.l + innerW} y1={y} y2={y} stroke="#f1f5f9" />}
          </g>
        );
      })}
      <text
        x={16}
        y={pad.t + innerH / 2}
        fontSize="12"
        fill="#374151"
        transform={`rotate(-90, 16, ${pad.t + innerH / 2})`}
        textAnchor="middle"
      >
        Sentiment Score
      </text>

      <line x1={pad.l} x2={pad.l+innerW} y1={baselineY} y2={baselineY} stroke="#e5e7eb" />
      {values.map((v, i) => {
        const x = pad.l + i * step;
        const y = Math.min(baselineY, sY(v));
        const h = Math.abs(sY(v) - baselineY);
        return <rect key={i} x={x - 1} y={y} width={2} height={Math.max(1, h)} fill="#6b47dc" opacity={0.7} />;
      })}

      {monthMarks.map(({ i, label }, k) => {
        const x = pad.l + i * step;
        return (
          <g key={k}>
            <line x1={x} x2={x} y1={pad.t + innerH} y2={pad.t + innerH + 5} stroke="#e5e7eb" />
            <text x={x} y={H - 10} fontSize="11" fill="#6b7280" textAnchor="middle">{label}</text>
          </g>
        );
      })}
    </svg>
  );
}

function PriceLine({ dates, values, height = 300, width = 980 }:{
  dates:string[]; values:number[]; height?:number; width?:number;
}) {
  const pad = { t: 28, r: 78, b: 44, l: 70 };
  const W = width, H = height;
  const innerW = W - pad.l - pad.r, innerH = H - pad.t - pad.b;
  const n = dates.length, step = n > 1 ? innerW / (n - 1) : innerW;

  const pMin = values.length ? Math.min(...values) : 0;
  const pMax = values.length ? Math.max(...values) : 1;
  const pY   = (v:number) => pad.t + (1 - (v - pMin) / Math.max(1e-9, pMax - pMin)) * innerH;
  const pTicks = [pMin, (pMin+pMax)/2, pMax];
  const monthMarks = monthTicks(dates);

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full rounded-xl border bg-white">
      <rect x={pad.l} y={pad.t} width={innerW} height={innerH} fill="none" stroke="#e5e7eb" />
      {pTicks.map((v, i) => {
        const y = pY(v);
        return (
          <g key={i}>
            <line x1={pad.l+innerW} x2={pad.l+innerW+6} y1={y} y2={y} stroke="#e5e7eb" />
            <text x={pad.l+innerW+8} y={y + 3} fontSize="11" fill="#6b7280">{v.toFixed(2)}</text>
            {i !== 0 && i !== pTicks.length-1 && <line x1={pad.l} x2={pad.l + innerW} y1={y} y2={y} stroke="#f1f5f9" />}
          </g>
        );
      })}
      <text
        x={W - 18}
        y={pad.t + innerH / 2}
        fontSize="12"
        fill="#374151"
        transform={`rotate(90, ${W - 18}, ${pad.t + innerH / 2})`}
        textAnchor="middle"
      >
        Stock Price
      </text>

      {values.map((v, i) => {
        if (i === 0) return null;
        const x1 = pad.l + (i-1) * step, y1 = pY(values[i-1]);
        const x2 = pad.l + i * step,     y2 = pY(v);
        return <line key={`l${i}`} x1={x1} y1={y1} x2={x2} y2={y2} stroke="#10b981" strokeWidth={2.5} />;
      })}
      {values.map((v, i) => {
        const cx = pad.l + i * step, cy = pY(v);
        return <circle key={`c${i}`} cx={cx} cy={cy} r={2.2} fill="#10b981" />;
      })}

      {monthMarks.map(({ i, label }, k) => {
        const x = pad.l + i * step;
        return (
          <g key={k}>
            <line x1={x} x2={x} y1={pad.t + innerH} y2={pad.t + innerH + 5} stroke="#e5e7eb" />
            <text x={x} y={H - 10} fontSize="11" fill="#6b7280" textAnchor="middle">{label}</text>
          </g>
        );
      })}
    </svg>
  );
}

/* ============ Utilities ============ */
function ma7(arr: number[]) {
  const out: number[] = [];
  let run = 0;
  for (let i = 0; i < arr.length; i++) {
    const v = Number(arr[i] || 0);
    run += v;
    if (i >= 7) run -= Number(arr[i - 7] || 0);
    out.push(i >= 6 ? run / 7 : NaN);
  }
  return out;
}
function label(v: number) {
  if (v >= 0.4) return "Strong Positive";
  if (v >= 0.1) return "Positive";
  if (v <= -0.4) return "Strong Negative";
  if (v <= -0.1) return "Negative";
  return "Neutral";
}
function recommendation(v: number) {
  if (v >= 0.4) return "Strong Buy";
  if (v >= 0.1) return "Buy";
  if (v <= -0.4) return "Strong Sell";
  if (v <= -0.1) return "Sell";
  return "Hold";
}
function toDateOnly(x: string) {
  const d = new Date(x);
  if (!isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  const m = String(x).match(/\d{4}-\d{2}-\d{2}/);
  return m ? m[0] : String(x).slice(0, 10);
}
function extractHost(u?: string) {
  try { return u ? new URL(u).host.replace(/^www\./,"") : ""; } catch { return ""; }
}

/* ===== NEW: headline sentiment pretty-printer (robust to many JSON shapes) ===== */
function fmtHeadlineSentiment(n: NewsItem): {
  label: string;
  s: number | null;
  hasProbs: boolean;
  pos?: number;
  neu?: number;
  neg?: number;
} {
  // Helper: extract {pos, neu, neg} from an object with many possible key names
  const asProbObj = (obj: any): { pos: number; neu: number; neg: number } | null => {
    if (!obj || typeof obj !== "object") return null;

    const get = (...keys: string[]) => {
      for (const k of keys) {
        const v = (obj as any)[k];
        if (v !== undefined && v !== null && Number.isFinite(Number(v))) return Number(v);
      }
      return undefined;
    };

    const pos = get("pos", "positive", "POS", "Positive");
    const neu = get("neu", "neutral", "NEU", "Neutral");
    const neg = get("neg", "negative", "NEG", "Negative");

    if ([pos, neu, neg].some((v) => v === undefined)) return null;
    return { pos: num(pos), neu: num(neu), neg: num(neg) };
  };

  // Helper: extract {pos, neu, neg} from an array of {label, score}
  const asProbArr = (arr: any): { pos: number; neu: number; neg: number } | null => {
    if (!Array.isArray(arr)) return null;
    let pos = 0, neu = 0, neg = 0;
    for (const it of arr) {
      if (!it) continue;
      const lbl = String(it.label ?? it.Label ?? "").toLowerCase();
      const sc  = Number(it.score ?? it.Score ?? NaN);
      if (!Number.isFinite(sc)) continue;
      if (lbl.includes("pos")) pos = sc;
      else if (lbl.includes("neu")) neu = sc;
      else if (lbl.includes("neg")) neg = sc;
    }
    const ok = [pos, neu, neg].some((v) => v > 0) || [pos, neu, neg].every((v) => v >= 0);
    return ok ? { pos, neu, neg } : null;
  };

  // 1) Try a direct probs object
  let probs =
    asProbObj((n as any).probs) ||
    asProbObj((n as any).scores) ||
    asProbArr((n as any).scores) ||
    asProbObj((n as any).sentiment);

  if (probs) {
    const s = clamp(num(probs.pos) - num(probs.neg));
    return { label: label(s), s, hasProbs: true, pos: probs.pos, neu: probs.neu, neg: probs.neg };
  }

  // 2) Try scalar fields
  const rawS = (n as any).s ?? (n as any).score ?? (n as any).sent;
  if (rawS !== undefined && rawS !== null && Number.isFinite(Number(rawS))) {
    const s = clamp(Number(rawS));
    return { label: label(s), s, hasProbs: false };
  }

  // 3) No usable info
  return { label: "", s: null, hasProbs: false };
}

function num(v: any): number {
  const x = Number(v);
  return Number.isFinite(x) ? x : 0;
}
function clamp(x: number, lo = -1, hi = 1): number {
  return Math.max(lo, Math.min(hi, x));
}
