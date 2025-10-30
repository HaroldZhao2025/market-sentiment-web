"use client";

import { useMemo, useState } from "react";

/* --------- Props (unchanged) --------- */
export type SeriesIn = { date: string[]; price: number[]; sentiment: number[] };
export type NewsItem = { ts: string; title: string; url: string; text?: string };

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
            {/* Sentiment bars around zero */}
            <SentimentBars dates={aligned.date} values={aligned.sentiment} height={300} />
            {/* Price line with markers */}
            <PriceLine dates={aligned.date} values={aligned.price} height={300} />
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

      {/* Headlines with date-only */}
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
                </tr>
              </thead>
              <tbody>
                {news.slice(0, 10).map((n, i) => (
                  <tr key={i} className="border-b last:border-b-0">
                    <td className="py-2 pr-3 text-neutral-600">{toDateOnly(n.ts)}</td>
                    <td className="py-2 pr-3">
                      <a className="underline decoration-dotted underline-offset-2" href={n.url} target="_blank" rel="noreferrer">
                        {n.title}
                      </a>
                    </td>
                    <td className="py-2 pr-3 text-neutral-500">{extractHost(n.url)}</td>
                  </tr>
                ))}
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

/* ============ Charts ============ */

/** Overlay chart: purple bars (sentiment around zero) + green price line */
function OverlayChart({
  dates, price, sentiment, height = 520, width = 980,
}:{
  dates:string[]; price?:number[]; sentiment?:number[]; height?:number; width?:number;
}) {
  const pad = { t: 22, r: 64, b: 36, l: 44 };
  const W = width, H = height;
  const innerW = W - pad.l - pad.r, innerH = H - pad.t - pad.b;
  const n = dates.length, step = n > 1 ? innerW / (n - 1) : innerW;

  // Sentiment axis centered at 0
  const sMax = sentiment && sentiment.length ? Math.max(0.5, ...sentiment.map((x)=>Math.abs(x))) : 1;
  const sY   = (v:number) => pad.t + innerH/2 - (v / sMax) * (innerH/2);

  // Price axis on the right (min/max)
  const pMin = price && price.length ? Math.min(...price) : 0;
  const pMax = price && price.length ? Math.max(...price) : 1;
  const pY   = (v:number) => pad.t + (1 - (v - pMin) / Math.max(1e-9, pMax - pMin)) * innerH;

  const baselineY = sY(0);

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full rounded-xl border bg-white">
      {/* frame */}
      <rect x={pad.l} y={pad.t} width={innerW} height={innerH} fill="none" stroke="#e5e7eb" />
      {/* sentiment baseline */}
      {sentiment && sentiment.length ? (
        <line x1={pad.l} x2={pad.l+innerW} y1={baselineY} y2={baselineY} stroke="#e5e7eb" />
      ) : null}

      {/* sentiment bars */}
      {sentiment?.map((v, i) => {
        const x = pad.l + i * step;
        const y = Math.min(baselineY, sY(v));
        const h = Math.abs(sY(v) - baselineY);
        return <rect key={i} x={x - 1} y={y} width={2} height={Math.max(1, h)} fill="#6b47dc" opacity={0.7} />;
      })}

      {/* price line with markers */}
      {price && price.length > 1
        ? price.map((v, i) => {
            if (i === 0) return null;
            const x1 = pad.l + (i - 1) * step, y1 = pY(price[i - 1]);
            const x2 = pad.l + i * step,       y2 = pY(v);
            return <line key={`l${i}`} x1={x1} y1={y1} x2={x2} y2={y2} stroke="#10b981" strokeWidth={1.5} />;
          })
        : null}
      {price?.map((v, i) => {
        const cx = pad.l + i * step, cy = pY(v);
        return <circle key={`c${i}`} cx={cx} cy={cy} r={1.8} fill="#10b981" />;
      })}

      {/* simple labels: left = dates start/end; right = price ticks */}
      <text x={pad.l} y={H - 10} fontSize="11" fill="#6b7280">{dates[0] || ""}</text>
      <text x={pad.l + innerW - 54} y={H - 10} fontSize="11" fill="#6b7280" textAnchor="start">{dates.at(-1) || ""}</text>

      {/* right y-axis for price */}
      {price && price.length ? (
        <>
          {[pMin, (pMin+pMax)/2, pMax].map((pv, i) => {
            const y = pY(pv);
            return (
              <g key={i}>
                <line x1={pad.l+innerW} x2={pad.l+innerW+6} y1={y} y2={y} stroke="#e5e7eb" />
                <text x={pad.l+innerW+8} y={y+3} fontSize="11" fill="#6b7280">{pv.toFixed(3)}</text>
              </g>
            );
          })}
        </>
      ) : null}
    </svg>
  );
}

/** Separate sentiment chart (bars around zero) */
function SentimentBars({ dates, values, height = 300, width = 980 }:{
  dates:string[]; values:number[]; height?:number; width?:number;
}) {
  const pad = { t: 22, r: 24, b: 36, l: 44 };
  const W = width, H = height;
  const innerW = W - pad.l - pad.r, innerH = H - pad.t - pad.b;
  const n = dates.length, step = n > 1 ? innerW / (n - 1) : innerW;

  const sMax = values.length ? Math.max(0.5, ...values.map((x)=>Math.abs(x))) : 1;
  const sY   = (v:number) => pad.t + innerH/2 - (v / sMax) * (innerH/2);
  const baselineY = sY(0);

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full rounded-xl border bg-white">
      <rect x={pad.l} y={pad.t} width={innerW} height={innerH} fill="none" stroke="#e5e7eb" />
      <line x1={pad.l} x2={pad.l+innerW} y1={baselineY} y2={baselineY} stroke="#e5e7eb" />
      {values.map((v, i) => {
        const x = pad.l + i * step;
        const y = Math.min(baselineY, sY(v));
        const h = Math.abs(sY(v) - baselineY);
        return <rect key={i} x={x - 1} y={y} width={2} height={Math.max(1, h)} fill="#6b47dc" opacity={0.7} />;
      })}
      <text x={pad.l} y={H - 10} fontSize="11" fill="#6b7280">{dates[0] || ""}</text>
      <text x={pad.l + innerW - 54} y={H - 10} fontSize="11" fill="#6b7280" textAnchor="start">{dates.at(-1) || ""}</text>
    </svg>
  );
}

/** Separate price chart (line + markers) */
function PriceLine({ dates, values, height = 300, width = 980 }:{
  dates:string[]; values:number[]; height?:number; width?:number;
}) {
  const pad = { t: 22, r: 64, b: 36, l: 44 };
  const W = width, H = height;
  const innerW = W - pad.l - pad.r, innerH = H - pad.t - pad.b;
  const n = dates.length, step = n > 1 ? innerW / (n - 1) : innerW;

  const pMin = values.length ? Math.min(...values) : 0;
  const pMax = values.length ? Math.max(...values) : 1;
  const pY   = (v:number) => pad.t + (1 - (v - pMin) / Math.max(1e-9, pMax - pMin)) * innerH;

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full rounded-xl border bg-white">
      <rect x={pad.l} y={pad.t} width={innerW} height={innerH} fill="none" stroke="#e5e7eb" />
      {values.map((v, i) => {
        if (i === 0) return null;
        const x1 = pad.l + (i-1) * step, y1 = pY(values[i-1]);
        const x2 = pad.l + i * step,     y2 = pY(v);
        return <line key={`l${i}`} x1={x1} y1={y1} x2={x2} y2={y2} stroke="#10b981" strokeWidth={1.5} />;
      })}
      {values.map((v, i) => {
        const cx = pad.l + i * step, cy = pY(v);
        return <circle key={`c${i}`} cx={cx} cy={cy} r={1.8} fill="#10b981" />;
      })}
      {/* right axis ticks */}
      {[pMin, (pMin+pMax)/2, pMax].map((pv, i) => {
        const y = pY(pv);
        return (
          <g key={i}>
            <line x1={pad.l+innerW} x2={pad.l+innerW+6} y1={y} y2={y} stroke="#e5e7eb" />
            <text x={pad.l+innerW+8} y={y+3} fontSize="11" fill="#6b7280">{pv.toFixed(3)}</text>
          </g>
        );
      })}
      <text x={pad.l} y={H - 10} fontSize="11" fill="#6b7280">{dates[0] || ""}</text>
      <text x={pad.l + innerW - 54} y={H - 10} fontSize="11" fill="#6b7280" textAnchor="start">{dates.at(-1) || ""}</text>
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
