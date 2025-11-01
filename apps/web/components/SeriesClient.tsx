"use client";

import { useMemo, useState } from "react";

/* --------- Props --------- */
export type SeriesIn = { date: string[]; price: number[]; sentiment: number[] };
type View = "overlay" | "price" | "sentiment" | "separate";

export default function SeriesClient({
  title,
  series,
}: {
  title: string;
  series: SeriesIn;
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

  // Simple daily price change %
  const priceChange = useMemo(() => {
    const p = aligned.price;
    if (p.length < 2) return 0;
    const prev = Number(p[p.length - 2] || 0);
    const cur  = Number(p[p.length - 1] || 0);
    if (!isFinite(prev) || prev === 0) return 0;
    return ((cur - prev) / prev) * 100;
  }, [aligned.price]);

  return (
    <div className="mx-auto max-w-6xl px-4 py-8 space-y-8">
      {/* Header + View selector */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-3xl font-bold tracking-tight">{title}</h1>
        <div className="inline-flex items-center rounded-xl border bg-white p-1" role="tablist" aria-label="Chart view">
          <SegButton active={mode === "separate"} onClick={() => setMode("separate")}  label="Separate View" />
          <SegButton active={mode === "overlay"}  onClick={() => setMode("overlay")}   label="Overlayed View" />
          {hasPrice && <SegButton active={mode === "price"} onClick={() => setMode("price")} label="Price Only" />}
          {hasSent  && <SegButton active={mode === "sentiment"} onClick={() => setMode("sentiment")} label="Sentiment Only" />}
        </div>
      </div>

      {/* Chart card(s) */}
      <div className="rounded-2xl p-6 shadow-sm border bg-white space-y-6">
        <h3 className="font-semibold">Sentiment and Price</h3>

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

      {/* KPIs */}
      <section className="rounded-2xl p-6 shadow-sm border bg-white space-y-4">
        <h3 className="font-semibold">Overview</h3>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <KpiCard title="Latest Sentiment" value={label(lastS)} sub="(daily score)" bigValue={lastS.toFixed(4)} />
          <KpiCard title="7-day Avg Sentiment" value={label(lastMA)} bigValue={lastMA.toFixed(4)} />
          <KpiCard title="Price Δ (day)" value={`${priceChange >= 0 ? "+" : ""}${priceChange.toFixed(2)}%`} />
          <KpiCard title="Advisory" value={recommendation(lastMA)} sub="Based on trend" />
        </div>
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
function toDateOnly(x: string) {
  const d = new Date(x);
  if (!isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  const m = String(x).match(/\d{4}-\d{2}-\d{2}/);
  return m ? m[0] : String(x).slice(0, 10);
}
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

/* ============ Overlay chart with hover (single-series tooltips only) ============ */
function OverlayChart({
  dates, price, sentiment, height = 520, width = 980,
}:{
  dates:string[]; price?:number[]; sentiment?:number[]; height?:number; width?:number;
}) {
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

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

  const isPriceOnly = !!(price && price.length) && !(sentiment && sentiment.length);
  const isSentOnly  = !!(sentiment && sentiment.length) && !(price && price.length);
  const interactive = (isPriceOnly || isSentOnly) && n > 0 && innerW > 0;

  function idxFromX(px: number) {
    const raw = Math.round((px - pad.l) / Math.max(1e-9, step));
    return Math.max(0, Math.min(n - 1, raw));
  }
  function onMoveClientX(clientX: number, svgRect: DOMRect) {
    const x = clientX - svgRect.left;
    if (x < pad.l || x > pad.l + innerW) return;
    setHoverIdx(idxFromX(x));
  }
  const onMouseMove: React.MouseEventHandler<SVGRectElement> = (e) => {
    if (!interactive) return;
    onMoveClientX(e.clientX, (e.currentTarget.ownerSVGElement as SVGSVGElement).getBoundingClientRect());
  };
  const onTouchMove: React.TouchEventHandler<SVGRectElement> = (e) => {
    if (!interactive) return;
    const t = e.touches?.[0];
    if (!t) return;
    onMoveClientX(t.clientX, (e.currentTarget.ownerSVGElement as SVGSVGElement).getBoundingClientRect());
  };
  const onLeave = () => setHoverIdx(null);

  function renderTooltip(i: number) {
    const x = pad.l + i * step;
    const dateStr = toDateOnly(dates[i] ?? "");
    let lines: string[] = [dateStr];

    if (isPriceOnly && price) {
      lines.push(`Price: ${Number(price[i] ?? 0).toFixed(2)}`);
    } else if (isSentOnly && sentiment) {
      const sVal = Number(sentiment[i] ?? 0);
      lines.push(`Sentiment: ${label(sVal)} (${sVal.toFixed(4)})`);
    } else {
      return null;
    }

    const padBox = 8;
    const lineH = 16;
    const textW = Math.max(...lines.map((s) => s.length)) * 7;
    const boxW = Math.min(Math.max(120, textW + padBox * 2), 280);
    const boxH = lines.length * lineH + padBox * 2;

    const toRight = x < W / 2;
    const bx = toRight ? Math.min(x + 12, W - boxW - 6) : Math.max(6, x - boxW - 12);
    const by = Math.max(pad.t + 6, Math.min(pad.t + innerH - boxH - 6, pad.t + 10));

    return (
      <g>
        <line x1={x} x2={x} y1={pad.t} y2={pad.t + innerH} stroke="#9ca3af" strokeDasharray="3,3" />
        {isPriceOnly && price ? (
          <circle cx={x} cy={pY(price[i])} r={3.5} fill="#10b981" stroke="white" strokeWidth={1.2} />
        ) : null}
        {isSentOnly && sentiment ? (
          <circle cx={x} cy={sY(sentiment[i])} r={3.5} fill="#6b47dc" stroke="white" strokeWidth={1.2} />
        ) : null}

        <rect x={bx} y={by} rx={8} ry={8} width={boxW} height={boxH} fill="white" stroke="#e5e7eb" />
        {lines.map((t, k) => (
          <text key={k} x={bx + padBox} y={by + padBox + 12 + k * lineH} fontSize="12" fill="#374151">
            {t}
          </text>
        ))}
      </g>
    );
  }

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full rounded-xl border bg-white">
      {/* frame */}
      <rect x={pad.l} y={pad.t} width={innerW} height={innerH} fill="none" stroke="#e5e7eb" />

      {/* left y-axis (sentiment) ticks + labels */}
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
      {/* left axis label */}
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

      {/* right y-axis (price) ticks + labels */}
      {pTicks.map((v, i) => {
        const y = pY(v);
        return (
          <g key={`p${i}`}>
            <line x1={pad.l+innerW} x2={pad.l+innerW+6} y1={y} y2={y} stroke="#e5e7eb" />
            <text x={pad.l+innerW+8} y={y + 3} fontSize="11" fill="#6b7280">{v.toFixed(2)}</text>
          </g>
        );
      })}
      {/* right axis label */}
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

      {/* sentiment baseline */}
      {sentiment && sentiment.length ? (
        <line x1={pad.l} x2={pad.l+innerW} y1={sY(0)} y2={sY(0)} stroke="#e5e7eb" />
      ) : null}

      {/* sentiment bars */}
      {sentiment?.map((v, i) => {
        const x = pad.l + i * step;
        const y = Math.min(sY(0), sY(v));
        const h = Math.abs(sY(v) - sY(0));
        return <rect key={i} x={x - 1} y={y} width={2} height={Math.max(1, h)} fill="#6b47dc" opacity={0.7} />;
      })}

      {/* price line */}
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

      {/* month tick labels along x-axis */}
      {monthMarks.map(({ i, label }, k) => {
        const x = pad.l + i * step;
        return (
          <g key={`m${k}`}>
            <line x1={x} x2={x} y1={pad.t + innerH} y2={pad.t + innerH + 5} stroke="#e5e7eb" />
            <text x={x} y={H - 10} fontSize="11" fill="#6b7280" textAnchor="middle">{label}</text>
          </g>
        );
      })}

      {/* interactive layer for single-series modes */}
      {(() => {
        const singlePrice = !!(price && price.length) && !(sentiment && sentiment.length);
        const singleSent  = !!(sentiment && sentiment.length) && !(price && price.length);
        const interactive = (singlePrice || singleSent) && n > 0 && innerW > 0;
        if (!interactive) return null;

        const onMove = (clientX: number, rect: DOMRect) => {
          const x = clientX - rect.left;
          if (x < pad.l || x > pad.l + innerW) return;
          const idx = Math.round((x - pad.l) / Math.max(1e-9, step));
          setHoverIdx(Math.max(0, Math.min(n - 1, idx)));
        };

        const renderTooltip = (i: number) => {
          const x = pad.l + i * step;
          const dateStr = toDateOnly(dates[i] ?? "");
          const lines: string[] = [dateStr];
          if (singlePrice && price) lines.push(`Price: ${Number(price[i] ?? 0).toFixed(2)}`);
          if (singleSent  && sentiment) {
            const sVal = Number(sentiment[i] ?? 0);
            lines.push(`Sentiment: ${label(sVal)} (${sVal.toFixed(4)})`);
          }
          const padBox = 8, lineH = 16;
          const textW = Math.max(...lines.map((s) => s.length)) * 7;
          const boxW = Math.min(Math.max(120, textW + padBox * 2), 280);
          const boxH = lines.length * lineH + padBox * 2;
          const toRight = x < W / 2;
          const bx = toRight ? Math.min(x + 12, W - boxW - 6) : Math.max(6, x - boxW - 12);
          const by = Math.max(pad.t + 6, Math.min(pad.t + innerH - boxH - 6, pad.t + 10));

          return (
            <g>
              <line x1={x} x2={x} y1={pad.t} y2={pad.t + innerH} stroke="#9ca3af" strokeDasharray="3,3" />
              {singlePrice && price ? (
                <circle cx={x} cy={pY(price[i])} r={3.5} fill="#10b981" stroke="white" strokeWidth={1.2} />
              ) : null}
              {singleSent && sentiment ? (
                <circle cx={x} cy={sY(sentiment[i])} r={3.5} fill="#6b47dc" stroke="white" strokeWidth={1.2} />
              ) : null}

              <rect x={bx} y={by} rx={8} ry={8} width={boxW} height={boxH} fill="white" stroke="#e5e7eb" />
              {lines.map((t, k) => (
                <text key={k} x={bx + padBox} y={by + padBox + 12 + k * lineH} fontSize="12" fill="#374151">
                  {t}
                </text>
              ))}
            </g>
          );
        };

        return (
          <g>
            <rect
              x={pad.l}
              y={pad.t}
              width={innerW}
              height={innerH}
              fill="transparent"
              onMouseMove={(e) => onMove(e.clientX, (e.currentTarget.ownerSVGElement as SVGSVGElement).getBoundingClientRect())}
              onMouseLeave={() => setHoverIdx(null)}
              onTouchStart={(e) => {
                const t = e.touches?.[0]; if (!t) return;
                onMove(t.clientX, (e.currentTarget.ownerSVGElement as SVGSVGElement).getBoundingClientRect());
              }}
              onTouchMove={(e) => {
                const t = e.touches?.[0]; if (!t) return;
                onMove(t.clientX, (e.currentTarget.ownerSVGElement as SVGSVGElement).getBoundingClientRect());
              }}
            />
            {hoverIdx !== null ? renderTooltip(hoverIdx) : null}
          </g>
        );
      })()}
    </svg>
  );
}

/* ============ Separate charts (with month ticks & y labels) ============ */
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
