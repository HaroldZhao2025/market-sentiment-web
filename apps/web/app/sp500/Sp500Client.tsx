"use client";

import { useMemo, useState } from "react";

export type SeriesIn = { date: string[]; price: number[]; sentiment: number[] };

type View = "separate" | "overlay" | "price" | "sentiment";

export default function Sp500Client({ series }: { series: SeriesIn }) {
  const [mode, setMode] = useState<View>("overlay");

  // ---- Clean & filter NaNs safely ----
  const cleaned = useMemo(() => {
    const n = Math.min(series.date?.length ?? 0, series.price?.length ?? 0, series.sentiment?.length ?? 0);
    const date = (series.date || []).slice(0, n);
    const price = (series.price || []).slice(0, n);
    const sentiment = (series.sentiment || []).slice(0, n);

    const idxPrice = Array.from({ length: n }, (_, i) => i).filter((i) => Number.isFinite(price[i]));
    const idxSent = Array.from({ length: n }, (_, i) => i).filter((i) => Number.isFinite(sentiment[i]));
    const idxBoth = Array.from({ length: n }, (_, i) => i).filter(
      (i) => Number.isFinite(price[i]) && Number.isFinite(sentiment[i])
    );

    const pick = (idxs: number[]) => ({
      date: idxs.map((i) => date[i]),
      price: idxs.map((i) => price[i]),
      sentiment: idxs.map((i) => sentiment[i]),
    });

    return {
      priceOnly: pick(idxPrice),
      sentOnly: pick(idxSent),
      both: pick(idxBoth),
    };
  }, [series]);

  const hasPrice = cleaned.priceOnly.date.length > 1;
  const hasSent = cleaned.sentOnly.date.length > 1;
  const hasBoth = cleaned.both.date.length > 1;

  // If overlay mode is selected but "both" is unavailable, fall back gracefully.
  const safeMode: View =
    mode === "overlay" && !hasBoth
      ? hasPrice
        ? "price"
        : "sentiment"
      : mode;

  return (
    <div className="space-y-4">
      {/* View selector (same feel as tickers) */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="inline-flex items-center rounded-xl border bg-white p-1" role="tablist" aria-label="SPX chart view">
          <SegButton active={safeMode === "separate"} onClick={() => setMode("separate")} label="Separate" />
          {hasBoth && <SegButton active={safeMode === "overlay"} onClick={() => setMode("overlay")} label="Overlay" />}
          {hasPrice && <SegButton active={safeMode === "price"} onClick={() => setMode("price")} label="Price" />}
          {hasSent && <SegButton active={safeMode === "sentiment"} onClick={() => setMode("sentiment")} label="Sentiment" />}
        </div>
      </div>

      {/* Chart card */}
      <div className="rounded-2xl p-6 shadow-sm border bg-white space-y-4">
        {safeMode === "separate" ? (
          <div className="space-y-6">
            <div className="space-y-2">
              <div className="text-sm font-medium text-neutral-700">SPX Sentiment</div>
              <OverlayChart
                dates={cleaned.sentOnly.date}
                sentiment={cleaned.sentOnly.sentiment}
                height={320}
              />
            </div>

            <div className="space-y-2">
              <div className="text-sm font-medium text-neutral-700">SPX Price</div>
              <OverlayChart
                dates={cleaned.priceOnly.date}
                price={cleaned.priceOnly.price}
                height={320}
              />
            </div>
          </div>
        ) : (
          <OverlayChart
            dates={
              safeMode === "overlay"
                ? cleaned.both.date
                : safeMode === "price"
                ? cleaned.priceOnly.date
                : cleaned.sentOnly.date
            }
            price={
              safeMode === "overlay"
                ? cleaned.both.price
                : safeMode === "price"
                ? cleaned.priceOnly.price
                : undefined
            }
            sentiment={
              safeMode === "overlay"
                ? cleaned.both.sentiment
                : safeMode === "sentiment"
                ? cleaned.sentOnly.sentiment
                : undefined
            }
            height={520}
          />
        )}

        <p className="text-xs text-neutral-500">
          Hover to see the exact date + value (price uses 2 decimals; sentiment uses 6 decimals).
        </p>
      </div>
    </div>
  );
}

/* ============ UI atoms ============ */
function SegButton({ active, onClick, label }: { active: boolean; onClick: () => void; label: string }) {
  return (
    <button
      role="tab"
      aria-selected={active}
      className={[
        "px-3 py-1.5 text-sm rounded-lg transition",
        active ? "bg-black text-white shadow-sm" : "text-neutral-700 hover:bg-neutral-50",
      ].join(" ")}
      onClick={onClick}
      type="button"
    >
      {label}
    </button>
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
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

function monthTicks(dates: string[]) {
  if (!dates.length) return [] as { i: number; label: string }[];
  const marks: { i: number; label: string }[] = [];
  let prevM = -1,
    prevY = -1;
  for (let i = 0; i < dates.length; i++) {
    const dt = parseISO(dates[i]),
      m = dt.getUTCMonth(),
      y = dt.getUTCFullYear();
    if (m !== prevM || y !== prevY) {
      marks.push({ i, label: `${MONTHS[m]}` });
      prevM = m;
      prevY = y;
    }
  }
  const maxLabels = 8;
  if (marks.length > maxLabels) {
    const stride = Math.ceil(marks.length / maxLabels);
    return marks.filter((_, idx) => idx % stride === 0);
  }
  return marks;
}

function toDateOnly(x: string) {
  const d = new Date(x);
  if (!isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  const m = String(x).match(/\d{4}-\d{2}-\d{2}/);
  return m ? m[0] : String(x).slice(0, 10);
}

function labelSent(v: number) {
  if (v >= 0.4) return "Strong Positive";
  if (v >= 0.1) return "Positive";
  if (v <= -0.4) return "Strong Negative";
  if (v <= -0.1) return "Negative";
  return "Neutral";
}

/* ============ Overlay chart (interactive tooltip in ALL modes) ============ */
function OverlayChart({
  dates,
  price,
  sentiment,
  height = 520,
  width = 980,
}: {
  dates: string[];
  price?: number[];
  sentiment?: number[];
  height?: number;
  width?: number;
}) {
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  const hasPrice = !!(price && price.length);
  const hasSent = !!(sentiment && sentiment.length);

  const pad = { t: 28, r: hasPrice ? 78 : 24, b: 44, l: hasSent ? 70 : 24 };
  const W = width,
    H = height;
  const innerW = W - pad.l - pad.r,
    innerH = H - pad.t - pad.b;

  const n = dates.length;
  if (n < 2 || innerW <= 0 || innerH <= 0) {
    return (
      <div className="text-sm text-neutral-500">
        Not enough data to render the chart.
      </div>
    );
  }

  const step = innerW / (n - 1);

  // Sentiment axis (centered at 0)
  const sMax = hasSent ? Math.max(0.5, ...(sentiment as number[]).map((x) => Math.abs(x))) : 1;
  const sY = (v: number) => pad.t + innerH / 2 - (v / sMax) * (innerH / 2);
  const sTicks = hasSent ? [-sMax, -sMax / 2, 0, sMax / 2, sMax] : [];

  // Price axis (right)
  const pMin = hasPrice ? Math.min(...(price as number[])) : 0;
  const pMax = hasPrice ? Math.max(...(price as number[])) : 1;
  const pY = (v: number) =>
    pad.t + (1 - (v - pMin) / Math.max(1e-9, pMax - pMin)) * innerH;
  const pTicks = hasPrice ? [pMin, (pMin + pMax) / 2, pMax] : [];

  const baselineY = hasSent ? sY(0) : null;
  const monthMarks = monthTicks(dates);

  // Mouse/touch -> index mapping
  const idxFromX = (px: number) => {
    const raw = Math.round((px - pad.l) / Math.max(1e-9, step));
    return Math.max(0, Math.min(n - 1, raw));
  };
  const onMoveClientX = (clientX: number, svgRect: DOMRect) => {
    const x = clientX - svgRect.left;
    if (x < pad.l || x > pad.l + innerW) return;
    setHoverIdx(idxFromX(x));
  };

  const onMouseMove: React.MouseEventHandler<SVGRectElement> = (e) => {
    onMoveClientX(e.clientX, (e.currentTarget.ownerSVGElement as SVGSVGElement).getBoundingClientRect());
  };
  const onTouchMove: React.TouchEventHandler<SVGRectElement> = (e) => {
    const t = e.touches?.[0];
    if (!t) return;
    onMoveClientX(t.clientX, (e.currentTarget.ownerSVGElement as SVGSVGElement).getBoundingClientRect());
  };
  const onLeave = () => setHoverIdx(null);

  function renderTooltip(i: number) {
    const x = pad.l + i * step;
    const dateStr = toDateOnly(dates[i] ?? "");

    const lines: string[] = [dateStr];

    if (hasPrice && price) lines.push(`Price: ${Number(price[i] ?? 0).toFixed(2)}`);
    if (hasSent && sentiment) {
      const sVal = Number(sentiment[i] ?? 0);
      lines.push(`Sentiment: ${labelSent(sVal)} (${sVal.toFixed(6)})`);
    }

    const padBox = 8;
    const lineH = 16;
    const textW = Math.max(...lines.map((s) => s.length)) * 7; // rough
    const boxW = Math.min(Math.max(140, textW + padBox * 2), 320);
    const boxH = lines.length * lineH + padBox * 2;

    const toRight = x < W / 2;
    const bx = toRight ? Math.min(x + 12, W - boxW - 6) : Math.max(6, x - boxW - 12);
    const by = Math.max(pad.t + 6, Math.min(pad.t + innerH - boxH - 6, pad.t + 10));

    return (
      <g>
        {/* vertical crosshair */}
        <line
          x1={x}
          x2={x}
          y1={pad.t}
          y2={pad.t + innerH}
          stroke="#9ca3af"
          strokeDasharray="3,3"
        />

        {/* markers */}
        {hasPrice && price ? (
          <circle cx={x} cy={pY(price[i])} r={3.5} fill="#10b981" stroke="white" strokeWidth={1.2} />
        ) : null}
        {hasSent && sentiment ? (
          <circle cx={x} cy={sY(sentiment[i])} r={3.5} fill="#6b47dc" stroke="white" strokeWidth={1.2} />
        ) : null}

        {/* tooltip box */}
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

      {/* sentiment axis (left) */}
      {hasSent
        ? sTicks.map((v, i) => {
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
          })
        : null}

      {hasSent ? (
        <text
          x={16}
          y={pad.t + innerH / 2}
          fontSize="12"
          fill="#374151"
          transform={`rotate(-90, 16, ${pad.t + innerH / 2})`}
          textAnchor="middle"
        >
          Sentiment
        </text>
      ) : null}

      {/* price axis (right) */}
      {hasPrice
        ? pTicks.map((v, i) => {
            const y = pY(v);
            return (
              <g key={`p${i}`}>
                <line x1={pad.l + innerW} x2={pad.l + innerW + 6} y1={y} y2={y} stroke="#e5e7eb" />
                <text x={pad.l + innerW + 8} y={y + 3} fontSize="11" fill="#6b7280">
                  {v.toFixed(2)}
                </text>
              </g>
            );
          })
        : null}

      {hasPrice ? (
        <text
          x={W - 18}
          y={pad.t + innerH / 2}
          fontSize="12"
          fill="#374151"
          transform={`rotate(90, ${W - 18}, ${pad.t + innerH / 2})`}
          textAnchor="middle"
        >
          Price
        </text>
      ) : null}

      {/* sentiment baseline */}
      {hasSent && baselineY !== null ? (
        <line x1={pad.l} x2={pad.l + innerW} y1={baselineY} y2={baselineY} stroke="#e5e7eb" />
      ) : null}

      {/* sentiment bars */}
      {hasSent && sentiment
        ? sentiment.map((v, i) => {
            const x = pad.l + i * step;
            const y = Math.min(sY(0), sY(v));
            const h = Math.abs(sY(v) - sY(0));
            return <rect key={`sb${i}`} x={x - 1} y={y} width={2} height={Math.max(1, h)} fill="#6b47dc" opacity={0.7} />;
          })
        : null}

      {/* price line */}
      {hasPrice && price && price.length > 1
        ? price.map((v, i) => {
            if (i === 0) return null;
            const x1 = pad.l + (i - 1) * step,
              y1 = pY(price[i - 1]);
            const x2 = pad.l + i * step,
              y2 = pY(v);
            return <line key={`pl${i}`} x1={x1} y1={y1} x2={x2} y2={y2} stroke="#10b981" strokeWidth={2.5} />;
          })
        : null}

      {hasPrice && price
        ? price.map((v, i) => {
            const cx = pad.l + i * step,
              cy = pY(v);
            return <circle key={`pc${i}`} cx={cx} cy={cy} r={2.2} fill="#10b981" />;
          })
        : null}

      {/* month tick labels */}
      {monthMarks.map(({ i, label }, k) => {
        const x = pad.l + i * step;
        return (
          <g key={`m${k}`}>
            <line x1={x} x2={x} y1={pad.t + innerH} y2={pad.t + innerH + 5} stroke="#e5e7eb" />
            <text x={x} y={H - 10} fontSize="11" fill="#6b7280" textAnchor="middle">
              {label}
            </text>
          </g>
        );
      })}

      {/* hit-rect + tooltip */}
      <g>
        <rect
          x={pad.l}
          y={pad.t}
          width={innerW}
          height={innerH}
          fill="transparent"
          onMouseMove={onMouseMove}
          onMouseLeave={onLeave}
          onTouchStart={onTouchMove}
          onTouchMove={onTouchMove}
        />
        {hoverIdx !== null ? renderTooltip(hoverIdx) : null}
      </g>
    </svg>
  );
}
