"use client";

import { useMemo, useState } from "react";

type Series = {
  label: string;
  values: number[];
  strokeClassName: string; // e.g. "stroke-blue-600"
  dotClassName?: string; // e.g. "fill-blue-600"
};

type Props = {
  dates: string[];
  series: Series[];
  height?: number;
  baselineValue?: number; // e.g. 1 for equity, 0 for drawdown
  valueFormat?: (v: number) => string;

  // NEW: labels
  yLabel?: string;
  showMinMaxLabels?: boolean;
};

function toPath(values: number[], w = 1000, h = 320, pad = 18, min = 0, max = 1) {
  const clean = values.map((v) => (Number.isFinite(v) ? v : NaN));
  const rng = max - min || 1;

  const x = (i: number) => (i / Math.max(1, clean.length - 1)) * w;
  const y = (v: number) => {
    const t = (v - min) / rng;
    return pad + (1 - t) * (h - 2 * pad);
  };

  let d = "";
  for (let i = 0; i < clean.length; i++) {
    const v = clean[i];
    if (!Number.isFinite(v)) continue;
    const cmd = d ? "L" : "M";
    d += `${cmd}${x(i).toFixed(2)},${y(v).toFixed(2)} `;
  }
  return d.trim();
}

export default function PortfolioChart({
  dates,
  series,
  height = 520,
  baselineValue,
  valueFormat,
  yLabel,
  showMinMaxLabels = true,
}: Props) {
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  const W = 1000;
  const H = Math.max(260, Math.floor(height));
  const pad = 22;

  const combined = useMemo(() => {
    const all = series
      .flatMap((s) => s.values)
      .filter((v) => Number.isFinite(v));

    const min = all.length ? Math.min(...all) : 0;
    const max = all.length ? Math.max(...all) : 1;
    return { min, max: max === min ? min + 1 : max };
  }, [series]);

  const scaleY = (v: number) => {
    const t = (v - combined.min) / (combined.max - combined.min);
    return pad + (1 - t) * (H - 2 * pad);
  };

  const scaleX = (i: number) => (i / Math.max(1, dates.length - 1)) * W;

  const paths = useMemo(() => {
    return series.map((s) => ({
      label: s.label,
      d: toPath(s.values, W, H, pad, combined.min, combined.max),
      strokeClassName: s.strokeClassName,
      dotClassName: s.dotClassName,
      values: s.values,
    }));
  }, [series, W, H, pad, combined.min, combined.max]);

  const onMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const px = e.clientX - rect.left;
    const t = px / rect.width;
    const idx = Math.round(t * (dates.length - 1));
    setHoverIdx(Math.max(0, Math.min(dates.length - 1, idx)));
  };

  const fmt = valueFormat ?? ((v: number) => (Number.isFinite(v) ? v.toFixed(4) : "—"));
  const hDate = hoverIdx != null ? dates[hoverIdx] : null;

  return (
    <div className="space-y-3">
      {/* legend + hover */}
      <div className="flex flex-col gap-2 md:flex-row md:items-center md:gap-4 text-sm text-neutral-700">
        <div className="flex flex-wrap items-center gap-3">
          {series.map((s) => (
            <div key={s.label} className="inline-flex items-center gap-2">
              <span className={`inline-block h-2 w-7 rounded ${s.strokeClassName.replace("stroke-", "bg-")}`} />
              <span className="font-medium">{s.label}</span>
            </div>
          ))}
        </div>

        <div className="md:ml-auto tabular-nums text-neutral-600">
          {hDate ? (
            <div className="flex flex-wrap items-center gap-x-4 gap-y-1">
              <span className="text-neutral-900 font-medium">{hDate}</span>
              {hoverIdx != null
                ? series.map((s) => {
                    const v = s.values[hoverIdx];
                    return Number.isFinite(v) ? (
                      <span key={s.label}>
                        {s.label}: <span className="text-neutral-900">{fmt(v)}</span>
                      </span>
                    ) : null;
                  })
                : null}
            </div>
          ) : (
            <span>Hover the chart</span>
          )}
        </div>
      </div>

      <div className="rounded-2xl border bg-white p-3">
        <svg
          viewBox={`0 0 ${W} ${H}`}
          className="w-full"
          onMouseMove={onMove}
          onMouseLeave={() => setHoverIdx(null)}
        >
          {/* subtle background */}
          <defs>
            <linearGradient id="bgFade" x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stopColor="rgb(248 250 252)" />
              <stop offset="100%" stopColor="rgb(255 255 255)" />
            </linearGradient>
          </defs>
          <rect x="0" y="0" width={W} height={H} fill="url(#bgFade)" />

          {/* grid */}
          <line x1="0" y1={pad} x2={W} y2={pad} className="stroke-neutral-100" strokeWidth="1" />
          <line x1="0" y1={H - pad} x2={W} y2={H - pad} className="stroke-neutral-100" strokeWidth="1" />
          <line
            x1="0"
            y1={(pad + (H - pad)) / 2}
            x2={W}
            y2={(pad + (H - pad)) / 2}
            className="stroke-neutral-100"
            strokeWidth="1"
          />

          {/* baseline */}
          {baselineValue != null && Number.isFinite(baselineValue) ? (
            <line
              x1="0"
              y1={scaleY(baselineValue)}
              x2={W}
              y2={scaleY(baselineValue)}
              className="stroke-neutral-200"
              strokeWidth="1.25"
            />
          ) : null}

          {/* axis labels */}
          {yLabel ? (
            <text x={14} y={18} className="fill-neutral-500" fontSize="12" fontFamily="ui-sans-serif, system-ui">
              {yLabel}
            </text>
          ) : null}

          {showMinMaxLabels ? (
            <>
              <text
                x={14}
                y={pad + 12}
                className="fill-neutral-400"
                fontSize="11"
                fontFamily="ui-sans-serif, system-ui"
              >
                max: {fmt(combined.max)}
              </text>
              <text
                x={14}
                y={H - pad - 6}
                className="fill-neutral-400"
                fontSize="11"
                fontFamily="ui-sans-serif, system-ui"
              >
                min: {fmt(combined.min)}
              </text>
            </>
          ) : null}

          {/* lines */}
          {paths.map((p) =>
            p.d ? <path key={p.label} d={p.d} fill="none" className={p.strokeClassName} strokeWidth="2.75" /> : null
          )}

          {/* hover */}
          {hoverIdx != null ? (
            <>
              <line
                x1={scaleX(hoverIdx)}
                y1={pad}
                x2={scaleX(hoverIdx)}
                y2={H - pad}
                className="stroke-neutral-200"
                strokeWidth="1"
              />
              {paths.map((p) => {
                const v = p.values[hoverIdx];
                if (!Number.isFinite(v)) return null;
                const dot = p.dotClassName ?? p.strokeClassName.replace("stroke-", "fill-");
                return <circle key={p.label} cx={scaleX(hoverIdx)} cy={scaleY(v)} r="5" className={dot} />;
              })}
            </>
          ) : null}
        </svg>
      </div>
    </div>
  );
}
