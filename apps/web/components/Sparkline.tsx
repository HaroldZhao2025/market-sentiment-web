// apps/web/components/Sparkline.tsx
"use client";

import { useMemo } from "react";

type Props = {
  data: number[];
  width?: number;
  height?: number;
  strokeWidth?: number;
  className?: string;
  baselineZero?: boolean;
};

function finiteNumbers(arr: number[]) {
  return arr.filter((v) => Number.isFinite(v));
}

export default function Sparkline({
  data,
  width = 720,
  height = 180,
  strokeWidth = 2,
  className = "",
  baselineZero = true,
}: Props) {
  const { points, yZero } = useMemo(() => {
    const xs = data.map((_, i) => i);
    const ys = data;

    const ysFinite = finiteNumbers(ys);
    const minY = ysFinite.length ? Math.min(...ysFinite) : 0;
    const maxY = ysFinite.length ? Math.max(...ysFinite) : 1;
    const spanY = maxY - minY || 1;

    const pad = 10;
    const w = width - pad * 2;
    const h = height - pad * 2;

    const toX = (i: number) => pad + (xs.length <= 1 ? 0 : (i / (xs.length - 1)) * w);
    const toY = (v: number) => pad + (maxY - v) / spanY * h;

    const pts = ys.map((v, i) => {
      const y = Number.isFinite(v) ? toY(v) : NaN;
      return `${toX(i)},${y}`;
    });

    const yZeroPx =
      baselineZero && minY <= 0 && maxY >= 0 ? toY(0) : null;

    return {
      points: pts.join(" "),
      yZero: yZeroPx,
    };
  }, [data, width, height, baselineZero]);

  return (
    <svg
      width="100%"
      viewBox={`0 0 ${width} ${height}`}
      className={className}
      role="img"
      aria-label="sparkline"
    >
      <rect x="0" y="0" width={width} height={height} fill="transparent" />
      {yZero !== null && (
        <line
          x1="0"
          x2={width}
          y1={yZero}
          y2={yZero}
          stroke="currentColor"
          strokeOpacity="0.15"
          strokeWidth="1"
        />
      )}
      <polyline
        fill="none"
        stroke="currentColor"
        strokeOpacity="0.9"
        strokeWidth={strokeWidth}
        strokeLinejoin="round"
        strokeLinecap="round"
        points={points}
      />
    </svg>
  );
}
