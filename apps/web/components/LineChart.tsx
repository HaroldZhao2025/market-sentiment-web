// apps/web/components/LineChart.tsx
"use client";

import { useMemo } from "react";
import { Line } from "react-chartjs-2";
import {
  Chart as ChartJS,
  LineElement,
  PointElement,
  LinearScale,
  TimeSeriesScale,
  CategoryScale,
  Tooltip,
  Legend,
} from "chart.js";

ChartJS.register(LineElement, PointElement, LinearScale, TimeSeriesScale, CategoryScale, Tooltip, Legend);

type Series = { x: string | number | Date; y: number }[];

export default function LineChart({
  left,
  right,
  overlay,
  height = 280,
}: {
  left?: Series;
  right?: Series;
  overlay?: Series;
  height?: number;
}) {
  const empty: Series = [];
  const L = Array.isArray(left) ? left : empty;
  const R = Array.isArray(right) ? right : empty;
  const O = Array.isArray(overlay) ? overlay : empty;

  const labels = (L.length ? L : R).map((p) => p.x);

  const data = useMemo(
    () => ({
      labels,
      datasets: [
        {
          label: "Close",
          data: L.map((p) => p.y),
          borderWidth: 1.5,
          tension: 0.2,
          yAxisID: "y",
        },
        {
          label: "Signal S",
          data: R.map((p) => p.y),
          borderWidth: 1.2,
          borderDash: [4, 3],
          tension: 0.2,
          yAxisID: "y1",
        },
        ...(O.length
          ? [
              {
                label: "Overlay",
                data: O.map((p) => p.y),
                borderWidth: 1,
                borderDash: [2, 2],
                tension: 0.2,
                yAxisID: "y",
              },
            ]
          : []),
      ],
    }),
    [labels, L, R, O]
  );

  const options = useMemo(
    () => ({
      responsive: true,
      interaction: { mode: "index" as const, intersect: false },
      plugins: { legend: { display: true } },
      scales: {
        y: {
          type: "linear" as const,
          display: true,
          position: "left" as const,
          ticks: {
            callback: (v: any) => (typeof v === "number" ? v.toFixed(2) : v),
          },
        },
        y1: {
          type: "linear" as const,
          display: true,
          position: "right" as const,
          grid: { drawOnChartArea: false },
          ticks: {
            callback: (v: any) => (typeof v === "number" ? v.toFixed(2) : v),
          },
        },
        x: { type: "category" as const },
      },
    }),
    []
  );

  if (!labels.length) {
    return (
      <div className="w-full border rounded p-6 text-sm text-gray-500" style={{ height }}>
        No series available.
      </div>
    );
  }

  return <Line options={options} data={data} height={height} />;
}

