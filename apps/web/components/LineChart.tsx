// apps/web/components/LineChart.tsx
"use client";

import { useMemo } from "react";
import {
  Chart as ChartJS,
  LinearScale,
  TimeScale,
  LineElement,
  PointElement,
  Legend,
  Tooltip,
  CategoryScale,
} from "chart.js";
import { Line } from "react-chartjs-2";

ChartJS.register(LinearScale, TimeScale, LineElement, PointElement, Legend, Tooltip, CategoryScale);

export type Series = {
  label: string;
  dates: string[];       // x-axis labels
  left?: number[];       // price or sentiment on left axis
  right?: number[];      // optional right axis series
  overlay?: number[];    // optional thin overlay (e.g., MA)
};

export default function LineChart({
  series,
  height = 280,
}: {
  series: Series;
  height?: number;
}) {
  const data = useMemo(() => {
    const ds: any[] = [];
    if (series.left) {
      ds.push({
        type: "line",
        label: series.label,
        data: series.left,
        yAxisID: "y",
        pointRadius: 0,
        borderWidth: 1.5,
      });
    }
    if (series.right) {
      ds.push({
        type: "line",
        label: "Right",
        data: series.right,
        yAxisID: "y1",
        pointRadius: 0,
        borderWidth: 1.2,
      });
    }
    if (series.overlay) {
      ds.push({
        type: "line",
        label: "Overlay",
        data: series.overlay,
        yAxisID: "y",
        pointRadius: 0,
        borderWidth: 1,
        borderDash: [4, 4],
      });
    }
    return { labels: series.dates, datasets: ds };
  }, [series]);

  const options = useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "nearest" as const, intersect: false },
      plugins: { legend: { display: false } },
      scales: {
        x: { display: true },
        y: {
          display: true,
          position: "left" as const,
          ticks: { callback: (v: any) => `${v}` },
        },
        y1: {
          display: !!series.right,
          position: "right" as const,
          grid: { drawOnChartArea: false },
          ticks: { callback: (v: any) => `${v}` },
        },
      },
    }),
    [series.right],
  );

  return (
    <div style={{ height }}>
      <Line data={data} options={options} />
    </div>
  );
}
