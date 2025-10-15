"use client";

import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  Legend,
  type ChartOptions,
  type ChartData,
} from "chart.js";
import { Line } from "react-chartjs-2";
import { useMemo } from "react";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Tooltip, Legend);

export default function LineChart({
  dates,
  price,
  sentiment,
  sentiment_ma7,
  overlay = true,
}: {
  dates: string[];
  price: number[];
  sentiment: number[];
  sentiment_ma7?: number[];
  overlay?: boolean;
}) {
  const data = useMemo<ChartData<"line">>(() => {
    const labels = dates.map((d) => new Date(d).toLocaleDateString());

    const ds: ChartData<"line">["datasets"] = [
      {
        label: "Price",
        data: price,
        yAxisID: "y",
        borderWidth: 2,
        tension: 0.2,
        borderColor: "rgb(33, 150, 243)", // blue
        pointRadius: 0,
      },
    ];

    const s = sentiment ?? [];
    const s7 = sentiment_ma7 ?? [];
    if (overlay) {
      ds.push({
        label: "Sentiment (MA7)",
        data: s7.length ? s7 : s,
        yAxisID: "y1",
        borderWidth: 2,
        borderDash: s7.length ? [] : [4, 4],
        tension: 0.2,
        borderColor: "rgb(76, 175, 80)", // green
        pointRadius: 0,
      });
    }

    return { labels, datasets: ds };
  }, [dates, price, sentiment, sentiment_ma7, overlay]);

  const options = useMemo<ChartOptions<"line">>(
    () => ({
      responsive: true,
      interaction: { mode: "index" as const, intersect: false },
      plugins: {
        legend: { display: true },
        tooltip: {
          callbacks: {
            label(ctx) {
              const y = ctx.raw as number;
              const label = ctx.dataset.label || "";
              if (ctx.dataset.yAxisID === "y") {
                return `${label}: $${(y ?? 0).toFixed(2)}`;
              }
              return `${label}: ${(y ?? 0).toFixed(2)}`;
            },
          },
        },
      },
      scales: {
        y: {
          type: "linear",
          display: true,
          position: "left",
          ticks: {
            callback: (val: unknown) =>
              typeof val === "number" ? `$${val.toFixed(0)}` : String(val),
          },
        },
        y1: {
          type: "linear",
          display: overlay,
          position: "right",
          grid: { drawOnChartArea: false },
          suggestedMin: -3,
          suggestedMax: 3,
          title: { display: true, text: "Sentiment (std-ish)" },
          ticks: {
            callback: (val: unknown) =>
              typeof val === "number" ? val.toFixed(1) : String(val),
          },
        },
        x: { display: true },
      },
    }),
    [overlay]
  );

  return <Line options={options} data={data} />;
}
