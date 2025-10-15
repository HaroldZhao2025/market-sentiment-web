"use client";

import {
  Chart as ChartJS,
  CategoryScale, LinearScale, PointElement, LineElement, Tooltip, Legend,
  type ChartOptions, type ChartData,
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
  label = "Sentiment",
}: {
  dates: string[];
  price: number[];
  sentiment: number[];
  sentiment_ma7?: number[];
  overlay?: boolean;
  label?: string;
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
        borderColor: "rgb(33, 150, 243)",
        pointRadius: 0,
      },
    ];
    const s = sentiment ?? [];
    const s7 = sentiment_ma7 ?? [];
    ds.push({
      label: `${label} ${s7.length ? "(MA7)" : ""}`,
      data: s7.length ? s7 : s,
      yAxisID: "y1",
      borderWidth: 2,
      borderDash: s7.length ? [] : [4, 4],
      tension: 0.2,
      borderColor: "rgb(76, 175, 80)",
      pointRadius: 0,
    });
    return { labels, datasets: ds };
  }, [dates, price, sentiment, sentiment_ma7, label]);

  const options = useMemo<ChartOptions<"line">>(() => ({
    responsive: true,
    interaction: { mode: "index" as const, intersect: false },
    plugins: { legend: { display: true } },
    scales: {
      y: { type: "linear", display: true, position: "left",
        ticks: { callback: (v: unknown) => typeof v === "number" ? `$${v.toFixed(0)}` : String(v) } },
      y1: { type: "linear", display: true, position: "right", grid: { drawOnChartArea: false },
        suggestedMin: -3, suggestedMax: 3, title: { display: true, text: "Sentiment" },
        ticks: { callback: (v: unknown) => typeof v === "number" ? v.toFixed(1) : String(v) } },
      x: { display: true },
    },
  }), []);

  return <Line options={options} data={data} />;
}
