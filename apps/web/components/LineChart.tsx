"use client";

import {
  Chart as ChartJS,
  CategoryScale, LinearScale, PointElement, LineElement, Tooltip, Legend,
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
  const data = useMemo(() => {
    const labels = dates.map((d) => new Date(d).toLocaleDateString());
    const ds: any[] = [
      {
        label: "Price",
        data: price,
        yAxisID: "y",
        borderWidth: 2,
        tension: 0.2,
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
        borderDash: s7.length ? [0] : [4, 4],
        tension: 0.2,
      });
    }
    return { labels, datasets: ds };
  }, [dates, price, sentiment, sentiment_ma7, overlay]);

  const options = useMemo(() => ({
    responsive: true,
    interaction: { mode: "index", intersect: false },
    plugins: { legend: { display: true } },
    scales: {
      y: { type: "linear", display: true, position: "left", ticks: { callback: (v: any) => `$${v}` } },
      y1: {
        type: "linear",
        display: overlay,
        position: "right",
        grid: { drawOnChartArea: false },
        suggestedMin: -3,
        suggestedMax: 3,
        title: { display: true, text: "Sentiment (std-ish)" },
      },
      x: { display: true },
    },
  }), [overlay]);

  return <Line options={options} data={data} />;
}
