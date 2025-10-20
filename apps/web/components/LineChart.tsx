"use client";

import {
  ResponsiveContainer,
  ComposedChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
} from "recharts";

export default function LineChart({
  mode,
  dates,
  price,
  sentiment,
  sentimentMA7,
  height = 380,
}: {
  mode: "overlay" | "separate";
  dates: string[];
  price: number[];
  sentiment: number[];
  sentimentMA7: number[];
  height?: number;
}) {
  const data = dates.map((d, i) => ({
    d,
    p: price[i] ?? null,
    s: sentiment[i] ?? null,
    m: sentimentMA7[i] ?? null,
  }));

  const Chart = (
    <ResponsiveContainer width="100%" height={height}>
      <ComposedChart data={data} margin={{ top: 10, right: 20, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="d" tick={{ fontSize: 11 }} minTickGap={28} />
        <YAxis yAxisId="left" tick={{ fontSize: 11 }} />
        <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 11 }} />
        <Tooltip />
        {/* Sentiment as bars (left axis) */}
        <Bar yAxisId="left" dataKey="s" opacity={0.35} />
        {/* 7-day smoothed sentiment (left axis) */}
        <Line yAxisId="left" type="monotone" dataKey="m" dot={false} strokeWidth={2} />
        {/* Price (right axis) */}
        <Line yAxisId="right" type="monotone" dataKey="p" dot={false} strokeWidth={2} />
      </ComposedChart>
    </ResponsiveContainer>
  );

  if (mode === "overlay") return Chart;

  // "Separate" = stack two mini-charts with shared x
  return (
    <div className="space-y-6">
      <ResponsiveContainer width="100%" height={220}>
        <ComposedChart data={data} margin={{ top: 10, right: 20, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="d" tick={{ fontSize: 11 }} minTickGap={28} />
          <YAxis tick={{ fontSize: 11 }} />
          <Tooltip />
          <Bar dataKey="s" opacity={0.35} />
          <Line type="monotone" dataKey="m" dot={false} strokeWidth={2} />
        </ComposedChart>
      </ResponsiveContainer>

      <ResponsiveContainer width="100%" height={220}>
        <ComposedChart data={data} margin={{ top: 10, right: 20, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="d" tick={{ fontSize: 11 }} minTickGap={28} />
          <YAxis tick={{ fontSize: 11 }} />
          <Tooltip />
          <Line type="monotone" dataKey="p" dot={false} strokeWidth={2} />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}
