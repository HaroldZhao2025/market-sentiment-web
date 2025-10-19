"use client";

import {
  LineChart as RC,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
} from "recharts";

export default function LineChart({
  mode,
  dates,
  price,
  sentiment,
  sentimentMA7,
  height = 360,
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

  return (
    <ResponsiveContainer width="100%" height={height}>
      <RC data={data} margin={{ top: 10, right: 20, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="d" tick={{ fontSize: 11 }} minTickGap={28} />
        <YAxis yAxisId="left" tick={{ fontSize: 11 }} />
        <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 11 }} />
        <Tooltip />
        {/* sentiment bars/line */}
        <Line
          yAxisId="left"
          type="monotone"
          dataKey="s"
          dot={false}
          strokeOpacity={0.4}
          strokeWidth={1.5}
        />
        <Line
          yAxisId="left"
          type="monotone"
          dataKey="m"
          dot={false}
          strokeWidth={2}
        />
        {/* price */}
        <Line
          yAxisId="right"
          type="monotone"
          dataKey="p"
          dot={false}
          strokeWidth={2}
        />
      </RC>
    </ResponsiveContainer>
  );
}
