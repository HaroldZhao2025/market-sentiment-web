// apps/web/components/LineChart.tsx
"use client";

import {
  LineChart as RC,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
  Area,
  Legend,
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

  const separate = mode === "separate";

  return (
    <ResponsiveContainer width="100%" height={height}>
      <RC data={data} margin={{ top: 12, right: 24, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="d" tick={{ fontSize: 11 }} minTickGap={28} />
        <YAxis
          yAxisId="left"
          tick={{ fontSize: 11 }}
          width={48}
          hide={!separate}
        />
        <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 11 }} />
        <Tooltip />
        <Legend wrapperStyle={{ fontSize: 12 }} />

        {/* Sentiment (area) */}
        <Area
          yAxisId={separate ? "left" : "right"}
          type="monotone"
          dataKey="s"
          name="Sentiment"
          fillOpacity={0.15}
          strokeOpacity={0.35}
          strokeWidth={1.5}
        />
        {/* Sentiment MA7 */}
        <Line
          yAxisId={separate ? "left" : "right"}
          type="monotone"
          dataKey="m"
          name="Sentiment MA(7)"
          dot={false}
          strokeWidth={2}
        />
        {/* Price */}
        <Line
          yAxisId="right"
          type="monotone"
          dataKey="p"
          name="Stock Price"
          dot={false}
          strokeWidth={2}
        />
      </RC>
    </ResponsiveContainer>
  );
}
