// apps/web/components/OverviewChart.tsx
"use client";

import {
  ResponsiveContainer,
  ComposedChart,
  Area,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceLine,
} from "recharts";

export default function OverviewChart({
  dates,
  sentiment,
  price,
  height = 320,
}: {
  dates: string[];
  sentiment: number[];
  price?: number[];
  height?: number;
}) {
  const data = dates.map((d, i) => ({
    d,
    s: typeof sentiment[i] === "number" ? sentiment[i] : null,
    p: Array.isArray(price) && typeof price[i] === "number" ? price[i] : null,
  }));

  return (
    <div style={{ width: "100%", height }}>
      <ResponsiveContainer>
        <ComposedChart data={data} margin={{ top: 12, right: 24, bottom: 0, left: 8 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="d" tick={{ fontSize: 11 }} minTickGap={28} />
          <YAxis yAxisId="left" domain={[-1, 1]} tick={{ fontSize: 11 }} />
          <YAxis
            yAxisId="right"
            orientation="right"
            tick={{ fontSize: 11 }}
            hide={!(price && price.some((x) => typeof x === "number"))}
          />
          <Tooltip />
          <Legend />
          <ReferenceLine yAxisId="left" y={0} strokeOpacity={0.4} />
          <Area yAxisId="left" type="monotone" dataKey="s" name="Market Sentiment" connectNulls />
          <Line yAxisId="right" type="monotone" dataKey="p" name="Index Price" dot={false} strokeWidth={2} connectNulls />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}
