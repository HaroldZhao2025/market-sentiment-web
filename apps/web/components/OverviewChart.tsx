// apps/web/components/OverviewChart.tsx
"use client";

import {
  LineChart as RC,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";

export default function OverviewChart({
  dates,
  sentiment,
  price,
  height = 380,
}: {
  dates: string[];
  sentiment: number[];
  price?: number[];
  height?: number;
}) {
  const data = dates.map((d, i) => ({
    d,
    s: Number.isFinite(sentiment[i]) ? sentiment[i] : null,
    p: price && Number.isFinite(price[i]) ? price[i] : null,
  }));

  return (
    <div style={{ width: "100%", height }}>
      <ResponsiveContainer width="100%" height="100%">
        <RC data={data} margin={{ top: 12, right: 20, bottom: 6, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="d" tick={{ fontSize: 11 }} minTickGap={28} />
          {/* Sentiment axis fixed to [-1, 1] */}
          <YAxis
            yAxisId="left"
            domain={[-1, 1]}
            allowDataOverflow={false}
            tick={{ fontSize: 11 }}
          />
          {/* Price axis auto */}
          <YAxis
            yAxisId="right"
            orientation="right"
            tick={{ fontSize: 11 }}
            allowDecimals
          />
          <ReferenceLine yAxisId="left" y={0} strokeOpacity={0.3} />
          <Tooltip />
          {/* Sentiment line */}
          <Line
            yAxisId="left"
            type="monotone"
            dataKey="s"
            dot={false}
            strokeWidth={2}
            strokeOpacity={0.9}
            connectNulls
            name="Sentiment"
          />
          {/* Price line (if provided) */}
          {price && (
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="p"
              dot={false}
              strokeWidth={2}
              connectNulls
              name="Price"
            />
          )}
        </RC>
      </ResponsiveContainer>
    </div>
  );
}
