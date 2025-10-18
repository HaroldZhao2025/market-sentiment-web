// apps/web/components/LineChart.tsx
"use client";

import { SeriesIn } from "../lib/data";
import {
  LineChart as RCLineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";

export default function LineChart({ series, height = 360 }: { series: SeriesIn; height?: number }) {
  const { left, right, overlay } = series;

  // build a single array of points keyed by date
  const len = Math.min(left.dates.length, left.values.length, right.dates.length, right.values.length);
  const data = Array.from({ length: len }).map((_, i) => ({
    date: left.dates[i],
    price: left.values[i],
    s: right.values[i],
    sma: overlay?.values?.[i] ?? null,
  }));

  return (
    <div style={{ width: "100%", height }}>
      <ResponsiveContainer>
        <RCLineChart data={data} margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="date" minTickGap={30} />
          <YAxis yAxisId="L" orientation="left" />
          <YAxis yAxisId="R" orientation="right" />
          <Tooltip />
          <Line yAxisId="L" type="monotone" dataKey="price" dot={false} strokeWidth={1.6} />
          <Line yAxisId="R" type="monotone" dataKey="s" dot={false} strokeWidth={1.4} />
          {overlay ? <Line yAxisId="R" type="monotone" dataKey="sma" dot={false} strokeWidth={1.2} /> : null}
        </RCLineChart>
      </ResponsiveContainer>
    </div>
  );
}
