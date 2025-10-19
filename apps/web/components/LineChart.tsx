"use client";

import React from "react";
import {
  LineChart as RCLineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  CartesianGrid,
  ResponsiveContainer,
} from "recharts";

export type SeriesIn = {
  date: string[];
  price: number[];
  sentiment: number[];
  sentiment_ma7?: number[];
  priceLabel?: string;
  sentimentLabel?: string;
};

type Props = {
  series: SeriesIn;
  height?: number; // default 380
};

export default function LineChart({ series, height = 380 }: Props) {
  // Build recharts row data
  const len = Math.min(
    series.date.length,
    series.price.length,
    series.sentiment.length
  );
  const rows = Array.from({ length: len }).map((_, i) => ({
    d: series.date[i],
    price: series.price[i],
    s: series.sentiment[i],
    s7: series.sentiment_ma7?.[i] ?? null,
  }));

  // IMPORTANT: parent must have explicit height for ResponsiveContainer
  return (
    <div className="w-full" style={{ height }}>
      <ResponsiveContainer width="100%" height="100%">
        <RCLineChart data={rows} margin={{ top: 10, right: 24, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="d" minTickGap={24} />
          <YAxis
            yAxisId="left"
            orientation="left"
            tickFormatter={(v) => (Math.abs(v) >= 1 ? v.toFixed(1) : v.toFixed(2))}
          />
          <YAxis
            yAxisId="right"
            orientation="right"
            tickFormatter={(v) => v.toFixed(0)}
            allowDecimals={false}
          />
          <Tooltip />
          <Legend />
          <Line
            yAxisId="left"
            type="monotone"
            dataKey="s"
            name={series.sentimentLabel ?? "Daily Sentiment"}
            dot={false}
            strokeWidth={1.5}
          />
          {series.sentiment_ma7 && (
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="s7"
              name="Sentiment MA(7)"
              dot={false}
              strokeWidth={2}
            />
          )}
          <Line
            yAxisId="right"
            type="monotone"
            dataKey="price"
            name={series.priceLabel ?? "Close"}
            dot={false}
            strokeWidth={1.5}
          />
        </RCLineChart>
      </ResponsiveContainer>
    </div>
  );
}
