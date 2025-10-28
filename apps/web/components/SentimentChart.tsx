"use client";

import * as React from "react";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid
} from "recharts";

type Item = {
  date: string;     // YYYY-MM-DD
  sentiment?: number;
  sentiment_ma7?: number;
  price?: number;
};

export default function SentimentChart({ data }: { data: Item[] }) {
  // Formatters
  const fmtPct = (v: any) => (v === null || v === undefined) ? "" : `${Number(v).toFixed(4)}`;
  const fmtPx  = (v: any) => (v === null || v === undefined) ? "" : `${Number(v).toFixed(2)}`;

  return (
    <div className="w-full rounded-2xl shadow-sm border p-4 bg-white">
      <div className="text-lg font-semibold mb-2">Live Market Sentiment</div>
      <div className="text-xs text-gray-500 mb-3">Sentiment (left axis, 4 d.p.) · Price (right axis)</div>
      <div style={{ width: "100%", height: 360 }}>
        <ResponsiveContainer>
          <LineChart data={data} margin={{ top: 8, right: 28, left: 8, bottom: 8 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="date" minTickGap={24} />
            <YAxis
              yAxisId="left"
              tickFormatter={fmtPct}
              width={60}
            />
            <YAxis
              yAxisId="right"
              orientation="right"
              tickFormatter={fmtPx}
              width={60}
            />
            <Tooltip
              formatter={(value: any, name: string) => {
                if (name.startsWith("Sentiment")) return [fmtPct(value), name];
                if (name === "Price") return [fmtPx(value), name];
                return [value, name];
              }}
              labelFormatter={(label) => `Date: ${label}`}
            />
            <Legend verticalAlign="top" height={32} />
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="sentiment"
              name="Sentiment"
              dot={false}
              strokeWidth={2.25}
            />
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="sentiment_ma7"
              name="Sentiment (MA7)"
              dot={false}
              strokeDasharray="5 3"
              strokeWidth={2}
            />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="price"
              name="Price"
              dot={false}
              strokeWidth={1.75}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
