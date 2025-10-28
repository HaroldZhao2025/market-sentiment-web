"use client";
import React from "react";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid,
} from "recharts";

type Props = {
  dates: string[];
  S: number[];
  S_ma7?: number[];
};

export default function SentimentChart({ dates, S, S_ma7 = [] }: Props) {
  const data = dates.map((d, i) => ({
    date: d,
    S: typeof S[i] === "number" ? Number(S[i].toFixed(4)) : null,
    S_ma7: typeof S_ma7[i] === "number" ? Number(S_ma7[i].toFixed(4)) : null,
  }));

  return (
    <div className="w-full rounded-2xl shadow p-4 bg-white">
      <h3 className="text-lg font-semibold mb-2">Live Market Sentiment</h3>
      <ResponsiveContainer width="100%" height={320}>
        <LineChart data={data} margin={{ top: 10, right: 24, bottom: 8, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="date" tick={{ fontSize: 12 }} />
          <YAxis domain={[-1, 1]} tick={{ fontSize: 12 }} />
          <Tooltip />
          <Legend verticalAlign="top" height={32} />
          <Line
            type="monotone"
            dataKey="S"
            name="Sentiment"
            stroke="#4f46e5"
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
          />
          <Line
            type="monotone"
            dataKey="S_ma7"
            name="Sentiment (MA7)"
            stroke="#22c55e"
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
      <p className="mt-2 text-xs text-gray-500">
        Scale: −1 (very negative) to +1 (very positive). Values rounded to 4 decimals.
      </p>
    </div>
  );
}
