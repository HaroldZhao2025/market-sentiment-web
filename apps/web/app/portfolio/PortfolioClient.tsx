// apps/web/app/portfolio/PortfolioClient.tsx
"use client";

import { useEffect, useState } from "react";
import { dataUrl } from "../../lib/paths";
import {
  ResponsiveContainer,
  LineChart,
  CartesianGrid,
  Line,
  XAxis,
  YAxis,
  Tooltip,
} from "recharts";

type PortfolioJSON = {
  dates: string[];
  equity?: number[];
  value?: number[];
};

export default function PortfolioClient() {
  const [data, setData] = useState<{ d: string; v: number }[]>([]);

  useEffect(() => {
    fetch(dataUrl("portfolio.json"), { cache: "no-cache" })
      .then((r) => r.json())
      .then((obj: PortfolioJSON) => {
        const v = obj.equity ?? obj.value ?? [];
        const rows = (obj.dates || []).map((d, i) => ({
          d,
          v: typeof v[i] === "number" ? v[i] : Number(v[i] ?? 0) || 0,
        }));
        setData(rows);
      })
      .catch(() => setData([]));
  }, []);

  return (
    <div className="max-w-6xl mx-auto p-6">
      <h1 className="text-2xl font-semibold mb-4">Portfolio</h1>
      <div className="rounded-2xl p-4 shadow-sm border bg-white">
        {data.length ? (
          <ResponsiveContainer width="100%" height={420}>
            <LineChart data={data} margin={{ top: 12, right: 24, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="d" tick={{ fontSize: 11 }} minTickGap={28} />
              <YAxis tick={{ fontSize: 11 }} width={56} />
              <Tooltip />
              <Line type="monotone" dataKey="v" name="Value" dot={false} strokeWidth={2} />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <div className="text-neutral-500">No portfolio data.</div>
        )}
      </div>
    </div>
  );
}
