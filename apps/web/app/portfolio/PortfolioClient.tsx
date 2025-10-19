// apps/web/app/portfolio/PortfolioClient.tsx
"use client";

import { useEffect, useState } from "react";
import {
  LineChart as RC,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
} from "recharts";
import { dataUrl } from "../../lib/paths";

type PortfolioJSON = {
  dates: string[];
  S: number[];
  S_MA7: number[];
  count?: number[];
};

export default function PortfolioClient() {
  const [data, setData] = useState<PortfolioJSON | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    const url = dataUrl("portfolio.json");
    fetch(url, { cache: "no-cache" })
      .then((r) => (r.ok ? r.json() : Promise.reject(r.statusText)))
      .then((j: PortfolioJSON) => setData(j))
      .catch((e) => setErr(String(e)));
  }, []);

  const rows =
    data?.dates?.map((d, i) => ({
      d,
      s: data.S?.[i] ?? null,
      m: data.S_MA7?.[i] ?? null,
    })) ?? [];

  return (
    <main className="max-w-6xl mx-auto p-6 space-y-6">
      <h1 className="text-3xl font-semibold tracking-tight">Market Sentiment — S&amp;P 500 Portfolio</h1>

      {!data || rows.length === 0 ? (
        <div className="text-neutral-500">
          {err ? `Failed to load portfolio: ${err}` : "No portfolio data yet."}
        </div>
      ) : (
        <div className="rounded-2xl p-4 shadow-sm border">
          <h3 className="font-semibold mb-3">Equal-Weight Daily Sentiment</h3>
          <ResponsiveContainer width="100%" height={380}>
            <RC data={rows} margin={{ top: 10, right: 20, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="d" tick={{ fontSize: 11 }} minTickGap={28} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip />
              <Line yAxisId={0} type="monotone" dataKey="s" dot={false} strokeWidth={1.5} strokeOpacity={0.45} />
              <Line yAxisId={0} type="monotone" dataKey="m" dot={false} strokeWidth={2} />
            </RC>
          </ResponsiveContainer>
        </div>
      )}
    </main>
  );
}
