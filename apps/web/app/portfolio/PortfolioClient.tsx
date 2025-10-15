"use client";

import { useEffect, useState } from "react";
import { assetPath } from "../../lib/paths"; // up two levels from app/portfolio to lib

export default function PortfolioClient() {
  const [data, setData] = useState<any | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    fetch(assetPath("data/portfolio.json"))
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`${r.status} ${r.statusText}`))))
      .then(setData)
      .catch((e) => setErr(e.message));
  }, []);

  if (err) return <div className="card text-red-700">Failed to load portfolio.json: {err}</div>;
  if (!data) return <div className="card">Loading…</div>;

  return (
    <div className="space-y-6">
      <div className="card">
        <h1 className="text-xl font-semibold mb-2">Portfolio Backtest</h1>
        <p className="text-sm text-gray-500 mb-3">Toy long/short using daily sentiment scores</p>
        <div className="w-full overflow-x-auto">
          <SimpleLine dates={data.date} values={data.equity} />
        </div>
      </div>
    </div>
  );
}

function SimpleLine({ dates, values }: { dates: string[]; values: number[] }) {
  const w = 800, h = 200, pad = 20;
  const xs = dates.map((_, i) => pad + (i / Math.max(1, dates.length - 1)) * (w - pad * 2));
  const min = Math.min(...values), max = Math.max(...values);
  const ys = values.map(v => h - pad - ((v - min) / Math.max(1e-9, (max - min))) * (h - pad * 2));
  return (
    <svg viewBox={`0 0 ${w} ${h}`} className="w-full h-56">
      <polyline points={xs.map((x,i)=>`${x},${ys[i]}`).join(" ")} fill="none" stroke="currentColor" strokeWidth="2" />
    </svg>
  );
}
