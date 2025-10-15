"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import LineChart from "../../../components/LineChart";

export default function TickerClient({ symbol }: { symbol: string }) {
  const s = symbol.toUpperCase();
  const [data, setData] = useState<any | null>(null);
  const [overlay, setOverlay] = useState(true);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    fetch(`data/${s}.json`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error("No data"))))
      .then(setData)
      .catch((e) => setErr(e.message));
  }, [s]);

  if (err) return <div className="card">Error: {err}</div>;
  if (!data) return <div className="card">Loading…</div>;

  const ins = data.insights || {};

  return (
    <div className="space-y-6">
      <div className="card flex items-center justify-between">
        <h2 className="text-xl font-semibold">Market Sentiment for {s}</h2>
        <div className="flex gap-2">
          <button className="btn" onClick={() => setOverlay(false)}>
            Separate View
          </button>
          <button className="btn" onClick={() => setOverlay(true)}>
            Overlayed View
          </button>
          <Link href={`/earnings/${s}`} className="btn">
            Earnings
          </Link>
        </div>
      </div>

      <div className="card">
        <h3 className="font-semibold mb-3">Sentiment and Price Analysis</h3>
        <LineChart
          dates={data.series.date}
          price={data.series.price}
          sentiment={data.series.sentiment}
          overlay={overlay}
        />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="card">
          <div className="kpi">
            {ins.live_sentiment || "—"}
          </div>
          <div className="kpi-sub">Live Market Sentiment</div>
        </div>
        <div className="card">
          <div className="kpi">
            {typeof ins.predicted_return === "number"
              ? (ins.predicted_return * 100).toFixed(2) + "%"
              : "—"}
          </div>
          <div className="kpi-sub">Predicted Return</div>
        </div>
        <div className="card">
          <div className="kpi">{ins.advisory || "—"}</div>
          <div className="kpi-sub">Our Recommendation</div>
        </div>
      </div>

      <div className="card">
        <h3 className="font-semibold mb-3">Recent Headlines for {s}</h3>
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-gray-500">
              <th className="py-1">Date Time</th>
              <th className="py-1">Headline</th>
              <th className="py-1">Source</th>
            </tr>
          </thead>
          <tbody>
            {data.recent_headlines?.map((h: any, i: number) => (
              <tr key={i} className="border-t">
                <td className="py-2">
                  {new Date(h.ts).toLocaleString()}
                </td>
                <td className="py-2">
                  <a className="text-blue-600 underline" href={h.url} target="_blank">
                    {h.title}
                  </a>
                </td>
                <td className="py-2">{h.source}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
