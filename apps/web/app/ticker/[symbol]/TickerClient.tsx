"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import LineChart from "../../../components/LineChart";
import { assetPath } from "../../../lib/paths";

function fmtPct(x?: number) { return typeof x === "number" ? `${(x*100).toFixed(2)}%` : "—"; }
function fmtNum(x?: number, d=2) { return typeof x === "number" ? x.toFixed(d) : "—"; }

export default function TickerClient({ symbol }: { symbol: string }) {
  const s = symbol.toUpperCase();
  const [data, setData] = useState<any | null>(null);
  const [overlay, setOverlay] = useState(true);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    fetch(assetPath(`data/${s}.json`))
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error("No data"))))
      .then(setData)
      .catch((e) => setErr(e.message));
  }, [s]);

  const kpis = useMemo(() => {
    if (!data?.series) return { S1: 0, S7: 0, pred: 0, news7: 0 };
    const S1 = data?.meta?.S_1d ?? 0;
    const S7 = data?.meta?.S_7d ?? 0;
    const pred = data?.insights?.predicted_return ?? 0;
    const news7 = data?.meta?.news_7d ?? 0;
    return { S1, S7, pred, news7 };
  }, [data]);

  if (err) return <div className="card">Error: {err}</div>;
  if (!data) return <div className="card">Loading…</div>;

  return (
    <div className="space-y-6">
      <div className="card flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold">Market Sentiment — {s}</h2>
          <div className="text-xs text-gray-500">Updated: {new Date(data?.meta?.last_updated ?? Date.now()).toLocaleString()}</div>
        </div>
        <div className="flex gap-2">
          <button className="btn" onClick={() => setOverlay(false)}>Separate View</button>
          <button className="btn" onClick={() => setOverlay(true)}>Overlayed View</button>
          <Link href={`/earnings/${s}`} className="btn">Earnings</Link>
        </div>
      </div>

      <div className="card">
        <h3 className="font-semibold mb-3">Price & Sentiment</h3>
        <LineChart
          dates={data.series.date}
          price={data.series.price}
          sentiment={data.series.sentiment}
          sentiment_ma7={data.series.sentiment_ma7}
          overlay={overlay}
        />
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="card">
          <div className="kpi">{fmtNum(kpis.S1, 2)}</div>
          <div className="kpi-sub">Daily Sentiment (S)</div>
        </div>
        <div className="card">
          <div className="kpi">{fmtNum(kpis.S7, 2)}</div>
          <div className="kpi-sub">7-day Avg Sentiment</div>
        </div>
        <div className="card">
          <div className="kpi">{fmtPct(kpis.pred)}</div>
          <div className="kpi-sub">Predicted Next-Day Return</div>
        </div>
        <div className="card">
          <div className="kpi">{kpis.news7}</div>
          <div className="kpi-sub">News Count (7d)</div>
        </div>
      </div>

      <div className="card">
        <h3 className="font-semibold mb-3">Recent Headlines</h3>
        <div className="divide-y">
          {data.recent_headlines?.map((h: any, i: number) => (
            <div key={i} className="py-2 flex items-start gap-3">
              <div className="w-48 text-xs text-gray-500">{new Date(h.ts).toLocaleString()}</div>
              <div className="flex-1">
                <a className="text-blue-600 underline" href={h.url} target="_blank" rel="noreferrer">
                  {h.title}
                </a>
                <div className="text-xs text-gray-500">{h.source}</div>
              </div>
            </div>
          ))}
          {(!data.recent_headlines || data.recent_headlines.length === 0) && (
            <div className="text-sm text-gray-500">No recent headlines found.</div>
          )}
        </div>
      </div>
    </div>
  );
}
