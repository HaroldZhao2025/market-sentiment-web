"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { assetPath } from "../lib/paths";

type Row = { ticker: string; S?: number; predicted_return?: number };

export default function HomeClient() {
  const [rows, setRows] = useState<Row[]>([]);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    fetch(assetPath("data/index.json"))
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`${r.status} ${r.statusText}`))))
      .then((d) => setRows(Array.isArray(d) ? d : []))
      .catch((e) => setErr(e.message));
  }, []);

  const top = useMemo(
    () => [...rows].sort((a, b) => (b.predicted_return ?? 0) - (a.predicted_return ?? 0)).slice(0, 20),
    [rows]
  );
  const bottom = useMemo(
    () => [...rows].sort((a, b) => (a.predicted_return ?? 0) - (b.predicted_return ?? 0)).slice(0, 20),
    [rows]
  );

  return (
    <div className="space-y-6">
      <header className="card flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold">Market Sentiment — S&amp;P 500</h1>
          <p className="text-sm text-gray-500">Daily aggregated news sentiment and price overlay</p>
        </div>
        <Link href="/portfolio" className="btn">Portfolio</Link>
      </header>

      {err && <div className="card text-red-700">Failed to load index.json: {err}</div>}

      <section className="card">
        <div className="flex items-center justify-between mb-3">
          <h2 className="font-semibold">Universe</h2>
          <div className="text-sm text-gray-500">{rows.length} tickers</div>
        </div>
        {rows.length === 0 ? (
          <div className="text-sm text-gray-500">No data yet.</div>
        ) : (
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-2">
            {rows.slice(0, 36).map((r) => (
              <Link key={r.ticker} href={`/ticker/${r.ticker}`} className="tile">
                <div className="tile-title">{r.ticker}</div>
                <div className="tile-sub">
                  {typeof r.predicted_return === "number"
                    ? `${(r.predicted_return * 100).toFixed(2)}%`
                    : "—"}
                </div>
              </Link>
            ))}
          </div>
        )}
      </section>

      <section className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="card">
          <h3 className="font-semibold mb-2">Top Predicted</h3>
          <Table rows={top} />
        </div>
        <div className="card">
          <h3 className="font-semibold mb-2">Bottom Predicted</h3>
          <Table rows={bottom} />
        </div>
      </section>
    </div>
  );
}

function Table({ rows }: { rows: Row[] }) {
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="text-left text-gray-500">
          <th className="py-1">Ticker</th>
          <th className="py-1">Predicted</th>
          <th className="py-1"></th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <tr key={r.ticker} className="border-t">
            <td className="py-2">{r.ticker}</td>
            <td className="py-2">
              {typeof r.predicted_return === "number" ? `${(r.predicted_return * 100).toFixed(2)}%` : "—"}
            </td>
            <td className="py-2">
              <Link href={`/ticker/${r.ticker}`} className="text-blue-600 underline">Open</Link>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
