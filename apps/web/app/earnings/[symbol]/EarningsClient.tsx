"use client";

import { useEffect, useState } from "react";
import Link from "next/link";

export default function EarningsClient({ symbol }: { symbol: string }) {
  const s = symbol.toUpperCase();
  const [data, setData] = useState<any | null>(null);

  useEffect(() => {
    fetch(`data/earnings/${s}.json`)
      .then((r) => (r.ok ? r.json() : null))
      .then(setData)
      .catch(() => setData(null));
  }, [s]);

  return (
    <div className="space-y-6">
      <div className="card flex items-center justify-between">
        <h2 className="text-xl font-semibold">Earnings Sentiment — {s}</h2>
        <Link href={`/ticker/${s}`} className="btn">Back</Link>
      </div>
      {!data ? (
        <div className="card">No transcripts available.</div>
      ) : (
        <div className="card">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-gray-500">
                <th className="py-1">Date</th>
                <th className="py-1">Quarter</th>
                <th className="py-1">Year</th>
                <th className="py-1">Excerpt</th>
              </tr>
            </thead>
            <tbody>
              {data.events?.map((e: any, i: number) => (
                <tr key={i} className="border-t">
                  <td className="py-2">
                    {new Date(e.ts).toLocaleDateString()}
                  </td>
                  <td className="py-2">{e.quarter || "-"}</td>
                  <td className="py-2">{e.year || "-"}</td>
                  <td className="py-2">{(e.text || "").slice(0, 160)}…</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
