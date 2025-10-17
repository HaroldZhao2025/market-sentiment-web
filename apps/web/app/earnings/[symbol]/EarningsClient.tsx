"use client";
import { useEffect, useState } from "react";
import Link from "next/link";

const BASE = process.env.NEXT_PUBLIC_BASE_PATH || "";

type Row = { ts: string; title: string; url: string; S?: number };

export default function EarningsClient({ symbol }: { symbol: string }) {
  const [rows, setRows] = useState<Row[] | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    const url = `${BASE}/data/earnings/${symbol}.json`;
    fetch(url, { cache: "no-store" })
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
        return r.json();
      })
      .then((j) => (Array.isArray(j) ? setRows(j) : setErr("Bad earnings json shape")))
      .catch((e) => setErr(e.message));
  }, [symbol]);

  return (
    <div className="max-w-5xl mx-auto px-4 py-8 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Earnings — {symbol}</h1>
        <Link href={`/ticker/${symbol}/`} className="text-sm underline hover:no-underline">← {symbol}</Link>
      </div>

      {err && <div className="text-sm text-red-600">Failed to load: {err}</div>}
      {!rows && !err && <div className="text-sm text-neutral-500">Loading…</div>}

      {rows && rows.length === 0 && <div className="text-sm text-neutral-500">No earnings docs in range.</div>}

      {rows && rows.length > 0 && (
        <ul className="space-y-2">
          {rows.map((r, i) => (
            <li key={i} className="border rounded-xl p-3">
              <div className="text-xs text-neutral-500">{r.ts}</div>
              <a href={r.url} target="_blank" className="text-sm underline">{r.title}</a>
              {"S" in r && <div className="text-xs">S: {r.S}</div>}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
