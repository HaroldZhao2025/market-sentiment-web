"use client";

import { useEffect, useState } from "react";
import Link from "next/link";

const BASE = process.env.NEXT_PUBLIC_BASE_PATH || "";

type PortfolioJSON = {
  dates: string[];
  long: number[];
  short: number[];
  long_short: number[];
};

export default function PortfolioClient() {
  const [data, setData] = useState<PortfolioJSON | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    const url = `${BASE}/data/portfolio.json`;
    fetch(url, { cache: "no-store" })
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
        return r.json();
      })
      .then(setData)
      .catch((e) => setErr(e.message));
  }, []);

  return (
    <div className="max-w-5xl mx-auto px-4 py-8 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Portfolio</h1>
        <Link href="/" className="text-sm underline hover:no-underline">← Home</Link>
      </div>

      {err && <div className="text-sm text-red-600">Failed to load: {err}</div>}
      {!data && !err && <div className="text-sm text-neutral-500">Loading…</div>}
      {data && data.dates?.length === 0 && <div className="text-sm text-neutral-500">No data.</div>}

      {data && data.dates?.length > 0 && (
        <div className="text-sm">
          Points: <b>{data.dates.length}</b> — long/short series loaded from <code>{BASE}/data/portfolio.json</code>
        </div>
      )}
    </div>
  );
}
