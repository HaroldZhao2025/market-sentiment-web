// apps/web/app/HomeClient.tsx
"use client";

import { useEffect, useState } from "react";
import Link from "next/link";

export default function HomeClient() {
  const [tickers, setTickers] = useState<string[] | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    // IMPORTANT: relative path – works with GitHub Pages basePath
    const url = "data/_tickers.json";
    fetch(url, { cache: "no-store" })
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((j) => {
        if (Array.isArray(j)) {
          setTickers(j);
        } else {
          setErr("Bad _tickers.json shape (expected array).");
        }
      })
      .catch((e) => setErr(e.message));
  }, []);

  return (
    <div className="max-w-6xl mx-auto px-4 py-8 space-y-6">
      <h1 className="text-2xl font-bold">Market Sentiment — S&amp;P 500</h1>

      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Browse tickers</h2>
        <Link href="/portfolio" className="text-sm underline hover:no-underline">
          Portfolio
        </Link>
      </div>

      {err && (
        <div className="text-sm text-red-600">
          Couldn’t load tickers: {err}
        </div>
      )}
      {!err && tickers === null && (
        <div className="text-sm text-neutral-500">Loading…</div>
      )}

      {Array.isArray(tickers) && tickers.length > 0 ? (
        <>
          <div className="text-xs text-neutral-500">
            Loaded {tickers.length} tickers from <code>data/_tickers.json</code>
          </div>
          <ul className="grid grid-cols-2 sm:grid-cols-4 md:grid-cols-6 lg:grid-cols-8 gap-2">
            {tickers.map((t) => (
              <li key={t}>
                <Link
                  href={`/ticker/${t}`}
                  className="block rounded-xl border border-neutral-200 px-3 py-2 text-center text-sm hover:bg-neutral-50"
                >
                  {t}
                </Link>
              </li>
            ))}
          </ul>
        </>
      ) : (
        !err && <div className="text-sm text-neutral-500">No data generated yet.</div>
      )}
    </div>
  );
}
