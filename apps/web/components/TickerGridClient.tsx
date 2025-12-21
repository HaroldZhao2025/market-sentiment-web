"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import { hrefs } from "../lib/paths";

export type TickerRow = {
  ticker: string;
  price: number | null;
  sentiment: number | null;
  dailyReturn: number | null; // computed from last 2 prices
};

type Props = {
  rows: TickerRow[];
};

type SortKey = "Alphabet" | "Sentiment" | "Return";

function fmtPrice(x: number | null) {
  if (x == null || !Number.isFinite(x)) return "—";
  return x.toFixed(2);
}

function fmtSent(x: number | null) {
  if (x == null || !Number.isFinite(x)) return "—";
  // compact like 0.20
  return x.toFixed(2);
}

function signColor(v: number | null) {
  if (v == null || !Number.isFinite(v)) return "text-neutral-500";
  if (v > 0) return "text-emerald-600";
  if (v < 0) return "text-rose-600";
  return "text-neutral-600";
}

export default function TickerGridClient({ rows }: Props) {
  const [sort, setSort] = useState<SortKey>("Alphabet");

  const sorted = useMemo(() => {
    const arr = [...rows];

    const numOrNegInf = (x: number | null) => (x == null || !Number.isFinite(x) ? -Infinity : x);

    if (sort === "Alphabet") {
      arr.sort((a, b) => a.ticker.localeCompare(b.ticker));
    } else if (sort === "Sentiment") {
      arr.sort((a, b) => numOrNegInf(b.sentiment) - numOrNegInf(a.sentiment));
    } else if (sort === "Return") {
      arr.sort((a, b) => numOrNegInf(b.dailyReturn) - numOrNegInf(a.dailyReturn));
    }

    return arr;
  }, [rows, sort]);

  const btn = (k: SortKey) =>
    `rounded-full px-3 py-1 text-xs font-semibold border transition ${
      sort === k
        ? "bg-neutral-900 text-white border-neutral-900"
        : "bg-white text-neutral-700 border-neutral-200 hover:bg-neutral-50"
    }`;

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-4 flex-wrap">
        <div className="text-sm text-neutral-600">
          Showing <span className="font-semibold text-neutral-900">{rows.length}</span> tickers
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs text-neutral-500">Sort:</span>
          <button className={btn("Alphabet")} onClick={() => setSort("Alphabet")}>
            Alphabet
          </button>
          <button className={btn("Sentiment")} onClick={() => setSort("Sentiment")}>
            Sentiment
          </button>
          <button className={btn("Return")} onClick={() => setSort("Return")}>
            Return
          </button>
        </div>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-2">
        {sorted.map((r) => {
          const priceColor = signColor(r.dailyReturn); // green if daily return > 0, red if < 0
          const sentColor = signColor(r.sentiment); // green if sentiment > 0, red if < 0

          return (
            <Link
              key={r.ticker}
              href={hrefs.ticker(r.ticker)}
              className="px-3 py-2 rounded-xl bg-white hover:bg-neutral-50 border text-sm flex items-center justify-between"
              title={`Open ${r.ticker}`}
            >
              <div className="flex items-baseline gap-2">
                <span className="font-semibold text-neutral-900">{r.ticker}</span>
                <span className="text-neutral-500">(</span>
                <span className={`tabular-nums ${priceColor}`}>{fmtPrice(r.price)}</span>
                <span className="text-neutral-500">,</span>
                <span className={`tabular-nums ${sentColor}`}>{fmtSent(r.sentiment)}</span>
                <span className="text-neutral-500">)</span>
              </div>

              <span className="text-xs text-neutral-400">→</span>
            </Link>
          );
        })}
      </div>

      <div className="text-xs text-neutral-500">
        Price color reflects <b>latest daily return</b>. Sentiment color reflects <b>sentiment sign</b>.
      </div>
    </div>
  );
}
