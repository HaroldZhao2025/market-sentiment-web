"use client";

import { useEffect, useState, useMemo } from "react";
import {
  ResponsiveContainer,
  LineChart as RC,
  Line,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ReferenceLine,
  Legend,
} from "recharts";

type Props = {
  mode: "overlay" | "separate";
  dates: string[];
  price?: number[];
  sentiment: number[];
  sentimentMA7?: number[];
  height?: number; // total height of the overlay chart (each split chart uses ~55%)
};

const fmtDate = (d: string) => {
  try {
    const o = new Date(d + "T00:00:00Z");
    return o.toLocaleDateString(undefined, { month: "short", day: "2-digit" });
  } catch {
    return d;
  }
};

function toRows(dates: string[], price?: number[], s?: number[], m?: number[]) {
  const n = Math.max(dates.length, price?.length ?? 0, s?.length ?? 0, m?.length ?? 0);
  const safe = (v: any) => (Number.isFinite(Number(v)) ? Number(v) : null);
  return Array.from({ length: n }, (_, i) => ({
    d: dates[i] ?? "",
    p: safe(price?.[i]),
    s: safe(s?.[i]),
    m: safe(m?.[i]),
  }));
}

function Overlay({ rows }: { rows: any[] }) {
  return (
    <RC data={rows} margin={{ top: 8, right: 20, bottom: 8, left: 8 }}>
      <CartesianGrid strokeDasharray="3 3" />
      <XAxis dataKey="d" tick={{ fontSize: 11 }} minTickGap={28} tickFormatter={fmtDate} />
      <YAxis yAxisId="left" domain={[-1, 1]} allowDataOverflow tick={{ fontSize: 11 }} tickCount={5} />
      <YAxis
        yAxisId="right"
        orientation="right"
        tick={{ fontSize: 11 }}
        tickFormatter={(v) => (v == null ? "" : Number(v).toFixed(0))}
      />
      <Tooltip
        labelFormatter={(l) => `Date: ${l}`}
        formatter={(value: any, name) => {
          if (name === "Sentiment" || name === "Sentiment MA(7)") return [Number(value).toFixed(2), name];
          if (name === "Price") return [Number(value).toFixed(2), name];
          return [value, name];
        }}
      />
      <Legend />
      <Area name="Sentiment" yAxisId="left" type="monotone" dataKey="s" dot={false} strokeOpacity={0.55} fillOpacity={0.15} />
      <Line name="Sentiment MA(7)" yAxisId="left" type="monotone" dataKey="m" dot={false} strokeWidth={2} />
      <ReferenceLine y={0} yAxisId="left" strokeOpacity={0.35} />
      <Line name="Price" yAxisId="right" type="monotone" dataKey="p" dot={false} strokeWidth={2} />
    </RC>
  );
}

function Single({ rows, which }: { rows: any[]; which: "sentiment" | "price" }) {
  const isSent = which === "sentiment";
  return (
    <RC data={rows} margin={{ top: 8, right: 20, bottom: 8, left: 8 }}>
      <CartesianGrid strokeDasharray="3 3" />
      <XAxis dataKey="d" tick={{ fontSize: 11 }} minTickGap={28} tickFormatter={fmtDate} />
      <YAxis
        domain={isSent ? [-1, 1] : ["auto", "auto"]}
        tick={{ fontSize: 11 }}
        tickFormatter={(v) => (isSent ? Number(v).toFixed(1) : Number(v).toFixed(0))}
      />
      <Tooltip labelFormatter={(l) => `Date: ${l}`} />
      {isSent ? (
        <>
          <Area type="monotone" dataKey="s" dot={false} strokeOpacity={0.55} fillOpacity={0.15} />
          <Line type="monotone" dataKey="m" dot={false} strokeWidth={2} />
          <ReferenceLine y={0} strokeOpacity={0.35} />
        </>
      ) : (
        <Line type="monotone" dataKey="p" dot={false} strokeWidth={2} />
      )}
    </RC>
  );
}

export default function LineChart({ mode, dates, price, sentiment, sentimentMA7, height = 400 }: Props) {
  // Render only on client to avoid hydration/layout quirks on GH Pages
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);

  const rows = useMemo(() => toRows(dates, price, sentiment, sentimentMA7), [dates, price, sentiment, sentimentMA7]);

  if (!mounted) {
    return (
      <div className="w-full grid place-items-center text-neutral-400 text-sm" style={{ height }}>
        Rendering chart…
      </div>
    );
  }

  if (mode === "overlay") {
    return (
      <div className="w-full" style={{ height }}>
        <ResponsiveContainer width="100%" height="100%">
          <Overlay rows={rows} />
        </ResponsiveContainer>
      </div>
    );
  }

  // Separate mode -> stack two charts (sentiment then price)
  const splitH = Math.max(180, Math.floor(height * 0.55));
  return (
    <div className="w-full space-y-4">
      <div style={{ height: splitH }}>
        <ResponsiveContainer width="100%" height="100%">
          <Single rows={rows} which="sentiment" />
        </ResponsiveContainer>
      </div>
      <div style={{ height: splitH }}>
        <ResponsiveContainer width="100%" height="100%">
          <Single rows={rows} which="price" />
        </ResponsiveContainer>
      </div>
    </div>
  );
}
