"use client";

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
  price?: number[];        // optional for portfolio
  sentiment: number[];
  sentimentMA7?: number[]; // optional; caller can compute or omit
  height?: number;         // chart height for each panel
};

const fmtDate = (d: string) => {
  // Expect "YYYY-MM-DD" strings; show month+day compactly.
  try {
    const o = new Date(d + "T00:00:00Z");
    return o.toLocaleDateString(undefined, { month: "short", day: "2-digit" });
  } catch {
    return d;
  }
};

function buildData(dates: string[], price?: number[], s?: number[], m?: number[]) {
  const n = dates.length;
  const rows = new Array(Math.max(n, price?.length ?? 0, s?.length ?? 0)).fill(0).map((_, i) => ({
    d: dates[i] ?? "",
    p: price?.[i] ?? null,
    s: s?.[i] ?? null,
    m: m?.[i] ?? null,
  }));
  return rows;
}

function OverlayChart({ data, height }: { data: any[]; height: number }) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <RC data={data} margin={{ top: 8, right: 20, bottom: 8, left: 8 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="d" tick={{ fontSize: 11 }} tickFormatter={fmtDate} minTickGap={30} />
        <YAxis
          yAxisId="left"
          domain={[-1, 1]}
          allowDataOverflow
          tick={{ fontSize: 11 }}
          tickCount={5}
        />
        <YAxis
          yAxisId="right"
          orientation="right"
          tick={{ fontSize: 11 }}
          tickFormatter={(v) => (v == null ? "" : Number(v).toFixed(0))}
        />
        <Tooltip
          formatter={(value: any, name) => {
            if (name === "Sentiment" || name === "Sentiment MA(7)") {
              return [Number(value).toFixed(2), name];
            }
            if (name === "Price") {
              return [Number(value).toFixed(2), name];
            }
            return [value, name];
          }}
          labelFormatter={(l) => `Date: ${l}`}
        />
        <Legend />
        {/* sentiment area + MA line (left axis) */}
        <Area
          name="Sentiment"
          yAxisId="left"
          type="monotone"
          dataKey="s"
          dot={false}
          strokeOpacity={0.55}
          fillOpacity={0.15}
        />
        <Line
          name="Sentiment MA(7)"
          yAxisId="left"
          type="monotone"
          dataKey="m"
          dot={false}
          strokeWidth={2}
        />
        <ReferenceLine y={0} yAxisId="left" strokeOpacity={0.35} />
        {/* price line (right axis) */}
        <Line
          name="Price"
          yAxisId="right"
          type="monotone"
          dataKey="p"
          dot={false}
          strokeWidth={2}
        />
      </RC>
    </ResponsiveContainer>
  );
}

function SingleChart({
  data,
  height,
  which,
}: {
  data: any[];
  height: number;
  which: "sentiment" | "price";
}) {
  const left = which === "sentiment";
  return (
    <ResponsiveContainer width="100%" height={height}>
      <RC data={data} margin={{ top: 8, right: 20, bottom: 8, left: 8 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="d" tick={{ fontSize: 11 }} tickFormatter={fmtDate} minTickGap={30} />
        <YAxis
          domain={left ? [-1, 1] : ["auto", "auto"]}
          tick={{ fontSize: 11 }}
          tickFormatter={(v) => (left ? Number(v).toFixed(1) : Number(v).toFixed(0))}
        />
        <Tooltip labelFormatter={(l) => `Date: ${l}`} />
        {left ? (
          <>
            <Area type="monotone" dataKey="s" dot={false} strokeOpacity={0.55} fillOpacity={0.15} />
            <Line type="monotone" dataKey="m" dot={false} strokeWidth={2} />
            <ReferenceLine y={0} strokeOpacity={0.35} />
          </>
        ) : (
          <Line type="monotone" dataKey="p" dot={false} strokeWidth={2} />
        )}
      </RC>
    </ResponsiveContainer>
  );
}

export default function LineChart({
  mode,
  dates,
  price,
  sentiment,
  sentimentMA7,
  height = 360,
}: Props) {
  const data = buildData(dates, price, sentiment, sentimentMA7);
  if (mode === "overlay") {
    return <OverlayChart data={data} height={height} />;
  }
  return (
    <div className="space-y-4">
      <SingleChart data={data} height={Math.max(180, Math.floor(height * 0.55))} which="sentiment" />
      <SingleChart data={data} height={Math.max(180, Math.floor(height * 0.55))} which="price" />
    </div>
  );
}
