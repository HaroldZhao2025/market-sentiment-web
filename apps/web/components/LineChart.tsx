"use client";

import {
  LineChart as RC,
  Line,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
  Legend,
  ReferenceLine,
} from "recharts";

type Props = {
  mode: "overlay" | "separate";
  dates: string[];          // "YYYY-MM-DD"
  price: number[];
  sentiment: number[];      // daily S ([-1,1] ideally; we clamp when drawing)
  sentimentMA7: number[];   // 7d MA of S
  height?: number;
};

function fmtDate(d: string) {
  // expect YYYY-MM-DD; show like "Nov 08"
  if (!d) return "";
  const [y, m, day] = d.split("-").map(Number);
  const dt = new Date(y, (m || 1) - 1, day || 1);
  return dt.toLocaleDateString(undefined, { month: "short", day: "2-digit" });
}

function fmtPrice(v?: number | string) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "";
  const n = Number(v);
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function clamp(v: number, lo = -1, hi = 1) {
  if (v === null || v === undefined || Number.isNaN(v)) return null;
  return Math.max(lo, Math.min(hi, v));
}

function useChartData(dates: string[], price: number[], s: number[], m: number[]) {
  const N = Math.max(dates.length, price.length, s.length, m.length);
  const rows = new Array(N).fill(0).map((_, i) => {
    const d = dates[i] ?? dates.at(-1) ?? "";
    const p = Number.isFinite(price[i]) ? price[i] : null;
    const S = Number.isFinite(s[i]) ? clamp(s[i]) : null;
    const M = Number.isFinite(m[i]) ? clamp(m[i]) : null;
    return { d, p, S, M };
  });
  return rows;
}

function CustomTooltip({
  active,
  payload,
  label,
}: {
  active?: boolean;
  payload?: any[];
  label?: string;
}) {
  if (!active || !payload || !payload.length) return null;

  // find by dataKey; recharts may reorder
  const byKey: Record<string, number | null> = {};
  for (const it of payload) {
    if (!it || typeof it.dataKey !== "string") continue;
    byKey[it.dataKey] = typeof it.value === "number" ? it.value : null;
  }

  const s = byKey["S"];
  const m = byKey["M"];
  const p = byKey["p"];

  return (
    <div className="rounded-xl bg-white/95 shadow ring-1 ring-black/5 px-3 py-2 text-sm">
      <div className="font-semibold">{label ? new Date(label).toLocaleDateString(undefined, { month: "short", day: "2-digit" }) : ""}</div>
      <div className="mt-1 space-y-0.5">
        <div>Sentiment: <span className="font-medium">{s ?? "—"}</span></div>
        <div>7-day MA: <span className="font-medium">{m ?? "—"}</span></div>
        <div>Price: <span className="font-medium">${fmtPrice(p)}</span></div>
      </div>
    </div>
  );
}

export default function LineChart({
  mode,
  dates,
  price,
  sentiment,
  sentimentMA7,
  height = 380,
}: Props) {
  const data = useChartData(dates, price, sentiment, sentimentMA7);

  if (!data.length) {
    return (
      <div className="h-[240px] grid place-items-center text-sm text-neutral-500">
        No data to plot.
      </div>
    );
  }

  const overlay = (
    <ResponsiveContainer width="100%" height={height}>
      <RC data={data} margin={{ top: 8, right: 16, bottom: 0, left: 8 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis
          dataKey="d"
          tick={{ fontSize: 11 }}
          minTickGap={28}
          tickFormatter={fmtDate}
        />
        {/* Left axis: sentiment in [-1, 1] */}
        <YAxis
          yAxisId="left"
          domain={[-1, 1]}
          tick={{ fontSize: 11 }}
          ticks={[-1, -0.5, 0, 0.5, 1]}
        />
        {/* Right axis: price */}
        <YAxis
          yAxisId="right"
          orientation="right"
          allowDecimals
          tick={{ fontSize: 11 }}
          tickFormatter={fmtPrice}
          // a bit of headroom
          domain={["auto", "auto"]}
        />
        <Tooltip content={<CustomTooltip />} />
        <Legend verticalAlign="top" height={24} />
        <ReferenceLine yAxisId="left" y={0} strokeOpacity={0.6} />

        {/* Sentiment area (left) */}
        <Area
          yAxisId="left"
          type="monotone"
          dataKey="S"
          name="Sentiment"
          strokeOpacity={0.25}
          fillOpacity={0.15}
          dot={false}
        />
        {/* Sentiment MA (left) */}
        <Line
          yAxisId="left"
          type="monotone"
          dataKey="M"
          name="Sentiment MA(7)"
          dot={false}
          strokeWidth={2}
        />
        {/* Price (right) */}
        <Line
          yAxisId="right"
          type="monotone"
          dataKey="p"
          name="Stock Price"
          dot={false}
          strokeWidth={2}
        />
      </RC>
    </ResponsiveContainer>
  );

  const separate = (
    <div className="space-y-6">
      <ResponsiveContainer width="100%" height={height * 0.55}>
        <RC data={data} margin={{ top: 8, right: 16, bottom: 0, left: 8 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis
            dataKey="d"
            tick={{ fontSize: 11 }}
            minTickGap={28}
            tickFormatter={fmtDate}
          />
          <YAxis
            domain={[-1, 1]}
            tick={{ fontSize: 11 }}
            ticks={[-1, -0.5, 0, 0.5, 1]}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend verticalAlign="top" height={24} />
          <ReferenceLine y={0} strokeOpacity={0.6} />
          <Area
            type="monotone"
            dataKey="S"
            name="Sentiment"
            strokeOpacity={0.25}
            fillOpacity={0.15}
            dot={false}
          />
          <Line
            type="monotone"
            dataKey="M"
            name="Sentiment MA(7)"
            dot={false}
            strokeWidth={2}
          />
        </RC>
      </ResponsiveContainer>

      <ResponsiveContainer width="100%" height={height * 0.55}>
        <RC data={data} margin={{ top: 8, right: 16, bottom: 0, left: 8 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis
            dataKey="d"
            tick={{ fontSize: 11 }}
            minTickGap={28}
            tickFormatter={fmtDate}
          />
          <YAxis
            orientation="right"
            tick={{ fontSize: 11 }}
            tickFormatter={fmtPrice}
            domain={["auto", "auto"]}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend verticalAlign="top" height={24} />
          <Line type="monotone" dataKey="p" name="Stock Price" dot={false} strokeWidth={2} />
        </RC>
      </ResponsiveContainer>
    </div>
  );

  return mode === "overlay" ? overlay : separate;
}
