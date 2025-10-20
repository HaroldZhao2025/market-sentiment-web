// apps/web/components/LineChart.tsx
"use client";

import {
  ResponsiveContainer,
  ComposedChart,
  Area,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceLine,
} from "recharts";

type Props = {
  mode: "overlay" | "separate";
  dates: string[];
  price: number[];
  sentiment: number[];
  sentimentMA7: number[];
  height?: number;
};

function toNumOrNull(v: any): number | null {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}
function fmtDate(d: string) {
  try {
    return new Date(d).toLocaleDateString(undefined, { month: "short", year: "2-digit" });
  } catch {
    return d;
  }
}

export default function LineChart({
  mode,
  dates,
  price,
  sentiment,
  sentimentMA7,
  height = 420,
}: Props) {
  const data = dates.map((d, i) => ({
    d,
    p: toNumOrNull(price[i]),
    s: toNumOrNull(sentiment[i]),
    m: toNumOrNull(sentimentMA7[i]),
  }));

  const TooltipBox = ({
    active,
    payload,
    label,
  }: {
    active?: boolean;
    payload?: any[];
    label?: string;
  }) => {
    if (!active || !payload || payload.length === 0) return null;
    const s = payload.find((p) => p.dataKey === "s")?.value ?? null;
    const m = payload.find((p) => p.dataKey === "m")?.value ?? null;
    const p = payload.find((p) => p.dataKey === "p")?.value ?? null;

    const dateStr = (() => {
      try {
        return new Date(label ?? "").toLocaleDateString(undefined, {
          year: "numeric",
          month: "short",
          day: "2-digit",
        });
      } catch {
        return label ?? "";
      }
    })();

    return (
      <div className="rounded-xl border bg-white/95 p-3 shadow text-sm">
        <div className="font-semibold">{dateStr}</div>
        {s != null && <div>Sentiment: {Number(s).toFixed(2)}</div>}
        {m != null && <div>Sentiment (7d MA): {Number(m).toFixed(2)}</div>}
        {p != null && (
          <div>
            Price:{" "}
            {Number(p).toLocaleString(undefined, { maximumFractionDigits: 2 })}
          </div>
        )}
      </div>
    );
  };

  return (
    <div style={{ width: "100%", height }}>
      <ResponsiveContainer>
        <ComposedChart data={data} margin={{ top: 12, right: 24, bottom: 0, left: 8 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="d" tick={{ fontSize: 11 }} minTickGap={28} tickFormatter={fmtDate} />
          {/* sentiment axis fixed [-1, 1] */}
          <YAxis
            yAxisId="left"
            domain={[-1, 1]}
            allowDataOverflow
            tick={{ fontSize: 11 }}
            tickFormatter={(v) => (typeof v === "number" ? v.toFixed(1) : "")}
          />
          {/* price axis auto */}
          <YAxis
            yAxisId="right"
            orientation="right"
            tick={{ fontSize: 11 }}
            tickFormatter={(v) =>
              typeof v === "number"
                ? v.toLocaleString(undefined, { maximumFractionDigits: 2 })
                : ""
            }
          />
          <Tooltip content={<TooltipBox />} />
          <Legend />
          <ReferenceLine yAxisId="left" y={0} strokeOpacity={0.4} />

          {/* sentiment area + its 7d MA */}
          <Area yAxisId="left" type="monotone" dataKey="s" name="Sentiment Score" connectNulls />
          <Line yAxisId="left" type="monotone" dataKey="m" name="Sentiment (7d MA)" dot={false} strokeWidth={2} connectNulls />
          {/* price */}
          <Line yAxisId="right" type="monotone" dataKey="p" name="Stock Price" dot={false} strokeWidth={2} connectNulls />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}
