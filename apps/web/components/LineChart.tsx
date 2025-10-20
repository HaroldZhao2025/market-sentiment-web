// apps/web/components/LineChart.tsx
"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import {
  LineChart as RC,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
} from "recharts";

type Props = {
  mode: "overlay" | "separate";
  dates: string[];
  price: number[];
  sentiment: number[];
  sentimentMA7: number[];
  height?: number;
};

export default function LineChart({
  mode,
  dates,
  price,
  sentiment,
  sentimentMA7,
  height = 380,
}: Props) {
  // Build rows
  const data = useMemo(
    () =>
      dates.map((d, i) => ({
        d,
        p: Number.isFinite(price[i]) ? price[i] : null,
        s: Number.isFinite(sentiment[i]) ? sentiment[i] : null,
        m: Number.isFinite(sentimentMA7[i]) ? sentimentMA7[i] : null,
      })),
    [dates, price, sentiment, sentimentMA7]
  );

  // Measure container width (ResponsiveContainer sometimes sees width=0 on GH Pages)
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const [w, setW] = useState<number>(800);
  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    const ro = new ResizeObserver(() => setW(el.clientWidth || 800));
    setW(el.clientWidth || 800);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const leftId = "left";
  const rightId = mode === "overlay" ? "right" : leftId;

  if (!data.length) {
    return (
      <div className="text-sm text-neutral-500">No time series to plot.</div>
    );
  }

  return (
    <div ref={wrapRef} style={{ width: "100%", height }}>
      <RC width={w} height={height} data={data} margin={{ top: 10, right: 24, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="d" tick={{ fontSize: 11 }} minTickGap={28} />
        <YAxis yAxisId={leftId} tick={{ fontSize: 11 }} />
        <YAxis yAxisId={rightId} orientation="right" tick={{ fontSize: 11 }} />
        <Tooltip />
        {/* sentiment */}
        <Line
          yAxisId={leftId}
          type="monotone"
          dataKey="s"
          dot={false}
          stroke="#7c83ff"
          strokeOpacity={0.45}
          strokeWidth={1.5}
          name="Sentiment"
        />
        <Line
          yAxisId={leftId}
          type="monotone"
          dataKey="m"
          dot={false}
          stroke="#4f46e5"
          strokeWidth={2}
          name="Sentiment MA7"
        />
        {/* price */}
        <Line
          yAxisId={rightId}
          type="monotone"
          dataKey="p"
          dot={false}
          stroke="#16a34a"
          strokeWidth={2}
          name="Stock Price"
        />
      </RC>
    </div>
  );
}
