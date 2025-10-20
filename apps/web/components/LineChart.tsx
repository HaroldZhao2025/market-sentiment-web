"use client";

import {
  LineChart as RC,
  Line,
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
  dates: string[];
  price?: number[];          // optional for portfolio
  sentiment: number[];
  sentimentMA7?: number[];
  height?: number;
};

function buildSeries(
  dates: string[],
  price: (number | undefined)[] = [],
  sentiment: (number | undefined)[] = [],
  sentimentMA7: (number | undefined)[] = []
) {
  const n = Math.min(
    dates.length,
    price.length || dates.length,
    sentiment.length || dates.length
  );
  const rows = [];
  for (let i = 0; i < n; i++) {
    const d = dates[i];
    const p = Number.isFinite(Number(price[i])) ? Number(price[i]) : null;
    const s = Number.isFinite(Number(sentiment[i])) ? Number(sentiment[i]) : null;
    const m = Number.isFinite(Number(sentimentMA7[i])) ? Number(sentimentMA7[i]) : null;
    rows.push({ d, p, s, m });
  }
  return rows;
}

const AxisTickStyle = { fontSize: 11 };

export default function LineChart({
  mode,
  dates,
  price = [],
  sentiment,
  sentimentMA7 = [],
  height = 360,
}: Props) {
  const data = buildSeries(dates, price, sentiment, sentimentMA7);

  const Chart = (
    <ResponsiveContainer width="100%" height={height}>
      <RC data={data} margin={{ top: 10, right: 20, bottom: 6, left: 6 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="d" tick={AxisTickStyle} minTickGap={28} />
        {/* Left = sentiment in [-1, 1] with zero line */}
        <YAxis
          yAxisId="left"
          domain={[-1, 1]}
          tick={AxisTickStyle}
          allowDataOverflow
          width={40}
        />
        {/* Right = price */}
        <YAxis
          yAxisId="right"
          orientation="right"
          tick={AxisTickStyle}
          allowDecimals
          width={56}
        />
        <Tooltip
          formatter={(val: any, name) => {
            if (name === "Sentiment" || name === "7-day MA") {
              return [Number(val).toFixed(2), name];
            }
            return [val, name];
          }}
          labelFormatter={(v) => `Date: ${v}`}
        />
        <Legend />
        <ReferenceLine yAxisId="left" y={0} strokeOpacity={0.4} />
        {/* Sentiment (bars feel crowded on export sites; stick to lines) */}
        <Line
          name="Sentiment"
          yAxisId="left"
          type="monotone"
          dataKey="s"
          dot={false}
          strokeWidth={1.6}
        />
        <Line
          name="7-day MA"
          yAxisId="left"
          type="monotone"
          dataKey="m"
          dot={false}
          strokeWidth={2.2}
        />
        {/* Price */}
        <Line
          name="Stock Price"
          yAxisId="right"
          type="monotone"
          dataKey="p"
          dot={false}
          strokeWidth={2}
        />
      </RC>
    </ResponsiveContainer>
  );

  if (mode === "overlay") return Chart;

  // “Separate” mode: show two smaller panels stacked
  return (
    <div className="space-y-4">
      <div className="rounded-xl border p-3">
        <ResponsiveContainer width="100%" height={220}>
          <RC data={data} margin={{ top: 8, right: 18, bottom: 6, left: 6 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="d" tick={AxisTickStyle} minTickGap={28} />
            <YAxis yAxisId="left" domain={[-1, 1]} tick={AxisTickStyle} />
            <Tooltip labelFormatter={(v) => `Date: ${v}`} />
            <Legend />
            <ReferenceLine yAxisId="left" y={0} strokeOpacity={0.4} />
            <Line
              name="Sentiment"
              yAxisId="left"
              type="monotone"
              dataKey="s"
              dot={false}
              strokeWidth={1.6}
            />
            <Line
              name="7-day MA"
              yAxisId="left"
              type="monotone"
              dataKey="m"
              dot={false}
              strokeWidth={2.2}
            />
          </RC>
        </ResponsiveContainer>
      </div>

      <div className="rounded-xl border p-3">
        <ResponsiveContainer width="100%" height={220}>
          <RC data={data} margin={{ top: 8, right: 18, bottom: 6, left: 6 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="d" tick={AxisTickStyle} minTickGap={28} />
            <YAxis yAxisId="right" orientation="right" tick={AxisTickStyle} />
            <Tooltip labelFormatter={(v) => `Date: ${v}`} />
            <Legend />
            <Line
              name="Stock Price"
              yAxisId="right"
              type="monotone"
              dataKey="p"
              dot={false}
              strokeWidth={2}
            />
          </RC>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
