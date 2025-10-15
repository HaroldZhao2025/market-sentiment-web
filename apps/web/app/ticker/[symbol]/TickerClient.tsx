// apps/web/app/ticker/[symbol]/TickerClient.tsx
"use client";

import LineChart from "../../../components/LineChart";

export default function TickerClient({
  dates,
  price,
  sentiment,
  sentiment_ma7,
  label = "Signal S",
}: {
  dates: string[];
  price: number[];
  sentiment: number[];
  sentiment_ma7?: number[];
  label?: string;
}) {
  const left = dates.map((d, i) => ({ x: d, y: Number.isFinite(price[i]) ? price[i] : 0 }));
  const right = dates.map((d, i) => ({ x: d, y: Number.isFinite(sentiment[i]) ? sentiment[i] : 0 }));
  const overlay =
    Array.isArray(sentiment_ma7) && sentiment_ma7.length
      ? dates.map((d, i) => ({ x: d, y: Number.isFinite(sentiment_ma7[i]) ? sentiment_ma7[i] : 0 }))
      : undefined;

  return (
    <div>
      <h3 className="font-semibold mb-3">Price & {label}</h3>
      <LineChart left={left} right={right} overlay={overlay} height={300} />
    </div>
  );
}
