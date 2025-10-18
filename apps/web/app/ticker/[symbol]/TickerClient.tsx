// apps/web/app/ticker/[symbol]/TickerClient.tsx
"use client";

import LineChart from "../../../components/LineChart";

type SeriesIn = {
  date: string[];
  price: number[];
  sentiment: number[];
  sentiment_ma7: number[];
  label: string;
};

type NewsItem = {
  ts?: string;
  title?: string;
  url?: string;
  S?: number;
  [k: string]: unknown;
};

export default function TickerClient({
  series,
  news,
}: {
  series: SeriesIn;
  news: NewsItem[];
}) {
  const { date, price, sentiment, sentiment_ma7, label } = series;

  // Adapt to the existing LineChart API (expects left/right/overlay)
  const left = { dates: date, values: price, label: "Close" };
  const right = { dates: date, values: sentiment, label };
  const overlay = {
    dates: date,
    values: sentiment_ma7,
    label: `${label} (7d MA)`,
  };

  return (
    <div className="space-y-8">
      <div className="rounded-2xl p-4 shadow-sm border">
        <h3 className="font-semibold mb-3">Price & Daily Sentiment</h3>
        <LineChart left={left} right={right} overlay={overlay} height={380} />
      </div>

      <div className="rounded-2xl p-4 shadow-sm border">
        <h3 className="font-semibold mb-3">Recent News</h3>
        <ul className="space-y-2">
          {(news ?? [])
            .slice(-30) // latest 30
            .reverse()
            .map((n, i) => (
              <li key={i} className="text-sm leading-5">
                <span className="opacity-60 mr-2">
                  {(n.ts ?? "").toString().slice(0, 10)}
                </span>
                {n.url ? (
                  <a
                    className="underline"
                    href={n.url}
                    target="_blank"
                    rel="noreferrer"
                  >
                    {n.title ?? "(no title)"}
                  </a>
                ) : (
                  <span>{n.title ?? "(no title)"}</span>
                )}
                {typeof n.S === "number" && (
                  <span className="ml-2 opacity-60">
                    S={n.S.toFixed(3)}
                  </span>
                )}
              </li>
            ))}
        </ul>
      </div>
    </div>
  );
}
