// apps/web/app/ticker/[symbol]/TickerClient.tsx
"use client";

import * as React from "react";

type SeriesIn = {
  date: string[];
  price: number[];
  sentiment: number[];
};

type NewsItem = {
  ts: string;
  title: string;
  url?: string;
  source?: string;
  score?: number;
};

type Props = {
  symbol: string;
  series: SeriesIn;
  news?: NewsItem[];
};

type ViewMode = "overlay" | "separate";

function toPct(x: number) {
  const sign = x >= 0 ? "" : "-";
  const v = Math.abs(x) * 100;
  return `${sign}${v.toFixed(2)}%`;
}

function mean(arr: number[]) {
  if (!arr.length) return 0;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

function SentimentPriceChart({
  series,
  mode,
  height = 320,
}: {
  series: SeriesIn;
  mode: ViewMode;
  height?: number;
}) {
  const width = 1000;
  const pad = { t: 12, r: 56, b: 28, l: 40 };

  const n = series.date.length;
  const xs = (i: number) =>
    pad.l + (i * (width - pad.l - pad.r)) / Math.max(1, n - 1);

  const sAbsMax = Math.max(
    1,
    Math.max(...series.sentiment.map((x) => Math.abs(x)))
  );
  const sY = (v: number) => {
    const mid = pad.t + (height - pad.t - pad.b) / 2;
    return mid - (v / sAbsMax) * ((height - pad.t - pad.b) / 2);
  };

  const pMin = Math.min(...series.price);
  const pMax = Math.max(...series.price);
  const pY = (v: number) =>
    pad.t +
    ((pMax - v) * (height - pad.t - pad.b)) / Math.max(1e-6, pMax - pMin);

  const [hoverI, setHoverI] = React.useState<number | null>(null);

  return (
    <div style={{ border: "1px solid #e5e7eb", borderRadius: 12, padding: 12 }}>
      <div style={{ fontWeight: 600, margin: "4px 8px 8px" }}>
        Sentiment and Price Analysis
      </div>

      {mode === "overlay" ? (
        <svg
          viewBox={`0 0 ${width} ${height}`}
          style={{ width: "100%", display: "block" }}
          onMouseLeave={() => setHoverI(null)}
        >
          {/* Baseline for sentiment */}
          <line
            x1={pad.l}
            x2={width - pad.r}
            y1={sY(0)}
            y2={sY(0)}
            stroke="#e5e7eb"
          />
          {/* Sentiment bars */}
          {series.sentiment.map((v, i) => {
            const x = xs(i);
            const y = Math.min(sY(0), sY(v));
            const h = Math.abs(sY(v) - sY(0));
            return (
              <rect
                key={`s${i}`}
                x={x - 1}
                y={y}
                width={2}
                height={Math.max(1, h)}
                fill="#6b47dc"
                opacity={0.6}
              />
            );
          })}
          {/* Price line */}
          {series.price.map((v, i) => {
            if (i === 0) return null;
            const x1 = xs(i - 1),
              y1 = pY(series.price[i - 1]);
            const x2 = xs(i),
              y2 = pY(v);
            return (
              <line
                key={`p${i}`}
                x1={x1}
                y1={y1}
                x2={x2}
                y2={y2}
                stroke="#10b981"
                strokeWidth={1.5}
              />
            );
          })}
          {/* Hover crosshair */}
          {hoverI != null && (
            <line
              x1={xs(hoverI)}
              x2={xs(hoverI)}
              y1={pad.t}
              y2={height - pad.b}
              stroke="#9ca3af"
              strokeDasharray="4 4"
            />
          )}
          {/* Hover hit-areas */}
          {series.date.map((_, i) => {
            const x = xs(i);
            const w =
              (width - pad.l - pad.r) / Math.max(1, n - 1);
            return (
              <rect
                key={`h${i}`}
                x={x - w / 2}
                y={pad.t}
                width={w}
                height={height - pad.t - pad.b}
                fill="transparent"
                onMouseEnter={() => setHoverI(i)}
              />
            );
          })}
        </svg>
      ) : (
        <div style={{ display: "grid", gridTemplateRows: "1fr 1fr", gap: 8 }}>
          {/* Sentiment only */}
          <svg
            viewBox={`0 0 ${width} ${160}`}
            style={{ width: "100%", display: "block" }}
            onMouseLeave={() => setHoverI(null)}
          >
            <line x1={pad.l} x2={width - pad.r} y1={80} y2={80} stroke="#e5e7eb" />
            {series.sentiment.map((v, i) => {
              const x =
                pad.l + (i * (width - pad.l - pad.r)) / Math.max(1, n - 1);
              const y0 = 80,
                y1 = 80 - (v / sAbsMax) * 70;
              const y = Math.min(y0, y1),
                h = Math.abs(y1 - y0);
              return (
                <rect
                  key={i}
                  x={x - 1}
                  y={y}
                  width={2}
                  height={Math.max(1, h)}
                  fill="#6b47dc"
                  opacity={0.6}
                />
              );
            })}
          </svg>
          {/* Price only */}
          <svg
            viewBox={`0 0 ${width} ${160}`}
            style={{ width: "100%", display: "block" }}
            onMouseLeave={() => setHoverI(null)}
          >
            {series.price.map((v, i) => {
              if (i === 0) return null;
              const x1 =
                pad.l + ((i - 1) * (width - pad.l - pad.r)) / Math.max(1, n - 1);
              const x2 =
                pad.l + (i * (width - pad.l - pad.r)) / Math.max(1, n - 1);
              const pMin = Math.min(...series.price);
              const pMax = Math.max(...series.price);
              const y = (vv: number) =>
                pad.t +
                ((pMax - vv) * (160 - pad.t - 20)) /
                  Math.max(1e-6, pMax - pMin);
              return (
                <line
                  key={i}
                  x1={x1}
                  y1={y(series.price[i - 1])}
                  x2={x2}
                  y2={y(v)}
                  stroke="#10b981"
                  strokeWidth={1.5}
                />
              );
            })}
          </svg>
        </div>
      )}

      {/* Tooltip readout */}
      <div style={{ marginTop: 8, fontSize: 14, color: "#4b5563" }}>
        {hoverI != null ? (
          <>
            <b>Date:</b> {series.date[hoverI]} &nbsp; | &nbsp;
            <b>Sentiment Score:</b> {series.sentiment[hoverI].toFixed(2)} &nbsp; | &nbsp;
            <b>Stock Price:</b> {series.price[hoverI].toFixed(2)} &nbsp; | &nbsp;
            <b>Predicted return:</b>{" "}
            {toPct(
              ((series.price[hoverI + 1] ?? series.price[hoverI]) -
                series.price[hoverI]) /
                Math.max(1e-6, series.price[hoverI])
            )}
          </>
        ) : (
          <>Hover the chart to see values.</>
        )}
      </div>
    </div>
  );
}

function Pill({
  children,
  color = "#111827",
}: {
  children: React.ReactNode;
  color?: string;
}) {
  return (
    <span
      style={{
        display: "inline-block",
        padding: "2px 8px",
        borderRadius: 999,
        fontSize: 12,
        fontWeight: 600,
        color: "#ffffff",
        background: color,
        lineHeight: "18px",
      }}
    >
      {children}
    </span>
  );
}

function InsightCards({ series }: { series: SeriesIn }) {
  const last = series.price.length - 1;
  const ret =
    last > 0
      ? (series.price[last] - series.price[last - 1]) /
        Math.max(1e-6, series.price[last - 1])
      : 0;

  const recent = series.sentiment.slice(
    Math.max(0, series.sentiment.length - 7)
  );
  const sAvg = mean(recent);
  const sLabel = sAvg > 0.1 ? "Positive" : sAvg < -0.1 ? "Negative" : "Neutral";

  const advisory =
    sAvg > 0.4
      ? "Strong Buy"
      : sAvg > 0.1
      ? "Buy"
      : sAvg < -0.4
      ? "Strong Sell"
      : sAvg < -0.1
      ? "Sell"
      : "Hold";
  const ourRec = advisory.includes("Buy")
    ? "Buy"
    : advisory.includes("Sell")
    ? "Sell"
    : "Hold";

  const Card = ({
    title,
    value,
    sub,
  }: {
    title: string;
    value: string;
    sub?: string;
  }) => (
    <div
      style={{
        border: "1px solid #e5e7eb",
        borderRadius: 12,
        padding: 16,
        minWidth: 180,
      }}
    >
      <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 4 }}>
        {title}
      </div>
      <div style={{ fontSize: 22, fontWeight: 700 }}>{value}</div>
      {sub && (
        <div style={{ fontSize: 12, color: "#6b7280", marginTop: 4 }}>{sub}</div>
      )}
    </div>
  );

  return (
    <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
      <Card title="Live Market Sentiment" value={sLabel} sub="7-day average" />
      <Card title="Predicted Return" value={toPct(ret)} />
      <Card title="Advisory Opinion" value={advisory} />
      <Card title="Our Recommendation" value={ourRec} />
    </div>
  );
}

function Headlines({ items }: { items: NewsItem[] }) {
  return (
    <div style={{ border: "1px solid #e5e7eb", borderRadius: 12, padding: 12 }}>
      <div style={{ fontWeight: 600, margin: "4px 8px 4px" }}>
        Recent Headlines
      </div>
      <div style={{ fontSize: 13, color: "#6b7280", margin: "0 8px 12px" }}>
        Latest headlines with aggregated sentiment (when available). Sources from
        your configured provider.
      </div>
      {items && items.length ? (
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>
              <th style={{ padding: "8px" }}>Date Time</th>
              <th style={{ padding: "8px" }}>Headline</th>
              <th style={{ padding: "8px" }}>Source</th>
              <th style={{ padding: "8px" }}>Sentiment</th>
            </tr>
          </thead>
          <tbody>
            {items.map((n, i) => (
              <tr key={i} style={{ borderBottom: "1px solid #f3f4f6" }}>
                <td style={{ padding: "8px", fontSize: 13, color: "#374151" }}>
                  {n.ts}
                </td>
                <td style={{ padding: "8px" }}>
                  {n.url ? (
                    <a
                      href={n.url}
                      target="_blank"
                      rel="noreferrer"
                      style={{ color: "#1d4ed8", textDecoration: "none" }}
                    >
                      {n.title || "(no title)"}
                    </a>
                  ) : (
                    n.title || "(no title)"
                  )}
                </td>
                <td style={{ padding: "8px", fontSize: 13, color: "#6b7280" }}>
                  {n.source || ""}
                </td>
                <td style={{ padding: "8px" }}>
                  {typeof n.score === "number" ? (
                    n.score > 0.1 ? (
                      <Pill color="#10b981">Positive</Pill>
                    ) : n.score < -0.1 ? (
                      <Pill color="#ef4444">Negative</Pill>
                    ) : (
                      <Pill color="#6b7280">Neutral</Pill>
                    )
                  ) : (
                    <span style={{ color: "#6b7280", fontSize: 12 }}>—</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <div style={{ padding: 12, color: "#6b7280", fontSize: 14 }}>
          No headlines available.
        </div>
      )}
    </div>
  );
}

export default function TickerClient({ symbol, series, news = [] }: Props) {
  const [mode, setMode] = React.useState<ViewMode>("overlay");

  return (
    <div style={{ display: "grid", gap: 16 }}>
      {/* Toggle */}
      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <div
          role="tablist"
          aria-label="View mode"
          style={{
            border: "1px solid #e5e7eb",
            borderRadius: 999,
            overflow: "hidden",
          }}
        >
          {(["overlay", "separate"] as ViewMode[]).map((m) => (
            <button
              key={m}
              onClick={() => setMode(m)}
              style={{
                padding: "6px 12px",
                border: "none",
                background: mode === m ? "#111827" : "white",
                color: mode === m ? "white" : "#111827",
                cursor: "pointer",
              }}
            >
              {m === "overlay" ? "Overlayed View" : "Separate View"}
            </button>
          ))}
        </div>
      </div>

      {/* Chart */}
      <SentimentPriceChart series={series} mode={mode} />

      {/* Insights */}
      <div>
        <div style={{ fontWeight: 600, marginBottom: 8 }}>
          Live Market Insights
        </div>
        <InsightCards series={series} />
      </div>

      {/* Headlines */}
      <Headlines items={news} />
    </div>
  );
}
