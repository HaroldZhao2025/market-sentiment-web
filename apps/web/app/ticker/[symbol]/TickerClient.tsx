// apps/web/app/ticker/[symbol]/TickerClient.tsx
"use client";

import * as React from "react";

type SeriesIn = { date: string[]; price: number[]; sentiment: number[] };
type NewsItem = { ts: string; title: string; url?: string; source?: string; score?: number };
type Props = { symbol: string; series: SeriesIn; news?: NewsItem[] };
type ViewMode = "overlay" | "price" | "sentiment";

// ---- utils ----
const mean = (a: number[]) => (a.length ? a.reduce((s, v) => s + v, 0) / a.length : 0);
const toPct = (x: number) => `${(x * 100).toFixed(2)}%`;
function dateOnly(s: string): string {
  const m = s.match(/\d{4}-\d{2}-\d{2}/);
  if (m) return m[0];
  const d = new Date(s);
  if (!Number.isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  return (s.split(" ")[0] || s).replace(/T.*/, "");
}

// ---- chart (pure SVG) ----
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
  const xAt = (i: number) => pad.l + (i * (width - pad.l - pad.r)) / Math.max(1, n - 1);

  // sentiment axis
  const sAbsMax = Math.max(1, ...series.sentiment.map((x) => Math.abs(x)));
  const sY = (v: number) => {
    const mid = pad.t + (height - pad.t - pad.b) / 2;
    return mid - (v / sAbsMax) * ((height - pad.t - pad.b) / 2);
  };

  // price axis
  const pMin = Math.min(...series.price);
  const pMax = Math.max(...series.price);
  const pY = (v: number) =>
    pad.t + ((pMax - v) * (height - pad.t - pad.b)) / Math.max(1e-6, pMax - pMin);

  const [hoverI, setHoverI] = React.useState<number | null>(null);
  const showSent = mode === "overlay" || mode === "sentiment";
  const showPrice = mode === "overlay" || mode === "price";

  return (
    <div style={{ border: "1px solid #e5e7eb", borderRadius: 12, padding: 12 }}>
      <div style={{ fontWeight: 600, margin: "4px 8px 8px" }}>Sentiment and Price Analysis</div>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        style={{ width: "100%", display: "block" }}
        onMouseLeave={() => setHoverI(null)}
        role="img"
        aria-label="Sentiment and price"
      >
        {showSent && (
          <line x1={pad.l} x2={width - pad.r} y1={sY(0)} y2={sY(0)} stroke="#e5e7eb" />
        )}

        {showSent &&
          series.sentiment.map((v, i) => {
            const x = xAt(i);
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

        {showPrice &&
          series.price.map((v, i) => {
            if (i === 0) return null;
            const x1 = xAt(i - 1), y1 = pY(series.price[i - 1]);
            const x2 = xAt(i), y2 = pY(v);
            return <line key={`p${i}`} x1={x1} y1={y1} x2={x2} y2={y2} stroke="#10b981" strokeWidth={1.5} />;
          })}

        {hoverI != null && (
          <line
            x1={xAt(hoverI)}
            x2={xAt(hoverI)}
            y1={pad.t}
            y2={height - pad.b}
            stroke="#9ca3af"
            strokeDasharray="4 4"
          />
        )}

        {series.date.map((_, i) => {
          const w = (width - pad.l - pad.r) / Math.max(1, n - 1);
          const x = xAt(i);
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

      <div style={{ marginTop: 8, fontSize: 14, color: "#4b5563" }}>
        {hoverI != null ? (
          <>
            <b>Date:</b> {series.date[hoverI]} &nbsp; | &nbsp;
            {showSent && (<><b>Sentiment:</b> {series.sentiment[hoverI].toFixed(2)} &nbsp; | &nbsp;</>)}
            {showPrice && (
              <>
                <b>Price:</b> {series.price[hoverI].toFixed(2)} &nbsp; | &nbsp;
                <b>Pred. return:</b>{" "}
                {toPct(
                  ((series.price[hoverI + 1] ?? series.price[hoverI]) - series.price[hoverI]) /
                    Math.max(1e-6, series.price[hoverI])
                )}
              </>
            )}
          </>
        ) : (
          <>Hover to see values.</>
        )}
      </div>
    </div>
  );
}

// ---- small UI ----
function Pill({ children, color = "#111827" }: { children: React.ReactNode; color?: string }) {
  return (
    <span
      style={{
        display: "inline-block",
        padding: "2px 8px",
        borderRadius: 999,
        fontSize: 12,
        fontWeight: 600,
        color: "#fff",
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
  const oneDayRet =
    last > 0
      ? (series.price[last] - series.price[last - 1]) / Math.max(1e-6, series.price[last - 1])
      : 0;
  const recent = series.sentiment.slice(Math.max(0, series.sentiment.length - 7));
  const sAvg = mean(recent);
  const sLabel = sAvg > 0.1 ? "Positive" : sAvg < -0.1 ? "Negative" : "Neutral";
  const advisory =
    sAvg > 0.4 ? "Strong Buy" :
    sAvg > 0.1 ? "Buy" :
    sAvg < -0.4 ? "Strong Sell" :
    sAvg < -0.1 ? "Sell" : "Hold";
  const ourRec = advisory.includes("Buy") ? "Buy" : advisory.includes("Sell") ? "Sell" : "Hold";

  const Card = ({ title, value, sub }: { title: string; value: string; sub?: string }) => (
    <div style={{ border: "1px solid #e5e7eb", borderRadius: 12, padding: 16, minWidth: 180 }}>
      <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 4 }}>{title}</div>
      <div style={{ fontSize: 22, fontWeight: 700 }}>{value}</div>
      {sub && <div style={{ fontSize: 12, color: "#6b7280", marginTop: 4 }}>{sub}</div>}
    </div>
  );

  return (
    <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
      <Card title="Live Market Sentiment" value={sLabel} sub="7-day average" />
      <Card title="Predicted Return" value={toPct(oneDayRet)} />
      <Card title="Advisory Opinion" value={advisory} />
      <Card title="Our Recommendation" value={ourRec} />
    </div>
  );
}

function Headlines({ items }: { items: NewsItem[] }) {
  return (
    <div style={{ border: "1px solid #e5e7eb", borderRadius: 12, padding: 12 }}>
      <div style={{ fontWeight: 600, margin: "4px 8px 4px" }}>Recent Headlines</div>
      <div style={{ fontSize: 13, color: "#6b7280", margin: "0 8px 12px" }}>
        Latest headlines with aggregated sentiment (when available).
      </div>
      {items && items.length ? (
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>
              <th style={{ padding: "8px" }}>Date</th>
              <th style={{ padding: "8px" }}>Headline</th>
              <th style={{ padding: "8px" }}>Source</th>
              <th style={{ padding: "8px" }}>Sentiment</th>
            </tr>
          </thead>
          <tbody>
            {items.map((n, i) => (
              <tr key={i} style={{ borderBottom: "1px solid #f3f4f6" }}>
                <td style={{ padding: "8px", fontSize: 13, color: "#374151" }}>{dateOnly(n.ts)}</td>
                <td style={{ padding: "8px" }}>
                  {n.url ? (
                    <a href={n.url} target="_blank" rel="noreferrer" style={{ color: "#1d4ed8", textDecoration: "none" }}>
                      {n.title || "(no title)"}
                    </a>
                  ) : (n.title || "(no title)")}
                </td>
                <td style={{ padding: "8px", fontSize: 13, color: "#6b7280" }}>{n.source || ""}</td>
                <td style={{ padding: "8px" }}>
                  {typeof n.score === "number" ? (
                    n.score > 0.1 ? <Pill color="#10b981">Positive</Pill>
                    : n.score < -0.1 ? <Pill color="#ef4444">Negative</Pill>
                    : <Pill color="#6b7280">Neutral</Pill>
                  ) : <span style={{ color: "#6b7280", fontSize: 12 }}>—</span>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <div style={{ padding: 12, color: "#6b7280", fontSize: 14 }}>No headlines available.</div>
      )}
    </div>
  );
}

// ---- main ----
export default function TickerClient({ symbol, series, news = [] }: Props) {
  const [mode, setMode] = React.useState<ViewMode>("overlay");

  const Button = ({ label, value }: { label: string; value: ViewMode }) => (
    <button
      onClick={() => setMode(value)}
      aria-pressed={mode === value}
      style={{
        padding: "6px 12px",
        border: "none",
        background: mode === value ? "#111827" : "white",
        color: mode === value ? "white" : "#111827",
        cursor: "pointer",
      }}
    >
      {label}
    </button>
  );

  return (
    <div style={{ display: "grid", gap: 16 }}>
      {/* 3-way segmented control */}
      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <div role="tablist" aria-label="View mode" style={{ border: "1px solid #e5e7eb", borderRadius: 999, overflow: "hidden" }}>
          <Button label="Overlay" value="overlay" />
          <Button label="Price Only" value="price" />
          <Button label="Sentiment Only" value="sentiment" />
        </div>
      </div>

      <SentimentPriceChart series={series} mode={mode} />

      <div>
        <div style={{ fontWeight: 600, marginBottom: 8 }}>Live Market Insights</div>
        <InsightCards series={series} />
      </div>

      <Headlines items={news} />
    </div>
  );
}
