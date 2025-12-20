// apps/web/pages/sp500.tsx
import type { GetStaticProps, InferGetStaticPropsType } from "next";
import Head from "next/head";
import Link from "next/link";
import fs from "fs";
import path from "path";
import React, { useMemo, useState } from "react";

type DailyRow = {
  date: string;      // YYYY-MM-DD
  close: number;     // index close (e.g., ^GSPC)
  sentiment: number; // sentiment_cap_weighted
};

type Sp500Props = {
  symbol: string; // "SPX"
  name: string;   // "S&P 500 Index"
  rows: DailyRow[];
  closeSourceSymbol?: string; // e.g. "^GSPC"
};

function clamp01(x: number) {
  if (!Number.isFinite(x)) return 0;
  return Math.max(0, Math.min(1, x));
}

function fmt2(x: number) {
  if (!Number.isFinite(x)) return "—";
  return x.toFixed(2);
}

function fmt4(x: number) {
  if (!Number.isFinite(x)) return "—";
  return x.toFixed(4);
}

function mean(xs: number[]) {
  const ys = xs.filter((v) => Number.isFinite(v));
  if (!ys.length) return NaN;
  return ys.reduce((a, b) => a + b, 0) / ys.length;
}

function calcAdvisory(latest: number, avg7: number) {
  if (!Number.isFinite(latest) || !Number.isFinite(avg7)) return "Hold";
  if (avg7 > 0.08) return "Buy";
  if (avg7 < -0.08) return "Sell";
  return "Hold";
}

function LineChartSvg(props: {
  rows: DailyRow[];
  mode: "overlay" | "separate" | "price" | "sentiment";
}) {
  const { rows, mode } = props;

  const W = 920;
  const H = 320;
  const pad = 36;

  const dates = rows.map((r) => r.date);
  const prices = rows.map((r) => r.close);
  const sents = rows.map((r) => r.sentiment);

  const pMin = Math.min(...prices);
  const pMax = Math.max(...prices);
  const sMin = Math.min(...sents);
  const sMax = Math.max(...sents);

  const xOf = (i: number) => {
    if (rows.length <= 1) return pad;
    const t = i / (rows.length - 1);
    return pad + t * (W - 2 * pad);
  };

  const yPrice = (v: number) => {
    if (pMax === pMin) return H / 2;
    const t = (v - pMin) / (pMax - pMin);
    return pad + (1 - t) * (H - 2 * pad);
  };

  const ySent = (v: number) => {
    if (sMax === sMin) return H / 2;
    const t = (v - sMin) / (sMax - sMin);
    return pad + (1 - t) * (H - 2 * pad);
  };

  const yOverlay = (t01: number) => pad + (1 - clamp01(t01)) * (H - 2 * pad);

  const price01 = prices.map((v) => (pMax === pMin ? 0.5 : (v - pMin) / (pMax - pMin)));
  const sent01 = sents.map((v) => (sMax === sMin ? 0.5 : (v - sMin) / (sMax - sMin)));

  const mkPath = (ys: number[], yFn: (v: number) => number) => {
    if (!ys.length) return "";
    return ys
      .map((v, i) => `${i === 0 ? "M" : "L"} ${xOf(i).toFixed(2)} ${yFn(v).toFixed(2)}`)
      .join(" ");
  };

  const pricePath =
    mode === "overlay" ? mkPath(price01, yOverlay) : mkPath(prices, yPrice);
  const sentPath =
    mode === "overlay" ? mkPath(sent01, yOverlay) : mkPath(sents, ySent);

  const showPrice = mode === "overlay" || mode === "separate" || mode === "price";
  const showSent = mode === "overlay" || mode === "separate" || mode === "sentiment";

  const xTicks = [0, Math.floor((rows.length - 1) * 0.33), Math.floor((rows.length - 1) * 0.66), rows.length - 1]
    .filter((i, idx, arr) => i >= 0 && i < rows.length && arr.indexOf(i) === idx);

  return (
    <div style={{ width: "100%", overflowX: "auto" }}>
      <svg width={W} height={H} viewBox={`0 0 ${W} ${H}`} role="img" aria-label="S&P 500 sentiment chart">
        {/* border */}
        <rect x="1" y="1" width={W - 2} height={H - 2} fill="none" stroke="currentColor" opacity="0.15" />

        {/* axes */}
        <line x1={pad} y1={H - pad} x2={W - pad} y2={H - pad} stroke="currentColor" opacity="0.2" />
        <line x1={pad} y1={pad} x2={pad} y2={H - pad} stroke="currentColor" opacity="0.2" />

        {/* x ticks */}
        {xTicks.map((i) => (
          <g key={i}>
            <line x1={xOf(i)} y1={H - pad} x2={xOf(i)} y2={H - pad + 6} stroke="currentColor" opacity="0.25" />
            <text x={xOf(i)} y={H - pad + 20} fontSize="10" textAnchor="middle" opacity="0.7">
              {dates[i]}
            </text>
          </g>
        ))}

        {/* paths */}
        {showPrice && (
          <path d={pricePath} fill="none" stroke="currentColor" strokeWidth="2" opacity="0.9" />
        )}
        {showSent && (
          <path d={sentPath} fill="none" stroke="currentColor" strokeWidth="2" opacity="0.55" strokeDasharray="4 3" />
        )}

        {/* legend */}
        <g>
          <text x={pad} y={16} fontSize="12" opacity="0.85">Price (solid) · Sentiment (dashed)</text>
        </g>
      </svg>
    </div>
  );
}

export default function Sp500Page(
  props: InferGetStaticPropsType<typeof getStaticProps>
) {
  const { symbol, name, rows, closeSourceSymbol } = props;

  const [mode, setMode] = useState<"separate" | "overlay" | "price" | "sentiment">("separate");

  const latest = rows.length ? rows[rows.length - 1] : null;

  const avg7 = useMemo(() => {
    const tail = rows.slice(-7).map((r) => r.sentiment);
    return mean(tail);
  }, [rows]);

  const advisory = useMemo(() => {
    const latestSent = latest?.sentiment ?? NaN;
    return calcAdvisory(latestSent, avg7);
  }, [latest, avg7]);

  if (!rows.length) {
    return (
      <>
        <Head>
          <title>S&P 500 Sentiment | Market Sentiment</title>
        </Head>
        <main style={{ maxWidth: 980, margin: "0 auto", padding: 16 }}>
          <h1>S&amp;P 500 Sentiment</h1>
          <p>
            No S&amp;P 500 index data yet. Make sure{" "}
            <code>apps/web/public/data/sp500_index.json</code> exists and is committed,
            or the build step copies it there.
          </p>
          <p>
            Quick fix: <code>cp data/sp500_index.json apps/web/public/data/sp500_index.json</code>
          </p>
        </main>
      </>
    );
  }

  return (
    <>
      <Head>
        <title>S&amp;P 500 Sentiment | Market Sentiment</title>
      </Head>

      <main style={{ maxWidth: 980, margin: "0 auto", padding: 16 }}>
        <h1>S&amp;P 500 Sentiment</h1>
        <p style={{ opacity: 0.8, marginTop: 4 }}>
          {name} ({symbol})
          {closeSourceSymbol ? ` · price source: ${closeSourceSymbol}` : ""}
        </p>

        <section style={{ marginTop: 18 }}>
          <h3>Sentiment and Price Analysis</h3>

          <div style={{ display: "flex", gap: 10, flexWrap: "wrap", margin: "10px 0 14px" }}>
            <button onClick={() => setMode("separate")}>Separate View</button>
            <button onClick={() => setMode("overlay")}>Overlayed View</button>
            <button onClick={() => setMode("price")}>Price Only</button>
            <button onClick={() => setMode("sentiment")}>Sentiment Only</button>
          </div>

          <LineChartSvg rows={rows} mode={mode} />
        </section>

        <section style={{ marginTop: 22 }}>
          <h3>Live Market Insights</h3>

          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 12 }}>
            <div style={{ border: "1px solid rgba(0,0,0,0.12)", borderRadius: 10, padding: 12 }}>
              <div style={{ opacity: 0.75 }}>Latest Close</div>
              <div style={{ fontSize: 22, fontWeight: 700 }}>
                {fmt2(latest?.close ?? NaN)}
              </div>
              <div style={{ opacity: 0.7, marginTop: 4 }}>{latest?.date ?? "—"}</div>
            </div>

            <div style={{ border: "1px solid rgba(0,0,0,0.12)", borderRadius: 10, padding: 12 }}>
              <div style={{ opacity: 0.75 }}>Latest Sentiment</div>
              <div style={{ fontSize: 22, fontWeight: 700 }}>
                {fmt4(latest?.sentiment ?? NaN)}
              </div>
              <div style={{ opacity: 0.7, marginTop: 4 }}>(Latest daily score)</div>
            </div>

            <div style={{ border: "1px solid rgba(0,0,0,0.12)", borderRadius: 10, padding: 12 }}>
              <div style={{ opacity: 0.75 }}>7-day sentiment average</div>
              <div style={{ fontSize: 22, fontWeight: 700 }}>{fmt4(avg7)}</div>
              <div style={{ opacity: 0.7, marginTop: 4 }}>Derived from sentiment trend</div>
            </div>

            <div style={{ border: "1px solid rgba(0,0,0,0.12)", borderRadius: 10, padding: 12 }}>
              <div style={{ opacity: 0.75 }}>Advisory Opinion</div>
              <div style={{ fontSize: 22, fontWeight: 700 }}>{advisory}</div>
              <div style={{ opacity: 0.7, marginTop: 4 }}>Simple rule-based signal</div>
            </div>
          </div>
        </section>

        <section style={{ marginTop: 22 }}>
          <h3>Notes</h3>
          <ul style={{ marginTop: 8 }}>
            <li>This page intentionally does <b>not</b> show news (index news is noisy & APIs are flaky).</li>
            <li>If you later want headlines, we can add it back behind a toggle without breaking the page.</li>
          </ul>
        </section>

        <div style={{ marginTop: 22, opacity: 0.8 }}>
          <Link href="/">← Back to Home</Link>
        </div>
      </main>
    </>
  );
}

export const getStaticProps: GetStaticProps<Sp500Props> = async () => {
  const filePath = path.join(process.cwd(), "public", "data", "sp500_index.json");

  if (!fs.existsSync(filePath)) {
    return {
      props: {
        symbol: "SPX",
        name: "S&P 500 Index",
        rows: [],
      },
    };
  }

  const raw = JSON.parse(fs.readFileSync(filePath, "utf-8"));

  const symbol = String(raw.symbol ?? "SPX");
  const name = String(raw.name ?? "S&P 500 Index");
  const daily: any[] = Array.isArray(raw.daily) ? raw.daily : [];

  const closeKey =
    daily.length
      ? (Object.keys(daily[0]).find((k) => k.startsWith("close_")) ?? "close")
      : "close";

  const closeSourceSymbol = closeKey.startsWith("close_") ? closeKey.replace("close_", "") : undefined;

  const rows: DailyRow[] = daily
    .map((r) => {
      const d = String(r.date ?? "");
      const close = Number(r[closeKey] ?? r.close);
      const sentiment = Number(r.sentiment_cap_weighted ?? r.sentiment);
      if (!d || !Number.isFinite(close) || !Number.isFinite(sentiment)) return null;
      return { date: d, close, sentiment };
    })
    .filter(Boolean) as DailyRow[];

  return {
    props: {
      symbol,
      name,
      rows,
      closeSourceSymbol,
    },
  };
};
