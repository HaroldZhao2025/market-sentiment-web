// apps/web/pages/sp500.tsx
import type { GetStaticProps, InferGetStaticPropsType } from "next";
import Head from "next/head";
import Link from "next/link";
import fs from "fs";
import path from "path";
import React, { useMemo, useState } from "react";

type DailyRow = {
  date: string;      // YYYY-MM-DD
  close: number;     // index close
  sentiment: number; // cap-weighted sentiment
};

type Sp500Props = {
  symbol: string;
  name: string;
  rows: DailyRow[];
  closeSourceSymbol?: string; // e.g. "^GSPC"
  dataPathUsed?: string;      // for debugging
};

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

// very light, explainable signal (you can tune later)
function advisoryFromSentiment(avg7: number) {
  if (!Number.isFinite(avg7)) return "Hold";
  if (avg7 >= 0.08) return "Buy";
  if (avg7 <= -0.08) return "Sell";
  return "Hold";
}

function ChartSvg(props: { rows: DailyRow[]; mode: "overlay" | "price" | "sentiment" }) {
  const { rows, mode } = props;

  const W = 920;
  const H = 320;
  const pad = 36;

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

  const y01 = (t01: number) => pad + (1 - Math.max(0, Math.min(1, t01))) * (H - 2 * pad);

  const p01 = prices.map((v) => (pMax === pMin ? 0.5 : (v - pMin) / (pMax - pMin)));
  const s01 = sents.map((v) => (sMax === sMin ? 0.5 : (v - sMin) / (sMax - sMin)));

  const mkPath = (ys01: number[]) =>
    ys01
      .map((v, i) => `${i === 0 ? "M" : "L"} ${xOf(i).toFixed(2)} ${y01(v).toFixed(2)}`)
      .join(" ");

  const pricePath = mkPath(p01);
  const sentPath = mkPath(s01);

  const showPrice = mode === "overlay" || mode === "price";
  const showSent = mode === "overlay" || mode === "sentiment";

  const tickIdx = [0, Math.floor((rows.length - 1) * 0.33), Math.floor((rows.length - 1) * 0.66), rows.length - 1]
    .filter((i, idx, arr) => i >= 0 && i < rows.length && arr.indexOf(i) === idx);

  return (
    <div style={{ width: "100%", overflowX: "auto" }}>
      <svg width={W} height={H} viewBox={`0 0 ${W} ${H}`} role="img" aria-label="S&P 500 sentiment chart">
        <rect x="1" y="1" width={W - 2} height={H - 2} fill="none" stroke="currentColor" opacity="0.15" />

        <line x1={pad} y1={H - pad} x2={W - pad} y2={H - pad} stroke="currentColor" opacity="0.2" />
        <line x1={pad} y1={pad} x2={pad} y2={H - pad} stroke="currentColor" opacity="0.2" />

        {tickIdx.map((i) => (
          <g key={i}>
            <line x1={xOf(i)} y1={H - pad} x2={xOf(i)} y2={H - pad + 6} stroke="currentColor" opacity="0.25" />
            <text x={xOf(i)} y={H - pad + 20} fontSize="10" textAnchor="middle" opacity="0.7">
              {rows[i].date}
            </text>
          </g>
        ))}

        {showPrice && (
          <path d={pricePath} fill="none" stroke="currentColor" strokeWidth="2" opacity="0.9" />
        )}
        {showSent && (
          <path d={sentPath} fill="none" stroke="currentColor" strokeWidth="2" opacity="0.55" strokeDasharray="4 3" />
        )}

        <text x={pad} y={16} fontSize="12" opacity="0.85">
          Price (solid) · Sentiment (dashed) — normalized
        </text>
      </svg>
    </div>
  );
}

export default function SP500Page(props: InferGetStaticPropsType<typeof getStaticProps>) {
  const { symbol, name, rows, closeSourceSymbol, dataPathUsed } = props;
  const [mode, setMode] = useState<"overlay" | "price" | "sentiment">("overlay");

  const latest = rows.length ? rows[rows.length - 1] : null;

  const avg7 = useMemo(() => mean(rows.slice(-7).map((r) => r.sentiment)), [rows]);
  const advisory = useMemo(() => advisoryFromSentiment(avg7), [avg7]);

  if (!rows.length) {
    return (
      <>
        <Head>
          <title>S&P 500 | Market Sentiment</title>
        </Head>
        <main style={{ maxWidth: 980, margin: "0 auto", padding: 16 }}>
          <h1>S&amp;P 500</h1>
          <p style={{ opacity: 0.85 }}>
            No SP500 index data found at build time.
          </p>

          <div style={{ marginTop: 10, padding: 12, border: "1px solid rgba(0,0,0,0.12)", borderRadius: 10 }}>
            <div style={{ fontWeight: 700 }}>Expected file location</div>
            <ul style={{ marginTop: 8 }}>
              <li><code>data/SPX/sp500_index.json</code> (repo root)</li>
              <li><code>apps/web/public/data/sp500_index.json</code> (fallback)</li>
            </ul>
            <div style={{ marginTop: 8, opacity: 0.8 }}>
              Fix: ensure your CI step generates/copies the file before <code>npm run export</code>.
            </div>
          </div>

          <div style={{ marginTop: 20, opacity: 0.8 }}>
            <Link href="/">← Back to Home</Link>
          </div>
        </main>
      </>
    );
  }

  return (
    <>
      <Head>
        <title>S&amp;P 500 | Market Sentiment</title>
      </Head>

      <main style={{ maxWidth: 980, margin: "0 auto", padding: 16 }}>
        <h1>S&amp;P 500</h1>
        <p style={{ opacity: 0.8, marginTop: 4 }}>
          {name} ({symbol})
          {closeSourceSymbol ? ` · price: ${closeSourceSymbol}` : ""}
          {dataPathUsed ? ` · data: ${dataPathUsed}` : ""}
        </p>

        <section style={{ marginTop: 14 }}>
          <h3>Sentiment and Price Analysis</h3>

          <div style={{ display: "flex", gap: 10, flexWrap: "wrap", margin: "10px 0 14px" }}>
            <button onClick={() => setMode("overlay")}>Overlayed View</button>
            <button onClick={() => setMode("price")}>Price Only</button>
            <button onClick={() => setMode("sentiment")}>Sentiment Only</button>
          </div>

          <ChartSvg rows={rows} mode={mode} />
        </section>

        <section style={{ marginTop: 18 }}>
          <h3>Live Market Insights</h3>

          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 12 }}>
            <div style={{ border: "1px solid rgba(0,0,0,0.12)", borderRadius: 10, padding: 12 }}>
              <div style={{ opacity: 0.75 }}>Latest Close</div>
              <div style={{ fontSize: 22, fontWeight: 700 }}>{fmt2(latest?.close ?? NaN)}</div>
              <div style={{ opacity: 0.7, marginTop: 4 }}>{latest?.date ?? "—"}</div>
            </div>

            <div style={{ border: "1px solid rgba(0,0,0,0.12)", borderRadius: 10, padding: 12 }}>
              <div style={{ opacity: 0.75 }}>Latest Sentiment</div>
              <div style={{ fontSize: 22, fontWeight: 700 }}>{fmt4(latest?.sentiment ?? NaN)}</div>
              <div style={{ opacity: 0.7, marginTop: 4 }}>Capital-weighted daily score</div>
            </div>

            <div style={{ border: "1px solid rgba(0,0,0,0.12)", borderRadius: 10, padding: 12 }}>
              <div style={{ opacity: 0.75 }}>7-day Avg Sentiment</div>
              <div style={{ fontSize: 22, fontWeight: 700 }}>{fmt4(avg7)}</div>
              <div style={{ opacity: 0.7, marginTop: 4 }}>Smooth trend indicator</div>
            </div>

            <div style={{ border: "1px solid rgba(0,0,0,0.12)", borderRadius: 10, padding: 12 }}>
              <div style={{ opacity: 0.75 }}>Advisory Opinion</div>
              <div style={{ fontSize: 22, fontWeight: 700 }}>{advisory}</div>
              <div style={{ opacity: 0.7, marginTop: 4 }}>Rule-based, for demo</div>
            </div>
          </div>
        </section>

        {/* ✅ Expand / collapse: how sentiment is calculated */}
        <section style={{ marginTop: 18 }}>
          <details style={{ border: "1px solid rgba(0,0,0,0.12)", borderRadius: 10, padding: 12 }}>
            <summary style={{ cursor: "pointer", fontWeight: 700 }}>
              ❓ how sp500 sentiment is calculated?
            </summary>

            <div style={{ marginTop: 10, lineHeight: 1.55 }}>
              <p style={{ marginTop: 0 }}>
                The S&amp;P 500 sentiment shown here is a <b>capital-weighted</b> aggregation of
                constituent-level daily sentiment scores.
              </p>

              <p>
                1) For each constituent stock <code>i</code> on date <code>t</code>, the pipeline computes a daily sentiment score
                <code> s(i,t) </code> based on that stock’s news items collected for the day (using this project’s sentiment model).
              </p>

              <p>
                2) Each stock is assigned a market-cap weight <code>w(i,t)</code>. We normalize weights over the set of stocks
                that have valid data on that date.
              </p>

              <p style={{ marginBottom: 0 }}>
                3) The index-level sentiment is computed as:
              </p>

              <pre style={{ marginTop: 8, padding: 10, borderRadius: 8, background: "rgba(0,0,0,0.04)", overflowX: "auto" }}>
{`S(t) = Σ_i w(i,t) · s(i,t)
where  w(i,t) = MCAP(i,t) / Σ_j MCAP(j,t)   (re-normalized among available stocks)`}
              </pre>

              <p style={{ marginTop: 10, opacity: 0.85 }}>
                Interpretation: large-cap constituents contribute more to the overall index sentiment. This tends to produce a more stable,
                index-like signal than an equal-weighted average.
              </p>

              <p style={{ marginTop: 8, opacity: 0.85 }}>
                Note: if a constituent has missing data on a given date, it is excluded from that day’s aggregation and the remaining weights
                are re-normalized.
              </p>
            </div>
          </details>
        </section>

        <div style={{ marginTop: 18, opacity: 0.8 }}>
          <Link href="/">← Back to Home</Link>
        </div>
      </main>
    </>
  );
}

export const getStaticProps: GetStaticProps<Sp500Props> = async () => {
  // IMPORTANT:
  // During `next build` / `next export`, process.cwd() is usually `apps/web`.
  // We want to read either:
  //  1) repo-root: data/SPX/sp500_index.json
  //  2) fallback: apps/web/public/data/sp500_index.json

  const cwd = process.cwd();

  const candidates = [
    // repo root relative to apps/web
    path.join(cwd, "..", "..", "data", "SPX", "sp500_index.json"),
    // fallback
    path.join(cwd, "public", "data", "sp500_index.json"),
  ];

  let used: string | null = null;
  let raw: any = null;

  for (const p of candidates) {
    if (fs.existsSync(p)) {
      used = p;
      raw = JSON.parse(fs.readFileSync(p, "utf-8"));
      break;
    }
  }

  if (!raw) {
    return {
      props: {
        symbol: "SPX",
        name: "S&P 500 Index",
        rows: [],
        dataPathUsed: used ?? undefined,
      },
    };
  }

  const symbol = String(raw.symbol ?? "SPX");
  const name = String(raw.name ?? "S&P 500 Index");
  const daily: any[] = Array.isArray(raw.daily) ? raw.daily : [];

  // detect close field: close_^GSPC / close_^SPX / close_SPY ...
  const closeKey =
    daily.length
      ? (Object.keys(daily[0]).find((k) => k.startsWith("close_")) ?? "close")
      : "close";

  const closeSourceSymbol = closeKey.startsWith("close_")
    ? closeKey.replace("close_", "")
    : undefined;

  const rows: DailyRow[] = daily
    .map((r) => {
      const date = String(r.date ?? "");
      const close = Number(r[closeKey] ?? r.close);
      const sentiment = Number(r.sentiment_cap_weighted ?? r.sentiment);
      if (!date || !Number.isFinite(close) || !Number.isFinite(sentiment)) return null;
      return { date, close, sentiment };
    })
    .filter(Boolean) as DailyRow[];

  return {
    props: {
      symbol,
      name,
      rows,
      closeSourceSymbol,
      dataPathUsed: used ?? undefined,
    },
  };
};

