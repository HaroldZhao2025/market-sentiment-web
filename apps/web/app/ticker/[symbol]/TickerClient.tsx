"use client";

import { useMemo, useState } from "react";

/* ========= Props (unchanged) ========= */
export type SeriesIn = {
  date: string[];
  price: number[];     // may be empty: UI adapts
  sentiment: number[]; // may be empty: UI adapts
};

export type NewsItem = { ts: string; title: string; url: string; text?: string };

export default function TickerClient({
  symbol,
  series,
  news,
  newsTotal = 0,
}: {
  symbol: string;
  series: SeriesIn;
  news: NewsItem[];
  newsTotal?: number;
}) {
  type View = "overlay" | "price" | "sentiment";
  const [mode, setMode] = useState<View>("overlay");

  /* ----- Sanitize & align series ----- */
  const aligned = useMemo(() => {
    const n = Math.min(
      series.date?.length ?? 0,
      series.price?.length || Infinity,
      series.sentiment?.length || Infinity
    );
    const date = (series.date || []).slice(0, n);
    const price = (series.price || []).slice(0, n);
    const sentiment = (series.sentiment || []).slice(0, n);
    return { date, price, sentiment, n };
  }, [series]);

  /* ----- Helpers ----- */
  const sMA7 = useMemo(() => ma7(aligned.sentiment), [aligned.sentiment]);
  const lastS = aligned.n ? Number(aligned.sentiment.at(-1) ?? 0) : 0;
  const lastMA = aligned.n ? Number(sMA7.at(-1) ?? 0) : 0;

  const hasPrice = aligned.price.length > 0 && aligned.price.length === aligned.date.length;
  const hasSent = aligned.sentiment.length > 0 && aligned.sentiment.length === aligned.date.length;

  const drawPrice = hasPrice && (mode === "overlay" || mode === "price");
  const drawSent  = hasSent  && (mode === "overlay" || mode === "sentiment");

  return (
    <div className="mx-auto max-w-6xl px-4 py-8 space-y-8">
      {/* ===== Header + View selector ===== */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-3xl font-bold tracking-tight">Market Sentiment for {symbol}</h1>

        <div className="inline-flex items-center rounded-xl border bg-white p-1">
          <SegButton active={mode === "overlay"} onClick={() => setMode("overlay")}>
            Overlay
          </SegButton>
          {hasPrice && (
            <SegButton active={mode === "price"} onClick={() => setMode("price")}>
              Price Only
            </SegButton>
          )}
          {hasSent && (
            <SegButton active={mode === "sentiment"} onClick={() => setMode("sentiment")}>
              Sentiment Only
            </SegButton>
          )}
        </div>
      </div>

      {/* ===== Chart Card ===== */}
      <div className="rounded-2xl p-6 shadow-sm border bg-white">
        <div className="flex items-center justify-between gap-4 mb-4">
          <h3 className="font-semibold">Sentiment &amp; Price</h3>
          <Legend showSent={drawSent} showPrice={drawPrice} />
        </div>
        <Chart
          dates={aligned.date}
          price={drawPrice ? aligned.price : undefined}
          sentiment={drawSent ? aligned.sentiment : undefined}
          height={520}
        />
      </div>

      {/* ===== KPI Cards ===== */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <KpiCard
          title="Live Market Sentiment"
          value={
            <>
              {label(lastS)}{" "}
              <span className="text-neutral-500 text-lg">({lastS.toFixed(4)})</span>
            </>
          }
        />
        <KpiCard
          title="Predicted Return"
          value={`${(lastMA * 100).toFixed(4)}%`}
          sub="7-day sentiment average"
        />
        <KpiCard
          title="Advisory Opinion"
          value={recommendation(lastMA)}
          sub="Derived from sentiment MA(7)"
        />
        <KpiCard title="News Items (period)" value={newsTotal.toLocaleString()} />
      </div>

      {/* ===== Headlines (date-only) ===== */}
      <div className="rounded-2xl p-6 shadow-sm border bg-white">
        <div className="flex items-baseline justify-between">
          <h3 className="font-semibold mb-1">Recent Headlines for {symbol}</h3>
          <p className="text-xs text-neutral-500">
            Shows up to 10 latest headlines; daily sentiment uses all sources.
          </p>
        </div>

        {news?.length ? (
          <ul className="mt-2 space-y-2">
            {news.slice(0, 10).map((n, i) => (
              <li key={i} className="text-sm leading-6">
                <span className="text-neutral-500 mr-2">{toDateOnly(n.ts)}</span>
                <a
                  className="underline decoration-dotted underline-offset-2"
                  href={n.url}
                  target="_blank"
                  rel="noreferrer"
                >
                  {n.title}
                </a>
              </li>
            ))}
          </ul>
        ) : (
          <div className="text-neutral-500">No recent headlines found.</div>
        )}
      </div>
    </div>
  );
}

/* ================= UI Bits ================= */

function SegButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      className={[
        "px-3 py-1.5 text-sm rounded-lg transition",
        active ? "bg-black text-white shadow-sm" : "text-neutral-700 hover:bg-neutral-50",
      ].join(" ")}
      onClick={onClick}
    >
      {children}
    </button>
  );
}

function KpiCard({ title, value, sub }: { title: string; value: React.ReactNode; sub?: string }) {
  return (
    <div className="rounded-2xl p-5 shadow-sm border bg-white">
      <div className="text-sm text-neutral-500 mb-1">{title}</div>
      <div className="text-2xl md:text-3xl font-semibold">{value}</div>
      {sub ? <div className="text-xs text-neutral-500 mt-1">{sub}</div> : null}
    </div>
  );
}

function Legend({ showSent, showPrice }: { showSent: boolean; showPrice: boolean }) {
  return (
    <div className="flex items-center gap-4 text-sm">
      {showSent && (
        <span className="inline-flex items-center gap-2">
          <span className="inline-block h-0.5 w-5 bg-blue-500" />
          <span>Sentiment (normalized)</span>
        </span>
      )}
      {showPrice && (
        <span className="inline-flex items-center gap-2">
          <span className="inline-block h-0.5 w-5 bg-emerald-500" />
          <span>Price (normalized)</span>
        </span>
      )}
    </div>
  );
}

/* ================= Chart (pure SVG) ================= */

function Chart({
  dates,
  price,
  sentiment,
  height = 520,
  width = 980,
}: {
  dates: string[];
  price?: number[];
  sentiment?: number[];
  height?: number;
  width?: number;
}) {
  const pad = { t: 18, r: 24, b: 36, l: 44 };
  const W = width;
  const H = height;
  const innerW = W - pad.l - pad.r;
  const innerH = H - pad.t - pad.b;
  const n = dates.length;

  const step = n > 1 ? innerW / (n - 1) : innerW;

  const norm = (xs?: number[]) => {
    if (!xs || xs.length === 0) return [] as number[];
    const lo = Math.min(...xs);
    const hi = Math.max(...xs);
    if (hi === lo) return xs.map(() => 0.5);
    return xs.map((v) => (v - lo) / (hi - lo));
  };

  const pN = norm(price);
  const sN = norm(sentiment);

  const pathFrom = (xs: number[], color: string) => {
    if (!xs.length) return null;
    let d = "";
    for (let i = 0; i < xs.length; i++) {
      const x = pad.l + i * step;
      const y = pad.t + (1 - xs[i]) * innerH;
      d += (i === 0 ? "M" : "L") + x + "," + y;
    }
    return <path d={d} fill="none" stroke={color} strokeWidth={2} />;
  };

  // Y grid (0%, 25%, 50%, 75%, 100% on normalized scale)
  const ticks = [0, 0.25, 0.5, 0.75, 1];

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full rounded-xl border bg-white">
      {/* Frame */}
      <rect x={pad.l} y={pad.t} width={innerW} height={innerH} fill="none" stroke="#e5e7eb" />

      {/* Grid */}
      {ticks.map((t, i) => {
        const y = pad.t + (1 - t) * innerH;
        return <line key={i} x1={pad.l} x2={pad.l + innerW} y1={y} y2={y} stroke="#f1f5f9" />;
      })}

      {/* X labels (start / end) */}
      <text x={pad.l} y={H - 10} fontSize="11" fill="#6b7280">
        {dates[0] || ""}
      </text>
      <text x={pad.l + innerW - 54} y={H - 10} fontSize="11" fill="#6b7280" textAnchor="start">
        {dates.at(-1) || ""}
      </text>

      {/* Series */}
      {sN.length ? pathFrom(sN, "#3b82f6") : null}
      {pN.length ? pathFrom(pN, "#10b981") : null}
    </svg>
  );
}

/* ================= Small utilities ================= */

function ma7(arr: number[]) {
  const out: number[] = [];
  let run = 0;
  for (let i = 0; i < arr.length; i++) {
    const v = Number(arr[i] || 0);
    run += v;
    if (i >= 7) run -= Number(arr[i - 7] || 0);
    out.push(i >= 6 ? run / 7 : NaN);
  }
  return out;
}

function label(v: number) {
  if (v >= 0.4) return "Strong Positive";
  if (v >= 0.1) return "Positive";
  if (v <= -0.4) return "Strong Negative";
  if (v <= -0.1) return "Negative";
  return "Neutral";
}
function recommendation(v: number) {
  if (v >= 0.4) return "Strong Buy";
  if (v >= 0.1) return "Buy";
  if (v <= -0.4) return "Strong Sell";
  if (v <= -0.1) return "Sell";
  return "Hold";
}
function toDateOnly(x: string) {
  // Try ISO first
  const d = new Date(x);
  if (!isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  // Otherwise extract YYYY-MM-DD if present
  const m = String(x).match(/\d{4}-\d{2}-\d{2}/);
  return m ? m[0] : String(x).slice(0, 10);
}
