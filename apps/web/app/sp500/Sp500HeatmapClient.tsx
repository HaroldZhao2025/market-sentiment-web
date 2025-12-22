"use client";

import Link from "next/link";
import { useMemo, useState } from "react";

type HeatmapTile = {
  symbol: string;
  name?: string;
  sector?: string;
  industry?: string;
  market_cap?: number;
  weight?: number;
  date?: string;
  price?: number | null;
  return_1d?: number | null;
  sentiment?: number | null;
  n_total?: number | null;
};

type Sp500HeatmapFile = {
  symbol: string;
  name: string;
  asof: string;
  updated_at_utc?: string;
  stats?: Record<string, unknown>;
  tiles: HeatmapTile[];
};

type Props = { data: Sp500HeatmapFile };

type Rect = HeatmapTile & { x: number; y: number; w: number; h: number; key: string };

function clamp(x: number, a: number, b: number) {
  return Math.max(a, Math.min(b, x));
}

function mix(a: number, b: number, t: number) {
  return Math.round(a + (b - a) * t);
}

function bgForMetric(v: number | null | undefined, mode: "sentiment" | "return"): string {
  if (v == null || !Number.isFinite(v)) return "rgb(235,235,235)";

  if (mode === "sentiment") {
    // sentiment typically small; scale to look finviz-like
    const s = clamp(v, -0.6, 0.6) / 0.6; // [-1,1]
    if (s >= 0) {
      const t = clamp(s, 0, 1);
      // white -> green
      const r = mix(245, 0, t);
      const g = mix(245, 170, t);
      const b = mix(245, 0, t);
      return `rgb(${r},${g},${b})`;
    } else {
      const t = clamp(-s, 0, 1);
      // white -> red
      const r = mix(245, 200, t);
      const g = mix(245, 0, t);
      const b = mix(245, 0, t);
      return `rgb(${r},${g},${b})`;
    }
  }

  // return mode: map ±5% roughly
  const s = clamp(v, -0.05, 0.05) / 0.05;
  if (s >= 0) {
    const t = clamp(s, 0, 1);
    const r = mix(245, 0, t);
    const g = mix(245, 170, t);
    const b = mix(245, 0, t);
    return `rgb(${r},${g},${b})`;
  } else {
    const t = clamp(-s, 0, 1);
    const r = mix(245, 200, t);
    const g = mix(245, 0, t);
    const b = mix(245, 0, t);
    return `rgb(${r},${g},${b})`;
  }
}

function textColor(bgRgb: string) {
  // crude luminance check
  const m = bgRgb.match(/rgb\((\d+),(\d+),(\d+)\)/);
  if (!m) return "text-black";
  const r = Number(m[1]), g = Number(m[2]), b = Number(m[3]);
  const lum = 0.2126 * r + 0.7152 * g + 0.0722 * b;
  return lum < 120 ? "text-white" : "text-black";
}

function fmtMoney(x: number | null | undefined) {
  if (x == null || !Number.isFinite(x)) return "—";
  return x.toFixed(1);
}
function fmtNum(x: number | null | undefined, d = 2) {
  if (x == null || !Number.isFinite(x)) return "—";
  return x.toFixed(d);
}
function fmtPct(x: number | null | undefined) {
  if (x == null || !Number.isFinite(x)) return "—";
  return `${(x * 100).toFixed(2)}%`;
}

// Simple binary treemap (stable, no extra deps)
function layoutBinary(items: HeatmapTile[], x: number, y: number, w: number, h: number): Rect[] {
  const arr = items
    .filter((t) => Number.isFinite(Number(t.market_cap)) && Number(t.market_cap) > 0)
    .slice()
    .sort((a, b) => Number(b.market_cap || 0) - Number(a.market_cap || 0));

  const total = arr.reduce((s, t) => s + Number(t.market_cap || 0), 0);
  if (!arr.length || total <= 0) return [];

  function rec(list: HeatmapTile[], x0: number, y0: number, w0: number, h0: number): Rect[] {
    if (list.length === 1) {
      const t = list[0];
      return [{ ...t, x: x0, y: y0, w: w0, h: h0, key: t.symbol }];
    }
    const sum = list.reduce((s, t) => s + Number(t.market_cap || 0), 0);
    if (sum <= 0) return [];

    const horizontal = w0 < h0; // if tall, split horizontally; else vertically
    let acc = 0;
    let k = 0;
    for (; k < list.length; k++) {
      acc += Number(list[k].market_cap || 0);
      if (acc >= sum / 2) break;
    }
    const left = list.slice(0, Math.max(1, k + 1));
    const right = list.slice(Math.max(1, k + 1));

    const sumL = left.reduce((s, t) => s + Number(t.market_cap || 0), 0);

    if (!right.length) {
      // fallback
      return left.map((t) => ({ ...t, x: x0, y: y0, w: w0, h: h0, key: t.symbol }));
    }

    if (horizontal) {
      const hL = h0 * (sumL / sum);
      return [
        ...rec(left, x0, y0, w0, hL),
        ...rec(right, x0, y0 + hL, w0, h0 - hL),
      ];
    } else {
      const wL = w0 * (sumL / sum);
      return [
        ...rec(left, x0, y0, wL, h0),
        ...rec(right, x0 + wL, y0, w0 - wL, h0),
      ];
    }
  }

  return rec(arr, x, y, w, h);
}

export default function Sp500HeatmapClient({ data }: Props) {
  const [sector, setSector] = useState<string>("All sectors");
  const [industry, setIndustry] = useState<string>("All industries");
  const [mode, setMode] = useState<"sentiment" | "return">("sentiment");

  const tiles = data.tiles || [];

  const sectors = useMemo(() => {
    const s = new Set<string>();
    for (const t of tiles) s.add(t.sector || "Unknown");
    return ["All sectors", ...Array.from(s).sort((a, b) => a.localeCompare(b))];
  }, [tiles]);

  const industries = useMemo(() => {
    const s = new Set<string>();
    for (const t of tiles) {
      const sec = t.sector || "Unknown";
      if (sector !== "All sectors" && sec !== sector) continue;
      s.add(t.industry || "Unknown");
    }
    return ["All industries", ...Array.from(s).sort((a, b) => a.localeCompare(b))];
  }, [tiles, sector]);

  const filtered = useMemo(() => {
    return tiles.filter((t) => {
      const sec = t.sector || "Unknown";
      const ind = t.industry || "Unknown";
      if (sector !== "All sectors" && sec !== sector) return false;
      if (industry !== "All industries" && ind !== industry) return false;
      return true;
    });
  }, [tiles, sector, industry]);

  const rects = useMemo(() => layoutBinary(filtered, 0, 0, 1000, 560), [filtered]);

  return (
    <div className="border rounded-xl bg-white overflow-hidden">
      {/* Header / Controls (finviz-like) */}
      <div className="px-4 py-3 border-b bg-gray-50 flex flex-wrap items-center justify-between gap-3">
        <div className="text-sm">
          <span className="text-gray-500">All sectors</span>
          {sector !== "All sectors" && (
            <>
              <span className="text-gray-400"> / </span>
              <button className="underline" onClick={() => { setIndustry("All industries"); }}>
                {sector}
              </button>
            </>
          )}
          {industry !== "All industries" && (
            <>
              <span className="text-gray-400"> / </span>
              <span className="font-semibold">{industry}</span>
            </>
          )}
          <span className="ml-3 text-gray-500">•</span>
          <span className="ml-3 text-gray-600">
            Latest trading day: <span className="font-medium">{data.asof}</span>
          </span>
        </div>

        <div className="flex flex-wrap items-center gap-2 text-sm">
          <select
            className="border rounded-md px-2 py-1 bg-white"
            value={sector}
            onChange={(e) => {
              setSector(e.target.value);
              setIndustry("All industries");
            }}
          >
            {sectors.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>

          <select
            className="border rounded-md px-2 py-1 bg-white"
            value={industry}
            onChange={(e) => setIndustry(e.target.value)}
          >
            {industries.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>

          <div className="border rounded-md overflow-hidden flex">
            <button
              className={`px-3 py-1 ${mode === "sentiment" ? "bg-black text-white" : "bg-white"}`}
              onClick={() => setMode("sentiment")}
              title="Color by sentiment"
            >
              Sentiment
            </button>
            <button
              className={`px-3 py-1 ${mode === "return" ? "bg-black text-white" : "bg-white"}`}
              onClick={() => setMode("return")}
              title="Color by 1D return"
            >
              Return
            </button>
          </div>
        </div>
      </div>

      {/* Map */}
      <div className="p-3 bg-white">
        <div className="relative w-full" style={{ height: "70vh", minHeight: 420 }}>
          {rects.map((r) => {
            const pad = 1.5; // finviz-like white gutters
            const left = (r.x / 1000) * 100;
            const top = (r.y / 560) * 100;
            const width = (r.w / 1000) * 100;
            const height = (r.h / 560) * 100;

            const metricValue = mode === "sentiment" ? (r.sentiment ?? null) : (r.return_1d ?? null);
            const bg = bgForMetric(metricValue as any, mode);
            const tc = textColor(bg);

            const showDetail = width > 6 && height > 6;

            return (
              <Link
                key={r.key}
                href={`/ticker/${r.symbol}`}
                className={`absolute rounded-sm border border-white/80 ${tc} hover:brightness-95`}
                style={{
                  left: `calc(${left}% + ${pad}px)`,
                  top: `calc(${top}% + ${pad}px)`,
                  width: `calc(${width}% - ${pad * 2}px)`,
                  height: `calc(${height}% - ${pad * 2}px)`,
                  background: bg,
                  textDecoration: "none",
                }}
                title={[
                  r.symbol,
                  r.name ? `Name: ${r.name}` : null,
                  `Sector: ${r.sector || "Unknown"}`,
                  `Industry: ${r.industry || "Unknown"}`,
                  `Price: ${fmtMoney(r.price ?? null)}`,
                  `Return(1D): ${fmtPct(r.return_1d ?? null)}`,
                  `Sentiment: ${fmtNum(r.sentiment ?? null, 4)}`,
                  r.n_total != null ? `Articles: ${r.n_total}` : null,
                ]
                  .filter(Boolean)
                  .join("\n")}
              >
                <div className="w-full h-full p-2 flex flex-col justify-center items-center text-center">
                  <div className="font-semibold leading-none" style={{ fontSize: showDetail ? 14 : 11 }}>
                    {r.symbol}
                  </div>
                  {showDetail && (
                    <div className="mt-1 text-[12px] opacity-90">
                      ({fmtMoney(r.price ?? null)}, {fmtNum(r.sentiment ?? null, 2)})
                    </div>
                  )}
                </div>
              </Link>
            );
          })}
        </div>

        <div className="mt-2 text-xs text-gray-500">
          Tip: click tiles to drill into a ticker page. Use filters to focus by sector/industry.
        </div>
      </div>
    </div>
  );
}
