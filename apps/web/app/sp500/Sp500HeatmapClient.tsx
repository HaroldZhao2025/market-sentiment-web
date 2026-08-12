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
type Mode = "contribution" | "sentiment" | "return";
type Rect = HeatmapTile & { x: number; y: number; w: number; h: number; key: string };

function clamp(x: number, a: number, b: number) {
  return Math.max(a, Math.min(b, x));
}

function finite(x: unknown): number | null {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function contribution(tile: HeatmapTile): number | null {
  const w = finite(tile.weight);
  const s = finite(tile.sentiment);
  if (w == null || s == null) return null;
  return w * s;
}

function metricValue(tile: HeatmapTile, mode: Mode): number | null {
  if (mode === "sentiment") return finite(tile.sentiment);
  if (mode === "return") return finite(tile.return_1d);
  return contribution(tile);
}

function metricLabel(mode: Mode) {
  if (mode === "sentiment") return "Sentiment";
  if (mode === "return") return "1D return";
  return "Index contribution";
}

function scaleFor(mode: Mode) {
  if (mode === "sentiment") return 0.45;
  if (mode === "return") return 0.04;
  return 0.008;
}

function bgForMetric(v: number | null, mode: Mode): string {
  if (v == null) return "linear-gradient(145deg, #1b1b20, #141417)";
  const t = clamp(Math.abs(v) / scaleFor(mode), 0, 1);
  if (v >= 0) {
    const alpha = 0.13 + t * 0.67;
    return `linear-gradient(145deg, rgba(16,185,129,${alpha}), rgba(5,46,40,${0.55 + t * 0.25}))`;
  }
  const alpha = 0.13 + t * 0.67;
  return `linear-gradient(145deg, rgba(244,63,94,${alpha}), rgba(76,5,25,${0.55 + t * 0.25}))`;
}

function fmtMoney(x: number | null | undefined) {
  if (x == null || !Number.isFinite(x)) return "—";
  return x.toFixed(2);
}

function fmtNum(x: number | null | undefined, d = 3) {
  if (x == null || !Number.isFinite(x)) return "—";
  return x.toFixed(d);
}

function fmtPct(x: number | null | undefined, d = 2) {
  if (x == null || !Number.isFinite(x)) return "—";
  return `${(x * 100).toFixed(d)}%`;
}

function metricDisplay(tile: HeatmapTile, mode: Mode) {
  const v = metricValue(tile, mode);
  if (v == null) return "—";
  if (mode === "return") return fmtPct(v, 2);
  if (mode === "contribution") return `${v >= 0 ? "+" : ""}${(v * 10000).toFixed(2)} bps`;
  return `${v >= 0 ? "+" : ""}${v.toFixed(3)}`;
}

// Dependency-free, stable binary treemap. Area remains proportional to market cap.
function layoutBinary(items: HeatmapTile[], x: number, y: number, w: number, h: number): Rect[] {
  const arr = items
    .filter((t) => Number.isFinite(Number(t.market_cap)) && Number(t.market_cap) > 0)
    .slice()
    .sort((a, b) => Number(b.market_cap || 0) - Number(a.market_cap || 0));

  if (!arr.length) return [];

  function rec(list: HeatmapTile[], x0: number, y0: number, w0: number, h0: number): Rect[] {
    if (list.length === 1) {
      const t = list[0];
      return [{ ...t, x: x0, y: y0, w: w0, h: h0, key: t.symbol }];
    }
    const sum = list.reduce((s, t) => s + Number(t.market_cap || 0), 0);
    if (sum <= 0) return [];

    let acc = 0;
    let split = 0;
    for (; split < list.length; split++) {
      acc += Number(list[split].market_cap || 0);
      if (acc >= sum / 2) break;
    }
    const left = list.slice(0, Math.max(1, split + 1));
    const right = list.slice(Math.max(1, split + 1));
    if (!right.length) return rec(left, x0, y0, w0, h0);

    const leftWeight = left.reduce((s, t) => s + Number(t.market_cap || 0), 0) / sum;
    if (w0 >= h0) {
      const wLeft = w0 * leftWeight;
      return [
        ...rec(left, x0, y0, wLeft, h0),
        ...rec(right, x0 + wLeft, y0, w0 - wLeft, h0),
      ];
    }
    const hLeft = h0 * leftWeight;
    return [
      ...rec(left, x0, y0, w0, hLeft),
      ...rec(right, x0, y0 + hLeft, w0, h0 - hLeft),
    ];
  }

  return rec(arr, x, y, w, h);
}

function RankList({
  title,
  rows,
  positive,
}: {
  title: string;
  rows: HeatmapTile[];
  positive: boolean;
}) {
  return (
    <div className="rounded-2xl border border-white/10 bg-white/[0.025] p-4">
      <div className="mb-3 text-xs font-semibold uppercase tracking-[0.14em] text-neutral-500">{title}</div>
      <div className="space-y-2">
        {rows.length ? rows.map((tile) => {
          const c = contribution(tile) ?? 0;
          return (
            <Link
              key={tile.symbol}
              href={`/ticker/${tile.symbol}`}
              className="flex items-center justify-between gap-3 rounded-xl px-2 py-2 transition hover:bg-white/[0.05]"
            >
              <div className="min-w-0">
                <div className="font-semibold text-white">{tile.symbol}</div>
                <div className="truncate text-[11px] text-neutral-600">{tile.sector || "Unknown sector"}</div>
              </div>
              <div className={`text-right font-mono text-xs ${positive ? "text-emerald-300" : "text-rose-300"}`}>
                {c >= 0 ? "+" : ""}{(c * 10000).toFixed(2)} bps
              </div>
            </Link>
          );
        }) : <div className="text-xs text-neutral-600">No observed contributors.</div>}
      </div>
    </div>
  );
}

export default function Sp500HeatmapClient({ data }: Props) {
  const [sector, setSector] = useState("All sectors");
  const [mode, setMode] = useState<Mode>("contribution");
  const tiles = data.tiles || [];

  const sectors = useMemo(() => {
    const set = new Set<string>();
    tiles.forEach((t) => set.add(t.sector || "Unknown"));
    return ["All sectors", ...Array.from(set).sort()];
  }, [tiles]);

  const filtered = useMemo(
    () => tiles.filter((t) => sector === "All sectors" || (t.sector || "Unknown") === sector),
    [tiles, sector]
  );

  const rects = useMemo(() => layoutBinary(filtered, 0, 0, 1200, 650), [filtered]);

  const observed = useMemo(
    () => filtered.filter((t) => finite(t.sentiment) != null),
    [filtered]
  );

  const positive = useMemo(
    () => observed.slice().sort((a, b) => (contribution(b) ?? -Infinity) - (contribution(a) ?? -Infinity)).filter((t) => (contribution(t) ?? 0) > 0).slice(0, 6),
    [observed]
  );
  const negative = useMemo(
    () => observed.slice().sort((a, b) => (contribution(a) ?? Infinity) - (contribution(b) ?? Infinity)).filter((t) => (contribution(t) ?? 0) < 0).slice(0, 6),
    [observed]
  );

  const observedWeight = observed.reduce((s, t) => s + Math.max(0, finite(t.weight) ?? 0), 0);

  return (
    <div className="space-y-4">
      <div className="ambient-panel overflow-hidden">
        <div className="flex flex-col gap-4 border-b border-white/10 px-5 py-4 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <div className="eyebrow">Constituent intelligence</div>
            <div className="mt-1 flex flex-wrap items-baseline gap-3">
              <h3 className="text-lg font-semibold text-white">S&P 500 constituent map</h3>
              <span className="text-xs text-neutral-600">As of {data.asof}</span>
            </div>
            <p className="mt-1 text-xs text-neutral-500">
              Area reflects market cap. Color reflects {metricLabel(mode).toLowerCase()}.
            </p>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <select
              className="rounded-xl border border-white/10 bg-neutral-900 px-3 py-2 text-xs text-neutral-300 outline-none"
              value={sector}
              onChange={(e) => setSector(e.target.value)}
            >
              {sectors.map((s) => <option key={s} value={s}>{s}</option>)}
            </select>
            <div className="flex rounded-xl border border-white/10 bg-black/30 p-1">
              {(["contribution", "sentiment", "return"] as Mode[]).map((m) => (
                <button
                  key={m}
                  type="button"
                  onClick={() => setMode(m)}
                  className={`rounded-lg px-3 py-1.5 text-xs font-medium transition ${
                    mode === m ? "bg-white/10 text-white" : "text-neutral-500 hover:text-neutral-300"
                  }`}
                >
                  {m === "contribution" ? "Contribution" : m === "sentiment" ? "Sentiment" : "1D Return"}
                </button>
              ))}
            </div>
          </div>
        </div>

        <div className="grid gap-0 xl:grid-cols-[minmax(0,1fr)_260px]">
          <div className="p-3 md:p-4">
            <div className="relative min-h-[470px] w-full overflow-hidden rounded-2xl border border-white/10 bg-black/30" style={{ height: "68vh", maxHeight: 720 }}>
              {rects.map((r) => {
                const gap = 2;
                const left = (r.x / 1200) * 100;
                const top = (r.y / 650) * 100;
                const width = (r.w / 1200) * 100;
                const height = (r.h / 650) * 100;
                const value = metricValue(r, mode);
                const showMetric = width > 5 && height > 6;
                const showName = width > 10 && height > 12;

                return (
                  <Link
                    key={r.key}
                    href={`/ticker/${r.symbol}`}
                    className="group absolute overflow-hidden rounded-lg border border-white/[0.07] shadow-inner shadow-white/[0.03] transition duration-200 hover:z-20 hover:scale-[1.012] hover:border-white/30 hover:brightness-110"
                    style={{
                      left: `calc(${left}% + ${gap}px)`,
                      top: `calc(${top}% + ${gap}px)`,
                      width: `calc(${width}% - ${gap * 2}px)`,
                      height: `calc(${height}% - ${gap * 2}px)`,
                      background: bgForMetric(value, mode),
                    }}
                    title={[
                      `${r.symbol} — ${r.name || ""}`,
                      r.sector || "Unknown sector",
                      `Weight: ${fmtPct(r.weight, 2)}`,
                      `Price: ${fmtMoney(r.price)}`,
                      `1D return: ${fmtPct(r.return_1d)}`,
                      `Sentiment: ${fmtNum(r.sentiment, 4)}`,
                      `Contribution: ${contribution(r) == null ? "—" : `${((contribution(r) || 0) * 10000).toFixed(2)} bps`}`,
                      r.n_total != null ? `Unique articles: ${r.n_total}` : null,
                    ].filter(Boolean).join("\n")}
                  >
                    <div className="flex h-full flex-col justify-between p-2.5">
                      <div>
                        <div className="font-semibold leading-none text-white drop-shadow-sm" style={{ fontSize: showName ? 14 : 11 }}>
                          {r.symbol}
                        </div>
                        {showName ? <div className="mt-1 truncate text-[10px] text-white/60">{r.name}</div> : null}
                      </div>
                      {showMetric ? (
                        <div className="font-mono text-[10px] font-semibold text-white/85 md:text-[11px]">
                          {metricDisplay(r, mode)}
                        </div>
                      ) : null}
                    </div>
                  </Link>
                );
              })}
            </div>

            <div className="mt-3 flex flex-wrap items-center justify-between gap-3 text-[11px] text-neutral-600">
              <span>{filtered.length} constituents shown · {observed.length} with observed sentiment</span>
              <span>Observed market-cap coverage: {fmtPct(observedWeight, 1)}</span>
            </div>
          </div>

          <aside className="space-y-3 border-t border-white/10 p-3 xl:border-l xl:border-t-0 xl:p-4">
            <RankList title="Top positive contributors" rows={positive} positive />
            <RankList title="Top negative contributors" rows={negative} positive={false} />
            <div className="rounded-2xl border border-white/10 bg-black/20 p-4 text-[11px] leading-5 text-neutral-600">
              <span className="text-neutral-400">Contribution</span> = constituent weight × observed sentiment. Missing sentiment is not treated as zero.
            </div>
          </aside>
        </div>
      </div>
    </div>
  );
}
