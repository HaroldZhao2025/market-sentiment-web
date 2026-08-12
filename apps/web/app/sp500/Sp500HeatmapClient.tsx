"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import { useRouter } from "next/navigation";

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
type Bubble = HeatmapTile & { x: number; y: number; r: number; cluster: string };

const W = 1200;
const H = 680;

function clamp(x: number, a: number, b: number) {
  return Math.max(a, Math.min(b, x));
}

function finite(x: unknown): number | null {
  if (x === null || x === undefined || x === "") return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function contribution(tile: HeatmapTile): number | null {
  const w = finite(tile.weight);
  const s = finite(tile.sentiment);
  return w == null || s == null ? null : w * s;
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

function fillFor(v: number | null, mode: Mode): string {
  if (v == null) return "rgba(82,82,91,0.42)";
  const t = clamp(Math.abs(v) / scaleFor(mode), 0, 1);
  if (v > 0) return `rgba(16,185,129,${0.24 + t * 0.68})`;
  if (v < 0) return `rgba(244,63,94,${0.24 + t * 0.68})`;
  return "rgba(113,113,122,0.5)";
}

function strokeFor(v: number | null) {
  if (v == null) return "rgba(255,255,255,0.08)";
  if (v > 0) return "rgba(110,231,183,0.42)";
  if (v < 0) return "rgba(253,164,175,0.42)";
  return "rgba(255,255,255,0.12)";
}

function fmtMoney(x: number | null | undefined) {
  return x == null || !Number.isFinite(x) ? "—" : x.toFixed(2);
}
function fmtNum(x: number | null | undefined, d = 3) {
  return x == null || !Number.isFinite(x) ? "—" : x.toFixed(d);
}
function fmtPct(x: number | null | undefined, d = 2) {
  return x == null || !Number.isFinite(x) ? "—" : `${(x * 100).toFixed(d)}%`;
}
function metricDisplay(tile: HeatmapTile, mode: Mode) {
  const v = metricValue(tile, mode);
  if (v == null) return "—";
  if (mode === "return") return `${v > 0 ? "+" : ""}${fmtPct(v, 2)}`;
  if (mode === "contribution") return `${v >= 0 ? "+" : ""}${(v * 10000).toFixed(2)} bps`;
  return `${v >= 0 ? "+" : ""}${v.toFixed(3)}`;
}

function hash(s: string) {
  let h = 2166136261;
  for (let i = 0; i < s.length; i += 1) h = Math.imul(h ^ s.charCodeAt(i), 16777619);
  return (h >>> 0) / 4294967295;
}

function clusterCenters(sectors: string[]) {
  const cols = sectors.length <= 4 ? 2 : sectors.length <= 9 ? 3 : 4;
  const rows = Math.ceil(sectors.length / cols);
  const centers = new Map<string, { x: number; y: number }>();
  sectors.forEach((s, i) => {
    const col = i % cols;
    const row = Math.floor(i / cols);
    centers.set(s, {
      x: ((col + 0.5) / cols) * W,
      y: ((row + 0.5) / rows) * H,
    });
  });
  return centers;
}

// Dependency-light deterministic circle packing. Area is proportional to constituent weight,
// while a mild sector attraction creates organic clusters instead of a rectangular treemap.
function pack(items: HeatmapTile[]): { bubbles: Bubble[]; centers: Map<string, { x: number; y: number }> } {
  const valid = items.filter((t) => (finite(t.weight) ?? finite(t.market_cap) ?? 0) > 0);
  if (!valid.length) return { bubbles: [], centers: new Map() };
  const sectors = Array.from(new Set(valid.map((t) => t.sector || "Unknown"))).sort();
  const centers = clusterCenters(sectors);
  const totalWeight = valid.reduce((s, t) => s + Math.max(0, finite(t.weight) ?? 0), 0);
  const fallbackTotal = valid.reduce((s, t) => s + Math.max(0, finite(t.market_cap) ?? 0), 0);

  const bubbles: Bubble[] = valid.map((t) => {
    const cluster = t.sector || "Unknown";
    const c = centers.get(cluster) ?? { x: W / 2, y: H / 2 };
    const share = totalWeight > 0
      ? Math.max(0, finite(t.weight) ?? 0) / totalWeight
      : Math.max(0, finite(t.market_cap) ?? 0) / Math.max(1, fallbackTotal);
    const r = clamp(Math.sqrt(share) * 330, 5, 62);
    return {
      ...t,
      cluster,
      r,
      x: c.x + (hash(`${t.symbol}-x`) - 0.5) * 110,
      y: c.y + (hash(`${t.symbol}-y`) - 0.5) * 110,
    };
  });

  for (let iter = 0; iter < 70; iter += 1) {
    for (const b of bubbles) {
      const c = centers.get(b.cluster) ?? { x: W / 2, y: H / 2 };
      b.x += (c.x - b.x) * 0.025;
      b.y += (c.y - b.y) * 0.025;
    }
    for (let i = 0; i < bubbles.length; i += 1) {
      for (let j = i + 1; j < bubbles.length; j += 1) {
        const a = bubbles[i];
        const b = bubbles[j];
        let dx = b.x - a.x;
        let dy = b.y - a.y;
        let d = Math.sqrt(dx * dx + dy * dy);
        const minD = a.r + b.r + 2.2;
        if (d >= minD) continue;
        if (d < 0.001) {
          dx = hash(`${a.symbol}-${b.symbol}`) - 0.5;
          dy = hash(`${b.symbol}-${a.symbol}`) - 0.5;
          d = Math.sqrt(dx * dx + dy * dy) || 1;
        }
        const push = (minD - d) * 0.5;
        const ux = dx / d;
        const uy = dy / d;
        a.x -= ux * push;
        a.y -= uy * push;
        b.x += ux * push;
        b.y += uy * push;
      }
    }
    for (const b of bubbles) {
      b.x = clamp(b.x, b.r + 5, W - b.r - 5);
      b.y = clamp(b.y, b.r + 5, H - b.r - 5);
    }
  }
  return { bubbles, centers };
}

function RankList({ title, rows, positive }: { title: string; rows: HeatmapTile[]; positive: boolean }) {
  return (
    <div className="rounded-2xl border border-white/10 bg-white/[0.025] p-4">
      <div className="mb-3 text-xs font-semibold uppercase tracking-[0.14em] text-neutral-500">{title}</div>
      <div className="space-y-2">
        {rows.length ? rows.map((tile) => {
          const c = contribution(tile) ?? 0;
          return (
            <Link key={tile.symbol} href={`/ticker/${tile.symbol}`} className="flex items-center justify-between gap-3 rounded-xl px-2 py-2 transition hover:bg-white/[0.05]">
              <div className="min-w-0"><div className="font-semibold text-white">{tile.symbol}</div><div className="truncate text-[11px] text-neutral-600">{tile.sector || "Unknown sector"}</div></div>
              <div className={`text-right font-mono text-xs ${positive ? "text-emerald-300" : "text-rose-300"}`}>{c >= 0 ? "+" : ""}{(c * 10000).toFixed(2)} bps</div>
            </Link>
          );
        }) : <div className="text-xs text-neutral-600">No observed contributors.</div>}
      </div>
    </div>
  );
}

export default function Sp500HeatmapClient({ data }: Props) {
  const router = useRouter();
  const [sector, setSector] = useState("All sectors");
  const [mode, setMode] = useState<Mode>("contribution");
  const [hovered, setHovered] = useState<Bubble | null>(null);
  const tiles = data.tiles || [];

  const sectors = useMemo(() => ["All sectors", ...Array.from(new Set(tiles.map((t) => t.sector || "Unknown"))).sort()], [tiles]);
  const filtered = useMemo(() => tiles.filter((t) => sector === "All sectors" || (t.sector || "Unknown") === sector), [tiles, sector]);
  const layout = useMemo(() => pack(filtered), [filtered]);
  const observed = useMemo(() => filtered.filter((t) => finite(t.sentiment) != null), [filtered]);
  const positive = useMemo(() => observed.slice().sort((a, b) => (contribution(b) ?? -Infinity) - (contribution(a) ?? -Infinity)).filter((t) => (contribution(t) ?? 0) > 0).slice(0, 6), [observed]);
  const negative = useMemo(() => observed.slice().sort((a, b) => (contribution(a) ?? Infinity) - (contribution(b) ?? Infinity)).filter((t) => (contribution(t) ?? 0) < 0).slice(0, 6), [observed]);
  const observedWeight = observed.reduce((s, t) => s + Math.max(0, finite(t.weight) ?? 0), 0);

  return (
    <div className="space-y-4">
      <div className="ambient-panel overflow-hidden">
        <div className="flex flex-col gap-4 border-b border-white/10 px-5 py-4 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <div className="eyebrow">Constituent intelligence</div>
            <div className="mt-1 flex flex-wrap items-baseline gap-3"><h3 className="text-lg font-semibold text-white">S&amp;P 500 clustered bubble map</h3><span className="text-xs text-neutral-600">As of {data.asof}</span></div>
            <p className="mt-1 text-xs text-neutral-500">Bubble area reflects constituent weight. Sector attraction forms organic clusters. Color reflects {metricLabel(mode).toLowerCase()}.</p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <select className="rounded-xl border border-white/10 bg-neutral-900 px-3 py-2 text-xs text-neutral-300 outline-none" value={sector} onChange={(e) => setSector(e.target.value)}>
              {sectors.map((s) => <option key={s} value={s}>{s}</option>)}
            </select>
            <div className="flex rounded-xl border border-white/10 bg-black/30 p-1">
              {(["contribution", "sentiment", "return"] as Mode[]).map((m) => <button key={m} type="button" onClick={() => setMode(m)} className={`rounded-lg px-3 py-1.5 text-xs font-medium transition ${mode === m ? "bg-white/10 text-white" : "text-neutral-500 hover:text-neutral-300"}`}>{m === "contribution" ? "Contribution" : m === "sentiment" ? "Sentiment" : "1D Return"}</button>)}
            </div>
          </div>
        </div>

        <div className="grid gap-0 xl:grid-cols-[minmax(0,1fr)_260px]">
          <div className="p-3 md:p-4">
            <div className="relative min-h-[500px] overflow-hidden rounded-2xl border border-white/10 bg-[radial-gradient(circle_at_50%_45%,rgba(255,255,255,0.035),transparent_55%)]">
              <svg viewBox={`0 0 ${W} ${H}`} className="h-full min-h-[500px] w-full" role="img" aria-label="S&P 500 clustered bubble map">
                {sector === "All sectors" ? Array.from(layout.centers.entries()).map(([name, c]) => (
                  <text key={name} x={c.x} y={Math.max(18, c.y - 88)} textAnchor="middle" fill="rgba(161,161,170,0.35)" fontSize="13" fontWeight="600">{name}</text>
                )) : null}
                {layout.bubbles.slice().sort((a, b) => b.r - a.r).map((b) => {
                  const value = metricValue(b, mode);
                  const major = b.r >= 19;
                  const showMetric = b.r >= 28;
                  return (
                    <g key={b.symbol} transform={`translate(${b.x} ${b.y})`} className="cursor-pointer" onMouseEnter={() => setHovered(b)} onMouseLeave={() => setHovered(null)} onClick={() => router.push(`/ticker/${b.symbol}`)}>
                      <circle r={b.r} fill={fillFor(value, mode)} stroke={strokeFor(value)} strokeWidth={hovered?.symbol === b.symbol ? 2.5 : 1.2} className="transition-all duration-200 hover:brightness-125" />
                      {major ? <text y={showMetric ? -2 : 4} textAnchor="middle" fill="white" fontSize={Math.min(14, Math.max(9, b.r / 2.7))} fontWeight="700" pointerEvents="none">{b.symbol}</text> : null}
                      {showMetric ? <text y={14} textAnchor="middle" fill="rgba(255,255,255,0.72)" fontSize="8.5" fontFamily="ui-monospace, SFMono-Regular, Menlo, monospace" pointerEvents="none">{metricDisplay(b, mode)}</text> : null}
                    </g>
                  );
                })}
              </svg>

              {hovered ? (
                <div className="pointer-events-none absolute z-30 w-64 rounded-xl border border-white/15 bg-neutral-950/95 p-3 shadow-2xl backdrop-blur" style={{ left: `${clamp((hovered.x / W) * 100, 4, 72)}%`, top: `${clamp((hovered.y / H) * 100, 5, 62)}%` }}>
                  <div className="flex items-start justify-between gap-3"><div><div className="font-semibold text-white">{hovered.symbol}</div><div className="mt-0.5 text-[11px] text-neutral-500">{hovered.name || "—"}</div></div><div className="font-mono text-xs text-neutral-300">{metricDisplay(hovered, mode)}</div></div>
                  <div className="mt-3 grid grid-cols-2 gap-x-3 gap-y-2 text-[11px]">
                    <span className="text-neutral-600">Sector</span><span className="text-right text-neutral-300">{hovered.sector || "—"}</span>
                    <span className="text-neutral-600">Industry</span><span className="truncate text-right text-neutral-300">{hovered.industry || "—"}</span>
                    <span className="text-neutral-600">Weight</span><span className="text-right font-mono text-neutral-300">{fmtPct(hovered.weight, 2)}</span>
                    <span className="text-neutral-600">Price</span><span className="text-right font-mono text-neutral-300">{fmtMoney(hovered.price)}</span>
                    <span className="text-neutral-600">1D return</span><span className="text-right font-mono text-neutral-300">{fmtPct(hovered.return_1d)}</span>
                    <span className="text-neutral-600">Sentiment</span><span className="text-right font-mono text-neutral-300">{fmtNum(hovered.sentiment, 4)}</span>
                    <span className="text-neutral-600">Contribution</span><span className="text-right font-mono text-neutral-300">{contribution(hovered) == null ? "—" : `${((contribution(hovered) || 0) * 10000).toFixed(2)} bps`}</span>
                    <span className="text-neutral-600">Unique news</span><span className="text-right font-mono text-neutral-300">{hovered.n_total ?? "—"}</span>
                  </div>
                  <div className="mt-3 text-[10px] text-neutral-600">Click bubble to open ticker intelligence.</div>
                </div>
              ) : null}
            </div>

            <div className="mt-3 flex flex-wrap items-center justify-between gap-3 text-[11px] text-neutral-600"><span>{filtered.length} constituents · {observed.length} with observed sentiment</span><span>Observed market-cap coverage: {fmtPct(observedWeight, 1)}</span></div>
          </div>

          <aside className="space-y-3 border-t border-white/10 p-3 xl:border-l xl:border-t-0 xl:p-4">
            <RankList title="Top positive contributors" rows={positive} positive />
            <RankList title="Top negative contributors" rows={negative} positive={false} />
            <div className="rounded-2xl border border-white/10 bg-black/20 p-4 text-[11px] leading-5 text-neutral-600"><span className="text-neutral-400">Contribution</span> = constituent weight × observed sentiment. Missing sentiment is not treated as zero.</div>
          </aside>
        </div>
      </div>
    </div>
  );
}
