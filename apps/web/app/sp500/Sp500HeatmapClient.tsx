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
type Sp500HeatmapFile = { symbol: string; name: string; asof: string; updated_at_utc?: string; stats?: Record<string, unknown>; tiles: HeatmapTile[] };
type Props = { data: Sp500HeatmapFile };
type Mode = "contribution" | "sentiment" | "return";
type ViewMode = "treemap" | "bubble";
type Bubble = HeatmapTile & { x: number; y: number; r: number; cluster: string };
type RectTile = HeatmapTile & { x: number; y: number; w: number; h: number };

const W = 1200;
const H = 680;
const clamp = (x: number, a: number, b: number) => Math.max(a, Math.min(b, x));
const finite = (x: unknown): number | null => {
  if (x === null || x === undefined || x === "") return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
};
const contribution = (tile: HeatmapTile) => {
  const w = finite(tile.weight), s = finite(tile.sentiment);
  return w == null || s == null ? null : w * s;
};
function metricValue(tile: HeatmapTile, mode: Mode) {
  if (mode === "sentiment") return finite(tile.sentiment);
  if (mode === "return") return finite(tile.return_1d);
  return contribution(tile);
}
function metricLabel(mode: Mode) {
  return mode === "sentiment" ? "Sentiment" : mode === "return" ? "1D return" : "Index contribution";
}
function scaleFor(mode: Mode) {
  return mode === "sentiment" ? 0.45 : mode === "return" ? 0.04 : 0.008;
}
function fillFor(v: number | null, mode: Mode) {
  if (v == null) return "rgba(82,82,91,0.46)";
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
const fmtMoney = (x: number | null | undefined) => x == null || !Number.isFinite(x) ? "—" : x.toFixed(2);
const fmtNum = (x: number | null | undefined, d = 3) => x == null || !Number.isFinite(x) ? "—" : x.toFixed(d);
const fmtPct = (x: number | null | undefined, d = 2) => x == null || !Number.isFinite(x) ? "—" : `${(x * 100).toFixed(d)}%`;
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
function sizeValue(tile: HeatmapTile) {
  return Math.max(0, finite(tile.weight) ?? finite(tile.market_cap) ?? 0);
}

function clusterCenters(sectors: string[]) {
  const cols = sectors.length <= 4 ? 2 : sectors.length <= 9 ? 3 : 4;
  const rows = Math.ceil(sectors.length / cols);
  const centers = new Map<string, { x: number; y: number }>();
  sectors.forEach((s, i) => centers.set(s, { x: (((i % cols) + 0.5) / cols) * W, y: ((Math.floor(i / cols) + 0.5) / rows) * H }));
  return centers;
}
function pack(items: HeatmapTile[]) {
  const valid = items.filter((t) => sizeValue(t) > 0);
  if (!valid.length) return { bubbles: [] as Bubble[], centers: new Map<string, { x: number; y: number }>() };
  const sectors = Array.from(new Set(valid.map((t) => t.sector || "Unknown"))).sort();
  const centers = clusterCenters(sectors);
  const total = valid.reduce((s, t) => s + sizeValue(t), 0);
  const bubbles: Bubble[] = valid.map((t) => {
    const cluster = t.sector || "Unknown";
    const c = centers.get(cluster) || { x: W / 2, y: H / 2 };
    const r = clamp(Math.sqrt(sizeValue(t) / Math.max(total, 1)) * 330, 5, 62);
    return { ...t, cluster, r, x: c.x + (hash(`${t.symbol}-x`) - 0.5) * 110, y: c.y + (hash(`${t.symbol}-y`) - 0.5) * 110 };
  });
  for (let iter = 0; iter < 70; iter += 1) {
    for (const b of bubbles) {
      const c = centers.get(b.cluster) || { x: W / 2, y: H / 2 };
      b.x += (c.x - b.x) * 0.025; b.y += (c.y - b.y) * 0.025;
    }
    for (let i = 0; i < bubbles.length; i += 1) for (let j = i + 1; j < bubbles.length; j += 1) {
      const a = bubbles[i], b = bubbles[j];
      let dx = b.x - a.x, dy = b.y - a.y, d = Math.sqrt(dx * dx + dy * dy);
      const minD = a.r + b.r + 2.2;
      if (d >= minD) continue;
      if (d < 0.001) { dx = hash(`${a.symbol}-${b.symbol}`) - 0.5; dy = hash(`${b.symbol}-${a.symbol}`) - 0.5; d = Math.sqrt(dx * dx + dy * dy) || 1; }
      const push = (minD - d) * 0.5, ux = dx / d, uy = dy / d;
      a.x -= ux * push; a.y -= uy * push; b.x += ux * push; b.y += uy * push;
    }
    for (const b of bubbles) { b.x = clamp(b.x, b.r + 5, W - b.r - 5); b.y = clamp(b.y, b.r + 5, H - b.r - 5); }
  }
  return { bubbles, centers };
}

function binaryTreemap(items: HeatmapTile[], x = 0, y = 0, w = W, h = H): RectTile[] {
  const rows = items.filter((t) => sizeValue(t) > 0).slice().sort((a, b) => sizeValue(b) - sizeValue(a));
  const out: RectTile[] = [];
  const layout = (arr: HeatmapTile[], rx: number, ry: number, rw: number, rh: number) => {
    if (!arr.length) return;
    if (arr.length === 1) { out.push({ ...arr[0], x: rx, y: ry, w: rw, h: rh }); return; }
    const total = arr.reduce((s, t) => s + sizeValue(t), 0);
    let acc = 0, split = 1;
    for (let i = 0; i < arr.length - 1; i += 1) { acc += sizeValue(arr[i]); split = i + 1; if (acc >= total / 2) break; }
    const a = arr.slice(0, split), b = arr.slice(split), aTotal = a.reduce((s, t) => s + sizeValue(t), 0), frac = total > 0 ? aTotal / total : 0.5;
    if (rw >= rh) { const aw = rw * frac; layout(a, rx, ry, aw, rh); layout(b, rx + aw, ry, rw - aw, rh); }
    else { const ah = rh * frac; layout(a, rx, ry, rw, ah); layout(b, rx, ry + ah, rw, rh - ah); }
  };
  layout(rows, x, y, w, h);
  return out;
}

function RankList({ title, rows, positive }: { title: string; rows: HeatmapTile[]; positive: boolean }) {
  return <div className="rounded-2xl border border-white/10 bg-white/[0.025] p-4"><div className="mb-3 text-xs font-semibold uppercase tracking-[0.14em] text-neutral-500">{title}</div><div className="space-y-2">
    {rows.length ? rows.map((tile) => <Link key={tile.symbol} href={`/ticker/${tile.symbol}`} className="flex items-center justify-between gap-3 rounded-xl px-2 py-2 transition hover:bg-white/[0.05]"><div className="min-w-0"><div className="font-semibold text-white">{tile.symbol}</div><div className="truncate text-[11px] text-neutral-600">{tile.name || tile.sector || "Unknown"}</div></div><div className={`text-right font-mono text-xs ${positive ? "text-emerald-300" : "text-rose-300"}`}>{((contribution(tile) || 0) * 10000).toFixed(2)} bps</div></Link>) : <div className="text-xs text-neutral-600">No observed contributors.</div>}
  </div></div>;
}

function Tooltip({ tile }: { tile: HeatmapTile }) {
  return <div className="pointer-events-none absolute right-3 top-3 z-30 w-72 rounded-xl border border-white/15 bg-neutral-950/95 p-4 shadow-2xl backdrop-blur">
    <div className="flex items-start justify-between gap-3"><div><div className="text-base font-bold text-white">{tile.symbol}</div><div className="mt-0.5 text-xs text-neutral-400">{tile.name || "Company name unavailable"}</div></div><div className="text-right text-xs text-neutral-500">{tile.date || ""}</div></div>
    <div className="mt-3 space-y-1.5 text-xs"><div className="text-neutral-400">{tile.sector || "Unknown sector"}</div><div className="text-neutral-600">{tile.industry || "Unknown industry"}</div>
      <div className="mt-3 grid grid-cols-2 gap-2 border-t border-white/10 pt-3"><span className="text-neutral-600">Weight</span><span className="text-right font-mono text-neutral-300">{fmtPct(finite(tile.weight), 2)}</span><span className="text-neutral-600">Price</span><span className="text-right font-mono text-neutral-300">{fmtMoney(finite(tile.price))}</span><span className="text-neutral-600">1D return</span><span className="text-right font-mono text-neutral-300">{fmtPct(finite(tile.return_1d), 2)}</span><span className="text-neutral-600">Sentiment</span><span className="text-right font-mono text-neutral-300">{fmtNum(finite(tile.sentiment), 3)}</span><span className="text-neutral-600">Contribution</span><span className="text-right font-mono text-neutral-300">{contribution(tile) == null ? "—" : `${(contribution(tile)! * 10000).toFixed(2)} bps`}</span><span className="text-neutral-600">Unique news</span><span className="text-right font-mono text-neutral-300">{finite(tile.n_total)?.toFixed(0) ?? "—"}</span></div>
    </div>
  </div>;
}

export default function Sp500HeatmapClient({ data }: Props) {
  const router = useRouter();
  const [sector, setSector] = useState("All sectors");
  const [mode, setMode] = useState<Mode>("contribution");
  const [view, setView] = useState<ViewMode>("treemap");
  const [hovered, setHovered] = useState<HeatmapTile | null>(null);
  const tiles = data.tiles || [];
  const sectors = useMemo(() => ["All sectors", ...Array.from(new Set(tiles.map((t) => t.sector || "Unknown"))).sort()], [tiles]);
  const filtered = useMemo(() => tiles.filter((t) => sector === "All sectors" || (t.sector || "Unknown") === sector), [tiles, sector]);
  const bubbleLayout = useMemo(() => pack(filtered), [filtered]);
  const rectLayout = useMemo(() => binaryTreemap(filtered, 0, 0, W, H), [filtered]);
  const observed = useMemo(() => filtered.filter((t) => finite(t.sentiment) != null), [filtered]);
  const positive = useMemo(() => observed.slice().sort((a, b) => (contribution(b) ?? -Infinity) - (contribution(a) ?? -Infinity)).filter((t) => (contribution(t) ?? 0) > 0).slice(0, 6), [observed]);
  const negative = useMemo(() => observed.slice().sort((a, b) => (contribution(a) ?? Infinity) - (contribution(b) ?? Infinity)).filter((t) => (contribution(t) ?? 0) < 0).slice(0, 6), [observed]);
  const observedWeight = observed.reduce((s, t) => s + Math.max(0, finite(t.weight) ?? 0), 0);

  return <div className="space-y-4">
    <div className="ambient-panel overflow-hidden">
      <div className="flex flex-col gap-4 border-b border-white/10 px-5 py-4 xl:flex-row xl:items-center xl:justify-between">
        <div><div className="eyebrow">Constituent intelligence</div><div className="mt-1 flex flex-wrap items-baseline gap-3"><h3 className="text-lg font-semibold text-white">S&amp;P 500 constituent map</h3><span className="text-xs text-neutral-600">As of {data.asof}</span></div><p className="mt-1 text-xs text-neutral-500">Choose classic treemap or clustered bubbles. Area reflects constituent weight; color reflects {metricLabel(mode).toLowerCase()}.</p></div>
        <div className="flex flex-wrap items-center gap-2">
          <select className="rounded-xl border border-white/10 bg-neutral-900 px-3 py-2 text-xs text-neutral-300 outline-none" value={sector} onChange={(e) => setSector(e.target.value)}>{sectors.map((s) => <option key={s}>{s}</option>)}</select>
          <div className="flex rounded-xl border border-white/10 bg-black/30 p-1">{(["treemap", "bubble"] as ViewMode[]).map((v) => <button key={v} type="button" onClick={() => setView(v)} className={`rounded-lg px-3 py-1.5 text-xs font-medium transition ${view === v ? "bg-white/10 text-white" : "text-neutral-500 hover:text-neutral-300"}`}>{v === "treemap" ? "Treemap" : "Bubbles"}</button>)}</div>
          <div className="flex rounded-xl border border-white/10 bg-black/30 p-1">{(["contribution", "sentiment", "return"] as Mode[]).map((m) => <button key={m} type="button" onClick={() => setMode(m)} className={`rounded-lg px-3 py-1.5 text-xs font-medium transition ${mode === m ? "bg-white/10 text-white" : "text-neutral-500 hover:text-neutral-300"}`}>{m === "contribution" ? "Contribution" : m === "sentiment" ? "Sentiment" : "1D Return"}</button>)}</div>
        </div>
      </div>
      <div className="grid xl:grid-cols-[minmax(0,1fr)_260px]">
        <div className="p-3 md:p-4"><div className="relative min-h-[500px] overflow-hidden rounded-2xl border border-white/10 bg-black/20">
          <svg viewBox={`0 0 ${W} ${H}`} className="h-full min-h-[500px] w-full" role="img" aria-label={`S&P 500 ${view}`}>
            {view === "bubble" ? <>
              {sector === "All sectors" ? Array.from(bubbleLayout.centers.entries()).map(([name, c]) => <text key={name} x={c.x} y={Math.max(18, c.y - 88)} textAnchor="middle" fill="rgba(161,161,170,0.35)" fontSize="13" fontWeight="600">{name}</text>) : null}
              {bubbleLayout.bubbles.slice().sort((a, b) => b.r - a.r).map((b) => { const value = metricValue(b, mode), major = b.r >= 19, showMetric = b.r >= 28; return <g key={b.symbol} transform={`translate(${b.x} ${b.y})`} className="cursor-pointer" onMouseEnter={() => setHovered(b)} onMouseLeave={() => setHovered(null)} onClick={() => router.push(`/ticker/${b.symbol}`)}><circle r={b.r} fill={fillFor(value, mode)} stroke={strokeFor(value)} strokeWidth={hovered?.symbol === b.symbol ? 2.5 : 1.2} />{major ? <text y={showMetric ? -2 : 4} textAnchor="middle" fill="white" fontSize={Math.min(14, Math.max(9, b.r / 2.7))} fontWeight="700" pointerEvents="none">{b.symbol}</text> : null}{showMetric ? <text y={14} textAnchor="middle" fill="rgba(255,255,255,0.72)" fontSize="8.5" pointerEvents="none">{metricDisplay(b, mode)}</text> : null}</g>; })}
            </> : rectLayout.map((r) => { const value = metricValue(r, mode), showName = r.w > 92 && r.h > 48, showMetric = r.w > 115 && r.h > 68; return <g key={r.symbol} className="cursor-pointer" onMouseEnter={() => setHovered(r)} onMouseLeave={() => setHovered(null)} onClick={() => router.push(`/ticker/${r.symbol}`)}><rect x={r.x + 1} y={r.y + 1} width={Math.max(0, r.w - 2)} height={Math.max(0, r.h - 2)} rx="4" fill={fillFor(value, mode)} stroke={hovered?.symbol === r.symbol ? "rgba(255,255,255,0.85)" : strokeFor(value)} strokeWidth={hovered?.symbol === r.symbol ? 2.2 : 1}/>{showName ? <text x={r.x + 8} y={r.y + 18} fill="white" fontSize={Math.min(14, Math.max(9, Math.min(r.w, r.h) / 5))} fontWeight="700" pointerEvents="none">{r.symbol}</text> : null}{showMetric ? <text x={r.x + 8} y={r.y + 34} fill="rgba(255,255,255,0.72)" fontSize="9" pointerEvents="none">{metricDisplay(r, mode)}</text> : null}</g>; })}
          </svg>
          {hovered ? <Tooltip tile={hovered} /> : null}
        </div></div>
        <aside className="space-y-3 border-t border-white/10 p-3 xl:border-l xl:border-t-0"><div className="rounded-2xl border border-white/10 bg-white/[0.025] p-4"><div className="text-[11px] uppercase tracking-[0.12em] text-neutral-600">Observed weight</div><div className="mt-1 text-2xl font-semibold text-white">{fmtPct(observedWeight, 1)}</div><div className="mt-1 text-xs text-neutral-600">{observed.length} / {filtered.length} constituents with observed sentiment</div></div><RankList title="Positive contribution" rows={positive} positive /><RankList title="Negative contribution" rows={negative} positive={false} /></aside>
      </div>
    </div>
  </div>;
}
