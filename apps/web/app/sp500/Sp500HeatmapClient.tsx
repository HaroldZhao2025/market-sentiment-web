"use client";

import Link from "next/link";
import { useMemo, useRef, useState } from "react";

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

type Rect = {
  x: number;
  y: number;
  w: number;
  h: number;
};

type TileRect = HeatmapTile & Rect & { key: string };

type GroupRect = {
  key: string;
  label: string;
  value: number;
  rect: Rect;
  tiles: HeatmapTile[];
};

const CANVAS_W = 1200; // internal coordinate system
const CANVAS_H = 680;

function clamp(x: number, a: number, b: number) {
  return Math.max(a, Math.min(b, x));
}

function isFiniteNum(x: unknown): x is number {
  return typeof x === "number" && Number.isFinite(x);
}

function fmtMoney(x: number | null | undefined, digits = 1) {
  if (!isFiniteNum(x)) return "—";
  return x.toFixed(digits);
}

function fmtNum(x: number | null | undefined, digits = 2) {
  if (!isFiniteNum(x)) return "—";
  return x.toFixed(digits);
}

function fmtPct(x: number | null | undefined, digits = 2) {
  if (!isFiniteNum(x)) return "—";
  return `${(x * 100).toFixed(digits)}%`;
}

function fmtMcap(x: number | null | undefined) {
  if (!isFiniteNum(x)) return "—";
  const abs = Math.abs(x);
  if (abs >= 1e12) return `${(x / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `${(x / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${(x / 1e6).toFixed(2)}M`;
  return x.toFixed(0);
}

/**
 * Finviz-like heat colors using HSL:
 * - Positive: green hue (120)
 * - Negative: red hue (0)
 * - Lightness decreases with magnitude
 */
function colorForMetric(
  v: number | null | undefined,
  mode: "sentiment" | "return"
): { bg: string; fg: "text-black" | "text-white" } {
  if (!isFiniteNum(v)) return { bg: "rgb(235,235,235)", fg: "text-black" };

  const cap = mode === "sentiment" ? 0.6 : 0.05;
  const s = clamp(v, -cap, cap) / cap; // [-1, 1]
  const mag = Math.abs(s);

  const hue = s >= 0 ? 120 : 0;
  const sat = 70;
  const light = 92 - mag * 55; // 92 -> 37

  const bg = `hsl(${hue} ${sat}% ${light}%)`;
  const fg = light < 55 ? "text-white" : "text-black";
  return { bg, fg };
}

/**
 * Squarify treemap algorithm (no deps).
 * Returns rectangles in the given container.
 */
function squarify<T>(
  items: T[],
  valueFn: (t: T) => number,
  rect: Rect
): Array<{ item: T; rect: Rect }> {
  const { x, y, w, h } = rect;
  const list = items
    .map((it) => ({ it, v: valueFn(it) }))
    .filter((d) => Number.isFinite(d.v) && d.v > 0)
    .sort((a, b) => b.v - a.v);

  const total = list.reduce((s, d) => s + d.v, 0);
  if (total <= 0 || w <= 0 || h <= 0) return [];

  // Normalize to areas
  const area = w * h;
  const normalized = list.map((d) => ({ it: d.it, a: (d.v / total) * area }));

  const out: Array<{ item: T; rect: Rect }> = [];

  function worst(row: number[], side: number) {
    if (!row.length) return Infinity;
    const sum = row.reduce((s, a) => s + a, 0);
    const maxA = Math.max(...row);
    const minA = Math.min(...row);
    const s2 = side * side;
    return Math.max((s2 * maxA) / (sum * sum), (sum * sum) / (s2 * minA));
  }

  function layoutRow(row: Array<{ it: T; a: number }>, r: Rect, horizontal: boolean) {
    const rowArea = row.reduce((s, d) => s + d.a, 0);
    if (rowArea <= 0) return { remaining: r };

    if (horizontal) {
      const rowH = rowArea / r.w;
      let cx = r.x;
      for (const d of row) {
        const cw = d.a / rowH;
        out.push({ item: d.it, rect: { x: cx, y: r.y, w: cw, h: rowH } });
        cx += cw;
      }
      return { remaining: { x: r.x, y: r.y + rowH, w: r.w, h: r.h - rowH } };
    } else {
      const rowW = rowArea / r.h;
      let cy = r.y;
      for (const d of row) {
        const ch = d.a / rowW;
        out.push({ item: d.it, rect: { x: r.x, y: cy, w: rowW, h: ch } });
        cy += ch;
      }
      return { remaining: { x: r.x + rowW, y: r.y, w: r.w - rowW, h: r.h } };
    }
  }

  let r: Rect = { x, y, w, h };
  let row: Array<{ it: T; a: number }> = [];
  let rowAreas: number[] = [];

  while (normalized.length) {
    const d = normalized[0];
    const nextRow = [...row, d];
    const side = Math.min(r.w, r.h);

    const currWorst = worst(rowAreas, side);
    const nextWorst = worst([...rowAreas, d.a], side);

    if (row.length === 0 || nextWorst <= currWorst) {
      row.push(d);
      rowAreas.push(d.a);
      normalized.shift();
    } else {
      const horizontal = r.w >= r.h;
      const laid = layoutRow(row, r, horizontal);
      r = laid.remaining;
      row = [];
      rowAreas = [];
    }
  }

  if (row.length) {
    const horizontal = r.w >= r.h;
    const laid = layoutRow(row, r, horizontal);
    r = laid.remaining;
  }

  return out;
}

function uniqSorted(arr: string[]) {
  return Array.from(new Set(arr)).sort((a, b) => a.localeCompare(b));
}

function safeStr(x: unknown, fallback: string) {
  const s = typeof x === "string" ? x.trim() : "";
  return s ? s : fallback;
}

export default function Sp500HeatmapClient({ data }: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);

  const [sector, setSector] = useState("All sectors");
  const [industry, setIndustry] = useState("All industries");
  const [mode, setMode] = useState<"sentiment" | "return">("sentiment");
  const [query, setQuery] = useState("");
  const [labelMode, setLabelMode] = useState<"auto" | "ticker" | "none">("auto");

  const [hover, setHover] = useState<{
    tile: HeatmapTile;
    px: number;
    py: number;
  } | null>(null);

  const tiles = data.tiles ?? [];

  const sectors = useMemo(() => {
    const s = tiles.map((t) => safeStr(t.sector, "Unknown"));
    return ["All sectors", ...uniqSorted(s)];
  }, [tiles]);

  const industries = useMemo(() => {
    const inds: string[] = [];
    for (const t of tiles) {
      const sec = safeStr(t.sector, "Unknown");
      if (sector !== "All sectors" && sec !== sector) continue;
      inds.push(safeStr(t.industry, "Unknown"));
    }
    return ["All industries", ...uniqSorted(inds)];
  }, [tiles, sector]);

  const filtered = useMemo(() => {
    const q = query.trim().toUpperCase();
    return tiles.filter((t) => {
      const sec = safeStr(t.sector, "Unknown");
      const ind = safeStr(t.industry, "Unknown");
      if (sector !== "All sectors" && sec !== sector) return false;
      if (industry !== "All industries" && ind !== industry) return false;
      if (q) {
        const sym = safeStr(t.symbol, "").toUpperCase();
        const nm = safeStr(t.name, "").toUpperCase();
        if (!sym.includes(q) && !nm.includes(q)) return false;
      }
      return true;
    });
  }, [tiles, sector, industry, query]);

  // Sector-grouped layout by default (finviz-like)
  const groupedLayout = useMemo(() => {
    // If user chooses a specific sector, we can skip grouping and just render that sector as a single group.
    const bySector = new Map<string, HeatmapTile[]>();
    for (const t of filtered) {
      const sec = safeStr(t.sector, "Unknown");
      if (!bySector.has(sec)) bySector.set(sec, []);
      bySector.get(sec)!.push(t);
    }

    const groups: Array<{ key: string; label: string; value: number; tiles: HeatmapTile[] }> = [];
    for (const [sec, arr] of bySector.entries()) {
      const v = arr.reduce((s, t) => s + (isFiniteNum(t.market_cap) ? t.market_cap : 0), 0);
      if (v > 0) groups.push({ key: sec, label: sec, value: v, tiles: arr });
    }

    // Sort sectors by size
    groups.sort((a, b) => b.value - a.value);

    const groupRectsRaw = squarify(groups, (g) => g.value, {
      x: 0,
      y: 0,
      w: CANVAS_W,
      h: CANVAS_H,
    });

    const headerH = 22; // sector header band inside each sector box
    const pad = 6;

    const groupRects: GroupRect[] = groupRectsRaw.map(({ item, rect }) => {
      // inner rect for tiles
      const inner: Rect = {
        x: rect.x + pad,
        y: rect.y + pad + headerH,
        w: Math.max(0, rect.w - pad * 2),
        h: Math.max(0, rect.h - pad * 2 - headerH),
      };
      return { key: item.key, label: item.label, value: item.value, rect, tiles: item.tiles };
    });

    // Tile rects (nested squarify within each sector rect)
    const tileRects: TileRect[] = [];
    for (const g of groupRects) {
      const inner: Rect = {
        x: g.rect.x + pad,
        y: g.rect.y + pad + headerH,
        w: Math.max(0, g.rect.w - pad * 2),
        h: Math.max(0, g.rect.h - pad * 2 - headerH),
      };

      const mapped = squarify(g.tiles, (t) => (isFiniteNum(t.market_cap) ? t.market_cap : 0), inner);
      for (const m of mapped) {
        tileRects.push({
          ...m.item,
          x: m.rect.x,
          y: m.rect.y,
          w: m.rect.w,
          h: m.rect.h,
          key: `${g.key}:${m.item.symbol}`,
        });
      }
    }

    return { groups: groupRects, tiles: tileRects };
  }, [filtered]);

  function onTileHover(e: React.MouseEvent, t: HeatmapTile) {
    const el = containerRef.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    const px = e.clientX - r.left;
    const py = e.clientY - r.top;
    setHover({ tile: t, px, py });
  }

  function onLeave() {
    setHover(null);
  }

  // Helpers for rendering sizes
  function pctX(x: number) {
    return (x / CANVAS_W) * 100;
  }
  function pctY(y: number) {
    return (y / CANVAS_H) * 100;
  }

  return (
    <div className="rounded-2xl border bg-white overflow-hidden shadow-sm">
      {/* Controls header */}
      <div className="px-4 py-3 bg-gray-50 border-b">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex flex-wrap items-center gap-2 text-sm">
            <div className="font-semibold">All sectors</div>
            <div className="text-gray-400">•</div>
            <div className="text-gray-600">
              Latest trading day: <span className="font-medium">{data.asof}</span>
            </div>
            {data.updated_at_utc ? (
              <>
                <div className="text-gray-400">•</div>
                <div className="text-gray-500">Updated: {data.updated_at_utc.replace("T", " ").replace("Z", " UTC")}</div>
              </>
            ) : null}
          </div>

          <div className="flex flex-wrap items-center gap-2 text-sm">
            <select
              className="border rounded-md px-2 py-1 bg-white min-w-[180px]"
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
              className="border rounded-md px-2 py-1 bg-white min-w-[220px]"
              value={industry}
              onChange={(e) => setIndustry(e.target.value)}
            >
              {industries.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>

            <input
              className="border rounded-md px-2 py-1 bg-white w-[220px]"
              placeholder="Search ticker or name…"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
            />

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

            <select
              className="border rounded-md px-2 py-1 bg-white"
              value={labelMode}
              onChange={(e) => setLabelMode(e.target.value as any)}
              title="Label density"
            >
              <option value="auto">Labels: auto</option>
              <option value="ticker">Labels: ticker</option>
              <option value="none">Labels: none</option>
            </select>
          </div>
        </div>

        {/* Legend */}
        <div className="mt-3 flex items-center justify-between gap-3 flex-wrap">
          <div className="text-xs text-gray-500">
            Tip: hover for details, click a tile to open its ticker page.
          </div>

          <div className="flex items-center gap-2 text-xs text-gray-600">
            <span>{mode === "sentiment" ? "-0.6" : "-5%"}</span>
            <div
              className="h-2 w-56 rounded-full border"
              style={{
                background:
                  mode === "sentiment"
                    ? "linear-gradient(90deg, hsl(0 70% 45%), hsl(0 70% 92%), hsl(120 70% 92%), hsl(120 70% 45%))"
                    : "linear-gradient(90deg, hsl(0 70% 45%), hsl(0 70% 92%), hsl(120 70% 92%), hsl(120 70% 45%))",
              }}
            />
            <span>{mode === "sentiment" ? "+0.6" : "+5%"}</span>
          </div>
        </div>
      </div>

      {/* Map container */}
      <div className="p-3 bg-white">
        <div
          ref={containerRef}
          className="relative w-full rounded-xl border bg-white overflow-hidden"
          style={{ height: "72vh", minHeight: 440 }}
          onMouseLeave={onLeave}
        >
          {/* Sector frames + headers */}
          {groupedLayout.groups.map((g) => {
            const left = pctX(g.rect.x);
            const top = pctY(g.rect.y);
            const width = pctX(g.rect.w);
            const height = pctY(g.rect.h);

            return (
              <div
                key={`sector:${g.key}`}
                className="absolute rounded-lg border bg-gray-50/40"
                style={{
                  left: `${left}%`,
                  top: `${top}%`,
                  width: `${width}%`,
                  height: `${height}%`,
                }}
              >
                <div className="px-2 py-1 text-xs font-semibold text-gray-700">
                  {g.label}
                </div>
              </div>
            );
          })}

          {/* Tiles */}
          {groupedLayout.tiles.map((t) => {
            const left = pctX(t.x);
            const top = pctY(t.y);
            const width = pctX(t.w);
            const height = pctY(t.h);

            const metricValue = mode === "sentiment" ? (t.sentiment ?? null) : (t.return_1d ?? null);
            const { bg, fg } = colorForMetric(metricValue as any, mode);

            // label rules
            const area = t.w * t.h;
            const showAny =
              labelMode !== "none" && area > 900; // tiny tiles: no text
            const showDetail =
              labelMode === "auto" ? area > 12000 : labelMode === "ticker" ? false : false;

            const fontTicker = clamp(Math.sqrt(area) / 10, 10, 22);
            const fontDetail = clamp(fontTicker - 6, 10, 14);

            return (
              <Link
                key={t.key}
                href={`/ticker/${t.symbol}`}
                className={`absolute rounded-md border border-white/80 hover:brightness-95 ${fg}`}
                style={{
                  left: `calc(${left}% + 1px)`,
                  top: `calc(${top}% + 1px)`,
                  width: `calc(${width}% - 2px)`,
                  height: `calc(${height}% - 2px)`,
                  background: bg,
                  textDecoration: "none",
                }}
                onMouseMove={(e) => onTileHover(e, t)}
              >
                {showAny ? (
                  <div className="w-full h-full flex flex-col items-center justify-center text-center px-1 select-none">
                    <div className="font-semibold leading-none" style={{ fontSize: fontTicker }}>
                      {t.symbol}
                    </div>
                    {showDetail ? (
                      <div className="mt-1 opacity-90 leading-none" style={{ fontSize: fontDetail }}>
                        ({fmtMoney(t.price ?? null, 1)}, {fmtNum(t.sentiment ?? null, 2)})
                      </div>
                    ) : null}
                  </div>
                ) : null}
              </Link>
            );
          })}

          {/* Tooltip card */}
          {hover && (
            <div
              className="absolute z-50 pointer-events-none"
              style={{
                left: clamp(hover.px + 14, 8, (containerRef.current?.clientWidth ?? 1000) - 320),
                top: clamp(hover.py + 14, 8, (containerRef.current?.clientHeight ?? 700) - 170),
              }}
            >
              <div className="w-[300px] rounded-xl border bg-white shadow-lg p-3">
                <div className="flex items-start justify-between gap-2">
                  <div>
                    <div className="text-sm font-semibold">{hover.tile.symbol}</div>
                    <div className="text-xs text-gray-600 line-clamp-2">{hover.tile.name || "—"}</div>
                  </div>
                  <div className="text-xs text-gray-500 text-right">
                    <div>{hover.tile.date || data.asof}</div>
                    <div className="mt-1">mcap: {fmtMcap(hover.tile.market_cap ?? null)}</div>
                  </div>
                </div>

                <div className="mt-2 grid grid-cols-2 gap-x-3 gap-y-1 text-xs">
                  <div className="text-gray-500">Sector</div>
                  <div className="text-gray-800">{safeStr(hover.tile.sector, "Unknown")}</div>

                  <div className="text-gray-500">Industry</div>
                  <div className="text-gray-800">{safeStr(hover.tile.industry, "Unknown")}</div>

                  <div className="text-gray-500">Price</div>
                  <div className="text-gray-800">{fmtMoney(hover.tile.price ?? null, 2)}</div>

                  <div className="text-gray-500">Return (1D)</div>
                  <div className="text-gray-800">{fmtPct(hover.tile.return_1d ?? null, 2)}</div>

                  <div className="text-gray-500">Sentiment</div>
                  <div className="text-gray-800">{fmtNum(hover.tile.sentiment ?? null, 4)}</div>

                  <div className="text-gray-500">Articles</div>
                  <div className="text-gray-800">{hover.tile.n_total ?? "—"}</div>
                </div>

                <div className="mt-2 text-[11px] text-gray-500">
                  Click tile to open ticker page
                </div>
              </div>
            </div>
          )}
        </div>

        <div className="mt-2 text-xs text-gray-500">
          Source: Wikipedia (GICS sector/industry) + yfinance (market cap)
        </div>
      </div>
    </div>
  );
}
