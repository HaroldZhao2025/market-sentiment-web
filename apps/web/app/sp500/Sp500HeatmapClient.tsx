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

type Rect = { x: number; y: number; w: number; h: number };

type SectorBox = { sector: string; value: number; rect: Rect };
type IndustryBox = { sector: string; industry: string; value: number; rect: Rect };
type TileBox = HeatmapTile & Rect & { key: string; sector: string; industry: string };

const CANVAS_W = 1200; // internal coordinate system
const CANVAS_H = 680;

const PAD_OUTER = 8; // padding inside the whole map
const PAD_SECTOR = 8; // padding inside each sector box
const PAD_INDUSTRY = 6; // padding inside each industry box

const SECTOR_HEADER_H = 24;
const IND_HEADER_H = 18;

function clamp(x: number, a: number, b: number) {
  return Math.max(a, Math.min(b, x));
}

function isFiniteNum(x: unknown): x is number {
  return typeof x === "number" && Number.isFinite(x);
}

function safeStr(x: unknown, fallback: string) {
  const s = typeof x === "string" ? x.trim() : "";
  return s ? s : fallback;
}

function fmtMoney(x: number | null | undefined, digits = 2) {
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

  // You can tune caps to your sentiment distribution
  const cap = mode === "sentiment" ? 0.6 : 0.05;
  const s = clamp(v, -cap, cap) / cap; // [-1,1]
  const mag = Math.abs(s);

  const hue = s >= 0 ? 120 : 0; // green/red
  const sat = 75;
  const light = 92 - mag * 55; // 92 -> 37
  const bg = `hsl(${hue} ${sat}% ${light}%)`;
  const fg = light < 55 ? "text-white" : "text-black";
  return { bg, fg };
}

/**
 * Squarify treemap (no deps).
 * Returns rectangles in container rect.
 */
function squarify<T>(
  items: T[],
  valueFn: (t: T) => number,
  rect: Rect
): Array<{ item: T; rect: Rect }> {
  const list = items
    .map((it) => ({ it, v: valueFn(it) }))
    .filter((d) => Number.isFinite(d.v) && d.v > 0)
    .sort((a, b) => b.v - a.v);

  const total = list.reduce((s, d) => s + d.v, 0);
  if (total <= 0 || rect.w <= 0 || rect.h <= 0) return [];

  const area = rect.w * rect.h;
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

  let r: Rect = { ...rect };
  let row: Array<{ it: T; a: number }> = [];
  let rowAreas: number[] = [];

  while (normalized.length) {
    const d = normalized[0];
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
    layoutRow(row, r, horizontal);
  }

  return out;
}

/**
 * Pack sectors into N columns (Finviz-like mosaic).
 * Greedy bin packing: put biggest sector into the currently smallest column.
 */
function layoutSectorsInColumns(
  sectors: Array<{ sector: string; value: number }>,
  nCols: number,
  rect: Rect
): SectorBox[] {
  const cols = Array.from({ length: nCols }, () => ({ sum: 0, items: [] as Array<{ sector: string; value: number }> }));

  const sorted = sectors.slice().sort((a, b) => b.value - a.value);
  for (const s of sorted) {
    cols.sort((a, b) => a.sum - b.sum);
    cols[0].items.push(s);
    cols[0].sum += s.value;
  }

  const colW = rect.w / nCols;
  const out: SectorBox[] = [];

  for (let ci = 0; ci < nCols; ci++) {
    const col = cols[ci];
    const x0 = rect.x + ci * colW;
    let yCursor = rect.y;
    const colSum = col.sum || 1;

    for (const s of col.items) {
      const h = (s.value / colSum) * rect.h;
      out.push({
        sector: s.sector,
        value: s.value,
        rect: { x: x0, y: yCursor, w: colW, h },
      });
      yCursor += h;
    }
  }

  return out;
}

export default function Sp500HeatmapClient({ data }: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);

  const [mode, setMode] = useState<"sentiment" | "return">("sentiment");
  const [sector, setSector] = useState<string>("All sectors");
  const [industry, setIndustry] = useState<string>("All industries");
  const [query, setQuery] = useState<string>("");

  const [hover, setHover] = useState<{
    tile: HeatmapTile;
    px: number;
    py: number;
  } | null>(null);

  const tiles = data.tiles ?? [];

  const sectors = useMemo(() => {
    const s = tiles.map((t) => safeStr(t.sector, "Unknown"));
    return ["All sectors", ...Array.from(new Set(s)).sort((a, b) => a.localeCompare(b))];
  }, [tiles]);

  const industries = useMemo(() => {
    const out: string[] = [];
    for (const t of tiles) {
      const sec = safeStr(t.sector, "Unknown");
      if (sector !== "All sectors" && sec !== sector) continue;
      out.push(safeStr(t.industry, "Unknown"));
    }
    return ["All industries", ...Array.from(new Set(out)).sort((a, b) => a.localeCompare(b))];
  }, [tiles, sector]);

  const filtered = useMemo(() => {
    const q = query.trim().toUpperCase();
    return tiles.filter((t) => {
      const sec = safeStr(t.sector, "Unknown");
      const ind = safeStr(t.industry, "Unknown");
      const sym = safeStr(t.symbol, "").toUpperCase();
      const nm = safeStr(t.name, "").toUpperCase();

      if (sector !== "All sectors" && sec !== sector) return false;
      if (industry !== "All industries" && ind !== industry) return false;
      if (q && !sym.includes(q) && !nm.includes(q)) return false;

      const mc = isFiniteNum(t.market_cap) ? t.market_cap : 0;
      return mc > 0;
    });
  }, [tiles, sector, industry, query]);

  // ---------- LAYOUT: sector mosaic -> industry treemap -> ticker treemap ----------
  const layout = useMemo(() => {
    const outer: Rect = {
      x: PAD_OUTER,
      y: PAD_OUTER,
      w: CANVAS_W - PAD_OUTER * 2,
      h: CANVAS_H - PAD_OUTER * 2,
    };

    // group tiles by sector
    const bySector = new Map<string, HeatmapTile[]>();
    for (const t of filtered) {
      const sec = safeStr(t.sector, "Unknown");
      if (!bySector.has(sec)) bySector.set(sec, []);
      bySector.get(sec)!.push(t);
    }

    const sectorValues: Array<{ sector: string; value: number }> = [];
    for (const [sec, arr] of bySector.entries()) {
      const v = arr.reduce((s, t) => s + (isFiniteNum(t.market_cap) ? t.market_cap : 0), 0);
      if (v > 0) sectorValues.push({ sector: sec, value: v });
    }

    // if user selects a sector, treat it as one big sector box
    let sectorBoxes: SectorBox[] = [];
    if (sector !== "All sectors") {
      const v = sectorValues.find((s) => s.sector === sector)?.value ?? 1;
      sectorBoxes = [{ sector, value: v, rect: outer }];
    } else {
      sectorBoxes = layoutSectorsInColumns(sectorValues, 3, outer);
    }

    const industryBoxes: IndustryBox[] = [];
    const tileBoxes: TileBox[] = [];

    for (const sb of sectorBoxes) {
      const secTiles = (bySector.get(sb.sector) ?? []).slice();

      const sectorInner: Rect = {
        x: sb.rect.x + PAD_SECTOR,
        y: sb.rect.y + PAD_SECTOR + SECTOR_HEADER_H,
        w: sb.rect.w - PAD_SECTOR * 2,
        h: sb.rect.h - PAD_SECTOR * 2 - SECTOR_HEADER_H,
      };

      if (sectorInner.w <= 20 || sectorInner.h <= 20) continue;

      // group by industry inside sector
      const byInd = new Map<string, HeatmapTile[]>();
      for (const t of secTiles) {
        const ind = safeStr(t.industry, "Unknown");
        if (industry !== "All industries" && ind !== industry) continue;
        if (!byInd.has(ind)) byInd.set(ind, []);
        byInd.get(ind)!.push(t);
      }

      const inds: Array<{ industry: string; value: number; tiles: HeatmapTile[] }> = [];
      for (const [ind, arr] of byInd.entries()) {
        const v = arr.reduce((s, t) => s + (isFiniteNum(t.market_cap) ? t.market_cap : 0), 0);
        if (v > 0) inds.push({ industry: ind, value: v, tiles: arr });
      }

      // industries treemap within sector
      const indRects = squarify(inds, (x) => x.value, sectorInner);

      for (const ir of indRects) {
        const indRect = ir.rect;
        industryBoxes.push({
          sector: sb.sector,
          industry: ir.item.industry,
          value: ir.item.value,
          rect: indRect,
        });

        const bigEnoughForHeader = indRect.w * indRect.h > 9000 && indRect.h > IND_HEADER_H + 18;

        const indInner: Rect = {
          x: indRect.x + PAD_INDUSTRY,
          y: indRect.y + PAD_INDUSTRY + (bigEnoughForHeader ? IND_HEADER_H : 0),
          w: indRect.w - PAD_INDUSTRY * 2,
          h: indRect.h - PAD_INDUSTRY * 2 - (bigEnoughForHeader ? IND_HEADER_H : 0),
        };

        if (indInner.w <= 10 || indInner.h <= 10) continue;

        const tRects = squarify(ir.item.tiles, (t) => (isFiniteNum(t.market_cap) ? t.market_cap : 0), indInner);

        for (const tr of tRects) {
          const t = tr.item;
          tileBoxes.push({
            ...t,
            sector: sb.sector,
            industry: ir.item.industry,
            x: tr.rect.x,
            y: tr.rect.y,
            w: tr.rect.w,
            h: tr.rect.h,
            key: `${sb.sector}:${ir.item.industry}:${t.symbol}`,
          });
        }
      }
    }

    return { sectorBoxes, industryBoxes, tileBoxes };
  }, [filtered, sector, industry]);

  function pctX(x: number) {
    return (x / CANVAS_W) * 100;
  }
  function pctY(y: number) {
    return (y / CANVAS_H) * 100;
  }

  function onTileMove(e: React.MouseEvent, t: HeatmapTile) {
    const el = containerRef.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    setHover({
      tile: t,
      px: e.clientX - r.left,
      py: e.clientY - r.top,
    });
  }

  function clearHover() {
    setHover(null);
  }

  // Breadcrumb behavior
  const crumbSector = sector !== "All sectors" ? sector : null;
  const crumbIndustry = industry !== "All industries" ? industry : null;

  return (
    <div className="rounded-2xl border bg-white overflow-hidden shadow-sm">
      {/* Header */}
      <div className="px-4 py-3 bg-gray-50 border-b">
        {/* Breadcrumb row (Finviz-like) */}
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="text-sm text-gray-700">
            <button
              className="font-semibold hover:underline"
              onClick={() => {
                setSector("All sectors");
                setIndustry("All industries");
              }}
            >
              All sectors
            </button>
            {crumbSector && (
              <>
                <span className="text-gray-400"> / </span>
                <button
                  className="font-semibold hover:underline"
                  onClick={() => {
                    setSector(crumbSector);
                    setIndustry("All industries");
                  }}
                >
                  {crumbSector}
                </button>
              </>
            )}
            {crumbIndustry && (
              <>
                <span className="text-gray-400"> / </span>
                <span className="font-semibold">{crumbIndustry}</span>
              </>
            )}

            <span className="text-gray-400"> · </span>
            <span className="text-gray-600">
              Latest trading day: <span className="font-medium">{data.asof}</span>
            </span>

            {data.updated_at_utc ? (
              <>
                <span className="text-gray-400"> · </span>
                <span className="text-gray-500">
                  Updated: {data.updated_at_utc.replace("T", " ").replace("Z", " UTC")}
                </span>
              </>
            ) : null}
          </div>

          <div className="flex items-center gap-2 text-xs text-gray-600">
            <span>{mode === "sentiment" ? "-0.6" : "-5%"}</span>
            <div
              className="h-2 w-56 rounded-full border"
              style={{
                background:
                  "linear-gradient(90deg, hsl(0 75% 45%), hsl(0 75% 92%), hsl(120 75% 92%), hsl(120 75% 45%))",
              }}
            />
            <span>{mode === "sentiment" ? "+0.6" : "+5%"}</span>
          </div>
        </div>

        {/* Controls row */}
        <div className="mt-3 flex flex-wrap items-center justify-between gap-2">
          <div className="flex flex-wrap items-center gap-2">
            <select
              className="border rounded-md px-2 py-1 bg-white min-w-[200px]"
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
              className="border rounded-md px-2 py-1 bg-white min-w-[260px]"
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
              className="border rounded-md px-2 py-1 bg-white w-[260px]"
              placeholder="Search ticker or name…"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
            />
          </div>

          <div className="border rounded-md overflow-hidden flex">
            <button
              className={`px-3 py-1 ${mode === "sentiment" ? "bg-black text-white" : "bg-white"}`}
              onClick={() => setMode("sentiment")}
            >
              Sentiment
            </button>
            <button
              className={`px-3 py-1 ${mode === "return" ? "bg-black text-white" : "bg-white"}`}
              onClick={() => setMode("return")}
            >
              Return
            </button>
          </div>
        </div>

        <div className="mt-2 text-xs text-gray-500">
          Tip: hover for details; click a tile to open its ticker page. Click sector/industry labels to drill down.
        </div>
      </div>

      {/* Map */}
      <div className="p-3 bg-white">
        <div
          ref={containerRef}
          className="relative w-full rounded-xl border bg-white overflow-hidden"
          style={{ height: "72vh", minHeight: 520 }}
          onMouseLeave={clearHover}
        >
          {/* Sector frames + headers */}
          {layout.sectorBoxes.map((sb) => {
            const left = pctX(sb.rect.x);
            const top = pctY(sb.rect.y);
            const width = pctX(sb.rect.w);
            const height = pctY(sb.rect.h);

            const showHeader = sb.rect.w * sb.rect.h > 16000;

            return (
              <div
                key={`sector:${sb.sector}`}
                className="absolute rounded-lg border bg-gray-50/30"
                style={{
                  left: `${left}%`,
                  top: `${top}%`,
                  width: `${width}%`,
                  height: `${height}%`,
                  zIndex: 1,
                }}
              >
                {showHeader ? (
                  <button
                    className="absolute left-2 top-1 text-xs font-semibold text-gray-700 hover:underline"
                    onClick={() => {
                      setSector(sb.sector);
                      setIndustry("All industries");
                    }}
                  >
                    {sb.sector}
                  </button>
                ) : null}
              </div>
            );
          })}

          {/* Industry borders + labels */}
          {layout.industryBoxes.map((ib) => {
            const left = pctX(ib.rect.x);
            const top = pctY(ib.rect.y);
            const width = pctX(ib.rect.w);
            const height = pctY(ib.rect.h);

            const area = ib.rect.w * ib.rect.h;
            const show = area > 24000 && ib.rect.h > IND_HEADER_H + 18;

            return (
              <div
                key={`ind:${ib.sector}:${ib.industry}`}
                className="absolute rounded-md border border-white/70 bg-white/0"
                style={{
                  left: `${left}%`,
                  top: `${top}%`,
                  width: `${width}%`,
                  height: `${height}%`,
                  zIndex: 2,
                }}
              >
                {show ? (
                  <button
                    className="absolute left-2 top-1 text-[11px] font-semibold text-gray-700 hover:underline"
                    onClick={() => {
                      setSector(ib.sector);
                      setIndustry(ib.industry);
                    }}
                  >
                    {ib.industry}
                  </button>
                ) : null}
              </div>
            );
          })}

          {/* Ticker tiles */}
          {layout.tileBoxes.map((t) => {
            const left = pctX(t.x);
            const top = pctY(t.y);
            const width = pctX(t.w);
            const height = pctY(t.h);

            const metric = mode === "sentiment" ? (t.sentiment ?? null) : (t.return_1d ?? null);
            const { bg, fg } = colorForMetric(metric as any, mode);

            const area = t.w * t.h;

            // Finviz-like label behavior:
            // - ticker only if enough space
            // - details only if very large
            const showTicker = area > 2200;
            const showDetail = area > 16000;

            const fontTicker = clamp(Math.sqrt(area) / 12, 10, 22);
            const fontDetail = clamp(fontTicker - 7, 10, 14);

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
                  zIndex: 3,
                }}
                onMouseMove={(e) => onTileMove(e, t)}
              >
                {showTicker ? (
                  <div
                    className="w-full h-full flex flex-col items-center justify-center text-center px-1 select-none"
                    style={{
                      textShadow: "0 1px 1px rgba(0,0,0,0.12)",
                    }}
                  >
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

          {/* Hover tooltip */}
          {hover && (
            <div
              className="absolute z-50 pointer-events-none"
              style={{
                left: clamp(hover.px + 14, 8, (containerRef.current?.clientWidth ?? 1200) - 320),
                top: clamp(hover.py + 14, 8, (containerRef.current?.clientHeight ?? 700) - 175),
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
