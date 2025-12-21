"use client";

import { useMemo, useRef, useState, useEffect } from "react";
import { useRouter } from "next/navigation";

type HeatmapRow = {
  symbol: string;
  name: string;
  sector: string;
  industry: string;
  market_cap: number;
  weight: number;
  sentiment: number | null;
  close: number | null;
  prev_close: number | null;
  return_1d: number | null;
};

type HeatmapSnapshot = {
  date: string;
  rows: HeatmapRow[];
  sector_stats: Array<{
    sector: string;
    weight_sum: number;
    market_cap_sum: number;
    sentiment_wavg: number | null;
    return_wavg: number | null;
    contribution_sum: number;
    n: number;
  }>;
};

type Sp500HeatmapFile = {
  symbol: string;
  name: string;
  asof: { latest_trading_day: string; current: string };
  snapshots: {
    latest_trading_day: HeatmapSnapshot;
    current: HeatmapSnapshot;
  };
};

type Props = { file: Sp500HeatmapFile };
type Metric = "sentiment" | "return_1d";

function clamp(x: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, x));
}

function fmtPct(x: number | null | undefined, digits = 2) {
  if (typeof x !== "number" || !Number.isFinite(x)) return "—";
  return `${(x * 100).toFixed(digits)}%`;
}

function fmtNum(x: number | null | undefined, digits = 4) {
  if (typeof x !== "number" || !Number.isFinite(x)) return "—";
  return x.toFixed(digits);
}

function fmtMoney(x: number | null | undefined, digits = 2) {
  if (typeof x !== "number" || !Number.isFinite(x)) return "—";
  return x.toFixed(digits);
}

function colorFor(v: number | null, domain: [number, number]) {
  if (v === null || !Number.isFinite(v)) return "hsl(0, 0%, 92%)";
  const [lo, hi] = domain;
  const x = clamp(v, lo, hi);
  const t = (x - lo) / (hi - lo + 1e-12); // 0..1
  const centered = t * 2 - 1; // -1..1

  const mag = Math.abs(centered);
  const light = 82 - 42 * mag;
  const sat = 45 + 40 * mag;
  const hue = centered >= 0 ? 120 : 0; // green / red
  return `hsl(${hue}, ${sat}%, ${light}%)`;
}

function textColorForHsl(bg: string) {
  const m = bg.match(/hsl\(\s*\d+\s*,\s*\d+%\s*,\s*(\d+)%\s*\)/);
  if (!m) return "black";
  const l = Number(m[1]);
  return l < 60 ? "white" : "black";
}

type Item = {
  key: string;
  label: string;
  value: number;
  metric: number | null;
  kind: "group" | "leaf";
  rows: HeatmapRow[];
};

type Rect = { x: number; y: number; w: number; h: number; item: Item };

function layoutBinaryTreemap(items: Item[], x: number, y: number, w: number, h: number): Rect[] {
  const out: Rect[] = [];
  const safe = items.filter((it) => Number.isFinite(it.value) && it.value > 0);
  if (!safe.length) return out;

  const sorted = [...safe].sort((a, b) => b.value - a.value);

  const rec = (arr: Item[], rx: number, ry: number, rw: number, rh: number) => {
    if (arr.length === 1) {
      out.push({ x: rx, y: ry, w: rw, h: rh, item: arr[0] });
      return;
    }
    const total = arr.reduce((s, it) => s + it.value, 0);
    if (total <= 0) return;

    let acc = 0;
    let k = 0;
    while (k < arr.length && acc + arr[k].value < total / 2) {
      acc += arr[k].value;
      k++;
    }
    if (k <= 0) k = 1;
    if (k >= arr.length) k = arr.length - 1;

    const left = arr.slice(0, k);
    const right = arr.slice(k);

    const sumL = left.reduce((s, it) => s + it.value, 0);

    if (rw >= rh) {
      const wL = rw * (sumL / total);
      rec(left, rx, ry, wL, rh);
      rec(right, rx + wL, ry, rw - wL, rh);
    } else {
      const hT = rh * (sumL / total);
      rec(left, rx, ry, rw, hT);
      rec(right, rx, ry + hT, rw, rh - hT);
    }
  };

  rec(sorted, x, y, w, h);
  return out;
}

function wavg(rows: HeatmapRow[], metric: Metric) {
  let num = 0;
  let den = 0;
  for (const r of rows) {
    const v = r[metric];
    if (typeof v !== "number" || !Number.isFinite(v)) continue;
    const w = typeof r.weight === "number" && Number.isFinite(r.weight) ? r.weight : 0;
    if (w <= 0) continue;
    num += w * v;
    den += w;
  }
  return den > 0 ? num / den : null;
}

function groupItems(rows: HeatmapRow[], level: 0 | 1 | 2, sectorPick?: string, industryPick?: string): Item[] {
  const filtered = rows.filter((r) => {
    if (sectorPick && r.sector !== sectorPick) return false;
    if (industryPick && r.industry !== industryPick) return false;
    return true;
  });

  if (level === 2) {
    return filtered.map((r) => ({
      key: r.symbol,
      label: r.symbol,
      value: Number.isFinite(r.market_cap) ? r.market_cap : r.weight ?? 0,
      metric: r.sentiment,
      kind: "leaf",
      rows: [r],
    }));
  }

  const by = new Map<string, HeatmapRow[]>();
  for (const r of filtered) {
    const k = level === 0 ? (r.sector || "Unknown") : (r.industry || "Unknown");
    const arr = by.get(k) ?? [];
    arr.push(r);
    by.set(k, arr);
  }

  const out: Item[] = [];
  for (const [k, rs] of by.entries()) {
    out.push({
      key: k,
      label: k,
      value: rs.reduce((s, r) => s + (Number.isFinite(r.market_cap) ? r.market_cap : 0), 0),
      metric: null,
      kind: "group",
      rows: rs,
    });
  }
  return out;
}

export default function Sp500HeatmapClient({ file }: Props) {
  const router = useRouter();

  const [which, setWhich] = useState<"latest_trading_day" | "current">("latest_trading_day");
  const [metric, setMetric] = useState<Metric>("sentiment");
  const [sectorPick, setSectorPick] = useState<string | null>(null);
  const [industryPick, setIndustryPick] = useState<string | null>(null);

  const snap = file.snapshots[which];
  const domain: [number, number] = metric === "sentiment" ? [-0.5, 0.5] : [-0.03, 0.03];

  const level: 0 | 1 | 2 = industryPick ? 2 : sectorPick ? 1 : 0;

  const items = useMemo(() => {
    const raw = groupItems(snap.rows, level, sectorPick ?? undefined, industryPick ?? undefined);
    return raw
      .map((it) => ({
        ...it,
        metric: it.kind === "leaf" ? it.rows[0][metric] : wavg(it.rows, metric),
      }))
      .filter((it) => Number.isFinite(it.value) && it.value > 0)
      .sort((a, b) => b.value - a.value);
  }, [snap.rows, level, sectorPick, industryPick, metric]);

  const boxRef = useRef<HTMLDivElement | null>(null);
  const [box, setBox] = useState<{ w: number; h: number }>({ w: 900, h: 520 });

  useEffect(() => {
    if (!boxRef.current) return;
    const el = boxRef.current;
    const ro = new ResizeObserver(() => {
      const r = el.getBoundingClientRect();
      setBox({ w: Math.max(320, Math.floor(r.width)), h: Math.max(360, Math.floor(r.height)) });
    });
    ro.observe(el);
    const r = el.getBoundingClientRect();
    setBox({ w: Math.max(320, Math.floor(r.width)), h: Math.max(360, Math.floor(r.height)) });
    return () => ro.disconnect();
  }, []);

  const rects = useMemo(() => layoutBinaryTreemap(items, 0, 0, box.w, box.h), [items, box.w, box.h]);

  const breadcrumb = useMemo(() => {
    if (!sectorPick) return [{ label: "All sectors", disabled: true, onClick: () => {} }];
    if (!industryPick) {
      return [
        { label: "All sectors", disabled: false, onClick: () => setSectorPick(null) },
        { label: sectorPick, disabled: true, onClick: () => {} },
      ];
    }
    return [
      { label: "All sectors", disabled: false, onClick: () => { setSectorPick(null); setIndustryPick(null); } },
      { label: sectorPick, disabled: false, onClick: () => setIndustryPick(null) },
      { label: industryPick, disabled: true, onClick: () => {} },
    ];
  }, [sectorPick, industryPick]);

  const topSectors = useMemo(() => [...snap.sector_stats].slice(0, 12), [snap.sector_stats]);

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div className="flex items-center gap-2 flex-wrap">
          <div className="inline-flex rounded-lg border overflow-hidden text-sm">
            <button
              className={`px-3 py-1.5 ${which === "latest_trading_day" ? "bg-gray-900 text-white" : "bg-white"}`}
              onClick={() => setWhich("latest_trading_day")}
            >
              Latest trading day
            </button>
            <button
              className={`px-3 py-1.5 ${which === "current" ? "bg-gray-900 text-white" : "bg-white"}`}
              onClick={() => setWhich("current")}
            >
              Current
            </button>
          </div>

          <div className="inline-flex rounded-lg border overflow-hidden text-sm">
            <button
              className={`px-3 py-1.5 ${metric === "sentiment" ? "bg-gray-900 text-white" : "bg-white"}`}
              onClick={() => setMetric("sentiment")}
            >
              Color: Sentiment
            </button>
            <button
              className={`px-3 py-1.5 ${metric === "return_1d" ? "bg-gray-900 text-white" : "bg-white"}`}
              onClick={() => setMetric("return_1d")}
            >
              Color: Return (1D)
            </button>
          </div>

          <div className="text-xs text-gray-500">
            Showing: <span className="font-medium">{snap.date}</span>
          </div>
        </div>

        <div className="flex items-center gap-2 text-xs text-gray-600">
          <span>Neg</span>
          <div
            className="h-2 w-28 rounded"
            style={{ background: "linear-gradient(90deg, hsl(0, 85%, 45%), hsl(0, 0%, 92%), hsl(120, 85%, 45%))" }}
          />
          <span>Pos</span>
          <span className="ml-2 text-gray-400">({metric === "sentiment" ? "≈ [-0.5, 0.5]" : "≈ [-3%, 3%]"})</span>
        </div>
      </div>

      <div className="border rounded-lg p-3">
        <div className="text-sm font-medium mb-2">Sector contribution (cap-weighted)</div>
        <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
          {topSectors.map((s) => {
            const v = metric === "sentiment" ? s.sentiment_wavg : s.return_wavg;
            const bg = colorFor(v, domain);
            const tc = textColorForHsl(bg);
            return (
              <div key={s.sector} className="flex items-center justify-between gap-2">
                <div className="truncate text-sm">{s.sector}</div>
                <div className="flex items-center gap-2">
                  <div className="text-xs text-gray-500 w-16 text-right">{fmtPct(s.weight_sum, 1)}</div>
                  <div className="px-2 py-1 rounded text-xs" style={{ background: bg, color: tc, minWidth: 84, textAlign: "right" }}>
                    {metric === "sentiment" ? fmtNum(s.sentiment_wavg, 3) : fmtPct(s.return_wavg, 2)}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      <div className="flex items-center gap-2 text-sm">
        {breadcrumb.map((b, i) => (
          <div key={i} className="flex items-center gap-2">
            {i > 0 ? <span className="text-gray-400">/</span> : null}
            <button
              className={`underline-offset-2 ${b.disabled ? "text-gray-800 font-medium" : "text-gray-600 hover:underline"}`}
              onClick={() => !b.disabled && b.onClick()}
              disabled={b.disabled}
            >
              {b.label}
            </button>
          </div>
        ))}
      </div>

      <div className="border rounded-lg overflow-hidden">
        <div ref={boxRef} className="relative w-full" style={{ height: 520 }}>
          {rects.map((r) => {
            const bg = colorFor(r.item.metric, domain);
            const tc = textColorForHsl(bg);
            const area = r.w * r.h;
            const small = area < 1400;
            const verySmall = area < 800;

            return (
              <button
                key={r.item.key}
                className="absolute border border-white/60 text-left p-2 focus:outline-none"
                style={{
                  left: r.x,
                  top: r.y,
                  width: r.w,
                  height: r.h,
                  background: bg,
                  color: tc,
                }}
                title={
                  r.item.kind === "leaf"
                    ? `${r.item.rows[0].symbol} ${r.item.rows[0].name}\n${r.item.rows[0].sector} / ${r.item.rows[0].industry}\nSent: ${fmtNum(
                        r.item.rows[0].sentiment,
                        4
                      )}\nClose: ${fmtMoney(r.item.rows[0].close, 2)}  1D: ${fmtPct(r.item.rows[0].return_1d, 2)}`
                    : `${r.item.label}\n# constituents: ${r.item.rows.length}\nAvg: ${
                        metric === "sentiment" ? fmtNum(r.item.metric, 4) : fmtPct(r.item.metric, 2)
                      }`
                }
                onClick={() => {
                  if (r.item.kind === "leaf") {
                    router.push(`/ticker/${r.item.rows[0].symbol}`);
                    return;
                  }
                  if (!sectorPick) {
                    setSectorPick(r.item.label);
                    setIndustryPick(null);
                    return;
                  }
                  if (sectorPick && !industryPick) {
                    setIndustryPick(r.item.label);
                    return;
                  }
                }}
              >
                {!verySmall ? (
                  <div className="leading-tight">
                    <div className={`font-semibold ${small ? "text-xs" : "text-sm"}`}>
                      {r.item.kind === "leaf" ? r.item.rows[0].symbol : r.item.label}
                    </div>
                    {!small ? (
                      <div className="text-xs opacity-90 mt-1">
                        {r.item.kind === "leaf" ? (
                          <>
                            ({fmtMoney(r.item.rows[0].close, 1)}, {fmtNum(r.item.rows[0].sentiment, 2)})
                          </>
                        ) : (
                          <>
                            {r.item.rows.length} names · {metric === "sentiment" ? fmtNum(r.item.metric, 3) : fmtPct(r.item.metric, 2)}
                          </>
                        )}
                      </div>
                    ) : null}
                  </div>
                ) : null}
              </button>
            );
          })}
        </div>

        <div className="px-3 py-2 text-xs text-gray-500 border-t bg-gray-50">
          Tip: click tiles to drill down. Click a ticker tile to open its page.
        </div>
      </div>
    </div>
  );
}
