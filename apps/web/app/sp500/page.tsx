// apps/web/app/sp500/page.tsx
import fs from "node:fs/promises";
import path from "node:path";
import Link from "next/link";

export const metadata = {
  title: "S&P 500 — Market Sentiment",
  description: "S&P 500 index sentiment (cap-weighted) dashboard.",
};

type DailyRow = Record<string, any> & {
  date: string;
  sentiment_cap_weighted?: number;
};

type SpxJson = {
  symbol: string;
  name?: string;
  price_symbol_candidates?: string[];
  news_symbol_candidates?: string[];
  daily?: DailyRow[];
};

function fmtNum(x: any, digits = 2) {
  const n = Number(x);
  if (!Number.isFinite(n)) return "—";
  return n.toLocaleString(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  });
}

function fmtPct(x: any, digits = 2) {
  const n = Number(x);
  if (!Number.isFinite(n)) return "—";
  return `${(n * 100).toFixed(digits)}%`;
}

function pickCloseKey(sample: DailyRow | undefined): string | null {
  if (!sample) return null;
  const keys = Object.keys(sample);
  const k = keys.find((kk) => kk.startsWith("close_"));
  return k ?? null;
}

function normalize(arr: number[]) {
  const finite = arr.filter((x) => Number.isFinite(x));
  if (finite.length === 0) return arr.map(() => NaN);
  const mn = Math.min(...finite);
  const mx = Math.max(...finite);
  const den = mx - mn || 1;
  return arr.map((x) => (Number.isFinite(x) ? (x - mn) / den : NaN));
}

function buildPolylinePoints(norm: number[], w: number, h: number, pad = 18) {
  const usableW = w - pad * 2;
  const usableH = h - pad * 2;
  const n = norm.length;
  if (n <= 1) return "";
  const pts: string[] = [];
  for (let i = 0; i < n; i++) {
    const yv = norm[i];
    if (!Number.isFinite(yv)) continue;
    const x = pad + (usableW * i) / (n - 1);
    const y = pad + usableH * (1 - yv);
    pts.push(`${x.toFixed(2)},${y.toFixed(2)}`);
  }
  return pts.join(" ");
}

async function loadSpxJson(): Promise<SpxJson | null> {
  // apps/web 构建时 cwd 通常是 apps/web
  const candidates = [
    // 1) 你现在说的主路径（repo root）
    path.join(process.cwd(), "..", "..", "data", "SPX", "sp500_index.json"),
    // 2) 兼容你未来如果把它拷到 public/data
    path.join(process.cwd(), "public", "data", "SPX", "sp500_index.json"),
    path.join(process.cwd(), "public", "data", "sp500_index.json"),
  ];

  for (const p of candidates) {
    try {
      const raw = await fs.readFile(p, "utf-8");
      return JSON.parse(raw) as SpxJson;
    } catch {
      // try next
    }
  }
  return null;
}

export default async function Sp500Page() {
  const js = await loadSpxJson();

  if (!js?.daily?.length) {
    return (
      <div className="space-y-4">
        <h1 className="text-2xl font-semibold">S&amp;P 500 (SPX)</h1>
        <p className="text-sm text-gray-600">
          没有找到 SPX 数据文件，或 daily 为空。
        </p>
        <div className="rounded-xl border p-4 text-sm text-gray-700 space-y-2">
          <div>我会按以下路径尝试读取：</div>
          <ul className="list-disc pl-5">
            <li>data/SPX/sp500_index.json（repo 根目录）</li>
            <li>apps/web/public/data/SPX/sp500_index.json</li>
            <li>apps/web/public/data/sp500_index.json</li>
          </ul>
          <div className="pt-2">
            你现在给的路径是：<code className="px-1 py-0.5 border rounded">data/SPX/sp500_index.json</code>
          </div>
        </div>
        <Link className="underline text-sm" href="/">
          ← Back to Home
        </Link>
      </div>
    );
  }

  const daily = js.daily.slice().sort((a, b) => (a.date < b.date ? -1 : 1));
  const closeKey = pickCloseKey(daily[0]);
  const latest = daily[daily.length - 1];

  const closes = daily.map((r) => Number(closeKey ? r[closeKey] : NaN));
  const sents = daily.map((r) => Number(r.sentiment_cap_weighted));

  const closeNorm = normalize(closes);
  const sentNorm = normalize(sents);

  const W = 980;
  const H = 320;
  const pricePts = buildPolylinePoints(closeNorm, W, H);
  const sentPts = buildPolylinePoints(sentNorm, W, H);

  const latestClose = closeKey ? latest[closeKey] : null;
  const latestSent = latest.sentiment_cap_weighted;

  return (
    <div className="space-y-6">
      <div className="flex items-end justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold">S&amp;P 500 (SPX)</h1>
          <p className="text-sm text-gray-600">
            Index-level sentiment (cap-weighted). No headlines on this page.
          </p>
        </div>
        <Link className="text-sm underline" href="/">
          Home
        </Link>
      </div>

      {/* KPI cards */}
      <div className="grid gap-4 md:grid-cols-3">
        <div className="rounded-xl border p-4">
          <div className="text-xs text-gray-500">Latest date</div>
          <div className="text-lg font-semibold">{latest.date ?? "—"}</div>
        </div>
        <div className="rounded-xl border p-4">
          <div className="text-xs text-gray-500">Close ({closeKey ?? "close"})</div>
          <div className="text-lg font-semibold">{fmtNum(latestClose, 2)}</div>
        </div>
        <div className="rounded-xl border p-4">
          <div className="text-xs text-gray-500">Cap-weighted sentiment</div>
          <div className="text-lg font-semibold">{fmtPct(latestSent, 2)}</div>
        </div>
      </div>

      {/* Chart */}
      <div className="rounded-xl border p-4">
        <div className="flex items-end justify-between gap-4 mb-3">
          <div>
            <div className="text-sm font-medium">Price vs. Sentiment</div>
            <div className="text-xs text-gray-500">
              Normalized overlay for visual comparison (not same units).
            </div>
          </div>
          <div className="text-xs text-gray-500">
            Source: {js.price_symbol_candidates?.join(", ") ?? "—"}
          </div>
        </div>

        <div className="w-full overflow-x-auto">
          <svg
            viewBox={`0 0 ${W} ${H}`}
            className="w-[980px] max-w-full"
            role="img"
            aria-label="SPX price and sentiment chart"
          >
            {/* background */}
            <rect x="0" y="0" width={W} height={H} fill="white" />

            {/* grid lines */}
            {Array.from({ length: 5 }).map((_, i) => {
              const y = 18 + ((H - 36) * i) / 4;
              return (
                <line
                  key={i}
                  x1="18"
                  x2={W - 18}
                  y1={y}
                  y2={y}
                  stroke="#e5e7eb"
                  strokeWidth="1"
                />
              );
            })}

            {/* price */}
            {pricePts && (
              <polyline
                points={pricePts}
                fill="none"
                stroke="#111827"
                strokeWidth="2"
              />
            )}

            {/* sentiment */}
            {sentPts && (
              <polyline
                points={sentPts}
                fill="none"
                stroke="#6b7280"
                strokeWidth="2"
                strokeDasharray="6 6"
              />
            )}
          </svg>
        </div>

        <div className="mt-3 flex flex-wrap gap-3 text-sm">
          <span className="inline-flex items-center gap-2">
            <span className="inline-block h-0.5 w-6 bg-gray-900" />
            Price
          </span>
          <span className="inline-flex items-center gap-2">
            <span className="inline-block h-0.5 w-6 bg-gray-500" style={{ borderTop: "2px dashed #6b7280" }} />
            Sentiment (cap-weighted)
          </span>
        </div>
      </div>

      {/* Expandable explainer */}
      <details className="rounded-xl border p-4">
        <summary className="cursor-pointer select-none font-medium">
          ❓ how sp500 sentiment is calculated?
        </summary>
        <div className="mt-3 space-y-2 text-sm text-gray-700">
          <p>
            The index sentiment is computed as a <b>capital-weighted</b> aggregation
            of constituent-level sentiment signals.
          </p>
          <p>
            Intuition: each constituent contributes a daily sentiment score; we
            then take a weighted average using its market-cap weight, so larger
            companies have proportionally larger influence on the index-level
            sentiment.
          </p>
          <p className="text-gray-600">
            This page intentionally omits news headlines to keep it stable and
            lightweight. For news + sentiment at the company level, use the
            per-ticker pages.
          </p>
        </div>
      </details>

      <div className="text-sm text-gray-600">
        Want to see ticker-level news? Go to{" "}
        <Link className="underline" href="/">
          Home
        </Link>{" "}
        and click a ticker.
      </div>
    </div>
  );
}
