a// apps/web/app/page.tsx
import fs from "node:fs/promises";
import path from "node:path";
import Link from "next/link";

export const dynamic = "error";
export const dynamicParams = false;
export const revalidate = false;

async function getTickers(): Promise<string[]> {
  try {
    const p = path.join(process.cwd(), "public", "data", "_tickers.json");
    const raw = await fs.readFile(p, "utf8");
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr.map((x: any) => String(x).toUpperCase()) : ["AAPL"];
  } catch {
    return ["AAPL"];
  }
}

type SPData = {
  dates: string[];
  price: number[];
  sentiment: number[];
};

async function readJSON<T = any>(p: string): Promise<T | null> {
  try {
    return JSON.parse(await fs.readFile(p, "utf8")) as T;
  } catch {
    return null;
  }
}

// attempt to load S&P-like series from multiple sources:
// 1) public/data/index/sp500.json (preferred if you add it later)
// 2) public/data/ticker/SPY.json (proxy)
// 3) public/data/ticker/VOO.json (proxy)
async function getSP500(): Promise<SPData | null> {
  const candidates = [
    path.join(process.cwd(), "public", "data", "index", "sp500.json"),
    path.join(process.cwd(), "public", "data", "ticker", "SPY.json"),
    path.join(process.cwd(), "public", "data", "ticker", "VOO.json"),
  ];

  for (const p of candidates) {
    const j = await readJSON<any>(p);
    if (!j) continue;

    // flexible field mapping
    const dates = Array.isArray(j?.dates) ? j.dates.map(String) : Array.isArray(j?.date) ? j.date.map(String) : [];
    const price =
      Array.isArray(j?.price)
        ? j.price.map((x: any) => Number(x) || 0)
        : Array.isArray(j?.close)
        ? j.close.map((x: any) => Number(x) || 0)
        : Array.isArray(j?.Close)
        ? j.Close.map((x: any) => Number(x) || 0)
        : [];
    const sentiment =
      Array.isArray(j?.sentiment)
        ? j.sentiment.map((x: any) => Number(x) || 0)
        : Array.isArray(j?.S)
        ? j.S.map((x: any) => Number(x) || 0)
        : [];

    const n = Math.min(dates.length, price.length || Infinity, sentiment.length || Infinity);
    if (Number.isFinite(n) && n > 1) {
      return {
        dates: dates.slice(-200, n), // keep last ~200 points for a clean sparkline
        price: price.slice(-200, n),
        sentiment: sentiment.slice(-200, n),
      };
    }
  }
  return null;
}

// simple sparkline path generator (server-side)
function sparklinePath(values: number[], width: number, height: number, pad = 8): string {
  if (!values.length) return "";
  const xs = values.map((v) => (Number.isFinite(v) ? Number(v) : 0));
  const vmin = Math.min(...xs);
  const vmax = Math.max(...xs);
  const range = vmax - vmin || 1;

  const w = Math.max(1, width - pad * 2);
  const h = Math.max(1, height - pad * 2);

  const step = w / Math.max(1, xs.length - 1);
  return xs
    .map((v, i) => {
      const x = pad + i * step;
      // SVG y grows downward → invert
      const y = pad + (1 - (v - vmin) / range) * h;
      return `${i === 0 ? "M" : "L"}${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");
}

export default async function Home() {
  const [tickers, sp] = await Promise.all([getTickers(), getSP500()]);

  // build sparkline paths (if data exists)
  const W = 800;
  const H = 180;
  const pricePath = sp?.price?.length ? sparklinePath(sp.price, W, H) : "";
  const sentiPath = sp?.sentiment?.length ? sparklinePath(sp.sentiment, W, H) : "";

  return (
    <main className="min-h-screen p-6">
      <div className="mx-auto max-w-6xl space-y-8">
        {/* Header */}
        <header className="flex items-center justify-between">
          <h1 className="text-3xl font-bold tracking-tight">Market Sentiment</h1>
          <Link href="/" className="rounded-xl border px-3 py-1.5 text-sm hover:bg-neutral-50">
            Home
          </Link>
        </header>

        {/* S&P 500 Section (kept on the homepage even if data isn't ready yet) */}
        <section className="rounded-2xl border shadow-sm bg-white">
          <div className="flex items-center justify-between px-5 py-4 border-b">
            <div>
              <h2 className="text-lg font-semibold">S&amp;P&nbsp;500 Overview</h2>
              <p className="text-xs text-neutral-500">Sparkline preview of index price &amp; aggregated sentiment.</p>
            </div>
            {/* Keep a future link placeholder; disabled to avoid 404 if route not built yet */}
            <button
              className="rounded-lg px-3 py-1.5 text-sm border text-neutral-400 cursor-not-allowed"
              title="Coming soon"
              disabled
            >
              Full view
            </button>
          </div>

          <div className="px-5 py-4">
            {pricePath || sentiPath ? (
              <div className="space-y-2">
                <div className="flex items-center gap-4 text-xs text-neutral-600">
                  <div className="inline-flex items-center gap-1">
                    <span className="inline-block h-2 w-2 rounded-full bg-black/70" />
                    Price (normalized)
                  </div>
                  <div className="inline-flex items-center gap-1">
                    <span className="inline-block h-2 w-2 rounded-full bg-neutral-400" />
                    Sentiment (normalized)
                  </div>
                </div>

                <div className="overflow-hidden rounded-xl border bg-white">
                  <svg viewBox={`0 0 ${W} ${H}`} width="100%" height="200" role="img" aria-label="S&P 500 sparkline">
                    {/* gridline baseline */}
                    <line x1="8" y1={H - 24} x2={W - 8} y2={H - 24} stroke="currentColor" className="text-neutral-200" />
                    {/* price path */}
                    {pricePath && <path d={pricePath} fill="none" stroke="currentColor" className="text-black/70" strokeWidth="2" />}
                    {/* sentiment path */}
                    {sentiPath && <path d={sentiPath} fill="none" stroke="currentColor" className="text-neutral-400" strokeWidth="2" />}
                  </svg>
                </div>

                <div className="text-[11px] text-neutral-500">
                  {sp?.dates?.length ? `Last ${sp.dates.length} days` : "Awaiting data"}
                </div>
              </div>
            ) : (
              <div className="h-40 flex items-center justify-center text-neutral-500 text-sm">
                S&amp;P 500 chart coming soon — section reserved so layout remains stable.
              </div>
            )}
          </div>
        </section>

        {/* Ticker list */}
        <section>
          <h3 className="text-base font-semibold mb-3">Universe</h3>
          <ul className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 gap-3">
            {tickers.map((t) => (
              <li key={t}>
                {/* route-only href; Next.js will prepend basePath automatically */}
                <Link
                  href={`/ticker/${t}`}
                  className="block rounded-xl border shadow-sm px-4 py-3 hover:shadow transition"
                >
                  {t}
                </Link>
              </li>
            ))}
          </ul>
        </section>
      </div>
    </main>
  );
}
