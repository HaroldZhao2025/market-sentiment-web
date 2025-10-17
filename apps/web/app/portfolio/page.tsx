// apps/web/app/portfolio/page.tsx
import fs from "node:fs/promises";
import path from "node:path";

export const dynamic = "error";
export const revalidate = false;

type Portfolio = {
  dates: string[];
  long: number[];
  short: number[];
  long_short: number[];
};

async function readJson(filePath: string): Promise<any | null> {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function loadPortfolio(): Promise<Portfolio | null> {
  const p = path.join(process.cwd(), "public", "data", "portfolio.json");
  const raw = await readJson(p);
  if (!raw || typeof raw !== "object") return null;
  const dates = raw.dates || raw.date || [];
  return {
    dates: Array.isArray(dates) ? dates : [],
    long: Array.isArray(raw.long) ? raw.long.map(Number) : [],
    short: Array.isArray(raw.short) ? raw.short.map(Number) : [],
    long_short: Array.isArray(raw.long_short) ? raw.long_short.map(Number) : [],
  };
}

export default async function PortfolioPage() {
  const pf = await loadPortfolio();

  return (
    <main className="mx-auto max-w-5xl p-6">
      <h1 className="text-2xl font-bold mb-4">Portfolio</h1>
      {!pf || pf.dates.length === 0 ? (
        <p className="text-sm text-gray-500">Portfolio series is empty.</p>
      ) : (
        <div className="space-y-3">
          <p className="text-sm">Points: {pf.dates.length}</p>
          <div className="rounded-xl border p-4 bg-white">
            <h3 className="font-semibold mb-2">Last 5 rows</h3>
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-gray-500">
                  <th className="py-1 pr-4">Date</th>
                  <th className="py-1 pr-4">Long</th>
                  <th className="py-1 pr-4">Short</th>
                  <th className="py-1 pr-4">L/S</th>
                </tr>
              </thead>
              <tbody>
                {pf.dates.slice(-5).map((d, i) => {
                  const idx = pf.dates.length - 5 + i;
                  return (
                    <tr key={d}>
                      <td className="py-1 pr-4">{d}</td>
                      <td className="py-1 pr-4">{pf.long[idx]?.toFixed(4)}</td>
                      <td className="py-1 pr-4">{pf.short[idx]?.toFixed(4)}</td>
                      <td className="py-1 pr-4">{pf.long_short[idx]?.toFixed(4)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </main>
  );
}
