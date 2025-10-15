import fs from "node:fs";
import path from "node:path";
import Link from "next/link";

type Portfolio = {
  series: { date: string[]; daily_ret: number[]; cumret: number[] };
  top: string[];
  bottom: string[];
};

export const revalidate = false;

function loadPortfolio(): Portfolio | null {
  try {
    const p = path.join(process.cwd(), "public", "data", "portfolio.json");
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch { return null; }
}

export default function PortfolioPage() {
  const pf = loadPortfolio();
  if (!pf) return (
    <main className="p-6 max-w-4xl mx-auto">
      <h1 className="text-xl font-bold mb-4">Portfolio</h1>
      <p>No portfolio data yet.</p>
      <Link className="underline" href="/">Home</Link>
    </main>
  );

  const last = pf.series.date.length ? pf.series.date[pf.series.date.length-1] : "";
  return (
    <main className="p-6 max-w-5xl mx-auto">
      <div className="flex justify-between items-center">
        <h1 className="text-xl font-bold">Long/Short Sentiment Portfolio</h1>
        <Link className="underline" href="/">Home</Link>
      </div>

      <section className="mt-6">
        <h2 className="font-semibold mb-2">Top/Bottom (latest: {last || "—"})</h2>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          <div>
            <div className="font-semibold mb-1">Top Predicted</div>
            <ul className="list-disc list-inside">
              {pf.top.map(s => <li key={s}><Link className="underline" href={`/ticker/${encodeURIComponent(s)}`}>{s}</Link></li>)}
            </ul>
          </div>
          <div>
            <div className="font-semibold mb-1">Bottom Predicted</div>
            <ul className="list-disc list-inside">
              {pf.bottom.map(s => <li key={s}><Link className="underline" href={`/ticker/${encodeURIComponent(s)}`}>{s}</Link></li>)}
            </ul>
          </div>
        </div>
      </section>

      <section className="mt-6">
        <h2 className="font-semibold mb-2">Returns</h2>
        <div className="rounded border p-3 overflow-x-auto">
          <table className="min-w-[600px] text-sm">
            <thead><tr><th className="px-2 py-1 text-left">Date</th><th className="px-2 py-1 text-right">Daily</th><th className="px-2 py-1 text-right">Cum</th></tr></thead>
            <tbody>
              {pf.series.date.map((d,i)=>(
                <tr key={d}>
                  <td className="px-2 py-1">{d}</td>
                  <td className="px-2 py-1 text-right">{pf.series.daily_ret[i]?.toFixed(4)}</td>
                  <td className="px-2 py-1 text-right">{pf.series.cumret[i]?.toFixed(4)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}
