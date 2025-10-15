import fs from "node:fs";
import path from "node:path";
import Link from "next/link";

type TickerData = {
  symbol: string;
  series: { date: string[]; close: number[]; S: number[]; S_news: number[]; S_earn: number[]; news_count: number[]; earn_count: number[]; };
  recent_headlines: { ts: string; title: string; url: string; score?: { pos: number; neg: number } }[];
};

export const dynamicParams = false;

export async function generateStaticParams() {
  const p = path.join(process.cwd(), "public", "data", "_tickers.json");
  const syms: string[] = fs.existsSync(p) ? JSON.parse(fs.readFileSync(p, "utf8")) : [];
  return syms.map(s => ({ symbol: s }));
}

function loadTicker(sym: string): TickerData | null {
  try {
    const p = path.join(process.cwd(), "public", "data", `${sym}.json`);
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch { return null; }
}

export default function TickerPage({ params }: { params: { symbol: string } }) {
  const sym = params.symbol;
  const data = loadTicker(sym);
  if (!data) return (
    <main className="p-6 max-w-4xl mx-auto">
      <p>No data for {sym}.</p>
      <Link className="underline" href="/">Back</Link>
    </main>
  );

  const last = data.series.date.length ? data.series.date[data.series.date.length-1] : "";
  return (
    <main className="p-6 max-w-5xl mx-auto">
      <div className="flex justify-between items-center">
        <h1 className="text-xl font-bold">{data.symbol}</h1>
        <Link className="underline" href="/">Home</Link>
      </div>

      <section className="mt-6">
        <h2 className="font-semibold mb-2">Signal vs Close</h2>
        <div className="text-sm text-gray-600 mb-2">Last date: {last || "—"}</div>
        <div className="rounded border p-3 overflow-x-auto">
          {/* simple table view; you can swap to Chart.js component again */}
          <table className="min-w-[600px] text-sm">
            <thead><tr><th className="px-2 py-1 text-left">Date</th><th className="px-2 py-1 text-right">Close</th><th className="px-2 py-1 text-right">S</th><th className="px-2 py-1 text-right">News</th><th className="px-2 py-1 text-right">Earn</th></tr></thead>
            <tbody>
              {data.series.date.map((d,i)=>(
                <tr key={d}>
                  <td className="px-2 py-1">{d}</td>
                  <td className="px-2 py-1 text-right">{data.series.close[i]?.toFixed(2)}</td>
                  <td className="px-2 py-1 text-right">{data.series.S[i]?.toFixed(4)}</td>
                  <td className="px-2 py-1 text-right">{data.series.S_news[i]?.toFixed(4)}</td>
                  <td className="px-2 py-1 text-right">{data.series.S_earn[i]?.toFixed(4)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="mt-6">
        <h2 className="font-semibold mb-2">Recent headlines</h2>
        {data.recent_headlines?.length ? (
          <ul className="space-y-2">
            {data.recent_headlines.map((h,idx)=>(
              <li key={idx} className="border rounded p-2">
                <div className="text-xs text-gray-600">{new Date(h.ts).toLocaleString()}</div>
                <a href={h.url} target="_blank" rel="noreferrer" className="underline">{h.title}</a>
                {h.score && <div className="text-xs mt-1">pos: {h.score.pos.toFixed(3)} · neg: {h.score.neg.toFixed(3)}</div>}
              </li>
            ))}
          </ul>
        ) : <p>No headlines captured.</p>}
      </section>
    </main>
  );
}
