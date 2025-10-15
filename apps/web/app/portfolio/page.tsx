// apps/web/app/portfolio/page.tsx
import { loadPortfolio } from "../../lib/loaders";
import LineChart from "../../components/LineChart";

export default function PortfolioPage() {
  const p = loadPortfolio();

  const left =
    p && p.dates && p.equity
      ? p.dates.map((d, i) => ({ x: d, y: Number.isFinite(p.equity[i]) ? p.equity[i] : 0 }))
      : [];

  const right =
    p && p.dates && p.ret
      ? p.dates.map((d, i) => ({ x: d, y: Number.isFinite(p.ret[i]) ? p.ret[i] : 0 }))
      : [];

  return (
    <main className="p-6 space-y-6">
      <h2 className="text-xl font-bold">Portfolio (Top/Bottom Decile, 1d)</h2>
      {left.length === 0 ? (
        <div className="text-sm text-gray-500">No data generated yet.</div>
      ) : (
        <>
          <h3 className="font-semibold">Equity Curve</h3>
          <LineChart left={left} height={300} />
          <h3 className="font-semibold mt-6">Daily Returns</h3>
          <LineChart right={right} height={300} />
        </>
      )}
    </main>
  );
}
