// apps/web/app/portfolio/page.tsx
import LineChart from "../../components/LineChart";
import { loadPortfolio } from "../../lib/loaders";
import { assetPath } from "../../lib/paths";

export default function PortfolioPage() {
  const p = loadPortfolio();

  return (
    <main className="max-w-5xl mx-auto p-6">
      <h1 className="text-xl font-semibold mb-4">Long/Short Portfolio</h1>

      {!p ? (
        <p className="text-sm text-gray-500">No portfolio data generated yet.</p>
      ) : (
        <>
          <div className="mb-6">
            <LineChart
              left={p.dates.map((d, i) => ({ x: d, y: p.equity[i] ?? 0 }))}
              height={300}
            />
          </div>
          {p.stats ? (
            <div className="text-sm text-gray-700">
              <div>Ann. Return: {p.stats.ann_return?.toFixed(2)}</div>
              <div>Ann. Vol: {p.stats.ann_vol?.toFixed(2)}</div>
              <div>Sharpe: {p.stats.sharpe?.toFixed(2)}</div>
              <div>Max Drawdown: {p.stats.max_dd?.toFixed(2)}</div>
            </div>
          ) : null}
        </>
      )}

      <div className="mt-8">
        <a className="underline text-blue-600" href={assetPath("")}>
          ← Back
        </a>
      </div>
    </main>
  );
}
