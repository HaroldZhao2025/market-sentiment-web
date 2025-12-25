// apps/web/app/research/ResearchStudyClient.tsx
"use client";

import { useMemo, useState } from "react";
import Sparkline from "../../components/Sparkline";
import type { ResearchStudy } from "../../lib/research";

type ModelOut = {
  params?: Record<string, number>;
  tvalues?: Record<string, number>;
  pvalues?: Record<string, number>;
  bse?: Record<string, number>;
  nobs?: number;
  rsquared?: number;
  rsquared_adj?: number;
  cov_type?: string;
  error?: string;
};

type GenericTable = {
  title?: string;
  columns?: string[];
  rows?: any[][];
};

function star(p?: number) {
  if (p == null || !Number.isFinite(p)) return "";
  if (p < 0.01) return "***";
  if (p < 0.05) return "**";
  if (p < 0.1) return "*";
  return "";
}

function fmt(x?: number, d = 4) {
  if (x == null || !Number.isFinite(x)) return "—";
  const ax = Math.abs(x);
  if (ax !== 0 && (ax < 1e-3 || ax > 1e4)) return x.toExponential(3);
  return x.toFixed(d).replace(/0+$/, "").replace(/\.$/, "");
}

function num(x: any): number | undefined {
  const v = Number(x);
  return Number.isFinite(v) ? v : undefined;
}

function safeStr(x: any): string {
  if (x == null) return "—";
  if (typeof x === "string") return x;
  return String(x);
}

function pickSeriesArray(series: any, keys: string[]): number[] | null {
  if (!series) return null;
  for (const k of keys) {
    const v = series?.[k];
    if (Array.isArray(v) && v.length) return v as number[];
  }
  return null;
}

function buildSeriesOptions(series: any) {
  const out: { key: string; title: string; subtitle: string; data: number[] }[] = [];

  const add = (key: string, title: string, subtitle: string, data: number[] | null) => {
    if (data && data.length) out.push({ key, title, subtitle, data });
  };

  add(
    "y_ret_fwd1",
    "Next-day returns (sample)",
    "log return (t+1)",
    pickSeriesArray(series, ["y_ret_fwd1"])
  );
  add("y_ret", "Returns (sample)", "log return", pickSeriesArray(series, ["y_ret"]));
  add("abs_ret", "Volatility proxy (sample)", "|log return|", pickSeriesArray(series, ["abs_ret"]));
  add(
    "score_mean",
    "Sentiment (sample)",
    "score_mean",
    pickSeriesArray(series, ["score_mean", "S", "sentiment", "sent"])
  );
  add("n_total", "News volume (sample)", "n_total", pickSeriesArray(series, ["n_total", "news_count", "n_news"]));

  return out;
}

function RegressionTable({ title, model }: { title: string; model?: ModelOut | null }) {
  const params = model?.params ?? {};
  const keys = Object.keys(params);

  const rows = keys
    .filter((k) => k !== "const")
    .sort((a, b) => a.localeCompare(b));

  const meta = [
    model?.cov_type ? `SE: ${model.cov_type}` : null,
    model?.nobs != null ? `N: ${model.nobs}` : null,
    model?.rsquared != null ? `R²: ${fmt(model.rsquared, 4)}` : null,
  ].filter(Boolean);

  return (
    <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-3">
      <div className="flex items-baseline justify-between gap-3">
        <div className="text-lg font-semibold">{title}</div>
        <div className="text-xs text-zinc-500">{meta.join(" • ")}</div>
      </div>

      {!model ? (
        <div className="text-sm text-zinc-500">No model output available.</div>
      ) : model.error ? (
        <div className="text-sm text-zinc-500">Model error: {model.error}</div>
      ) : !rows.length ? (
        <div className="text-sm text-zinc-500">No coefficients available.</div>
      ) : (
        <div className="overflow-auto rounded-xl border border-zinc-100 bg-zinc-50">
          <table className="w-full text-sm">
            <thead className="text-xs text-zinc-500">
              <tr className="border-b border-zinc-200">
                <th className="text-left font-medium p-3">Variable</th>
                <th className="text-right font-medium p-3">Coef</th>
                <th className="text-right font-medium p-3">t</th>
                <th className="text-right font-medium p-3">p</th>
                <th className="text-right font-medium p-3">bps / 1.0</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((k) => {
                const b = model.params?.[k];
                const t = model.tvalues?.[k];
                const p = model.pvalues?.[k];
                const bps = b != null ? b * 10000 : undefined;
                return (
                  <tr key={k} className="border-b border-zinc-200 last:border-b-0">
                    <td className="p-3 font-medium text-zinc-800">{k}</td>
                    <td className="p-3 text-right">
                      {fmt(b, 6)}
                      <span className="text-zinc-500">{star(p)}</span>
                    </td>
                    <td className="p-3 text-right">{fmt(t, 3)}</td>
                    <td className="p-3 text-right">{fmt(p, 4)}</td>
                    <td className="p-3 text-right">{fmt(bps, 2)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      <div className="text-xs text-zinc-500">Stars: *** p&lt;0.01, ** p&lt;0.05, * p&lt;0.10.</div>
    </div>
  );
}

function GenericTableCard({ table }: { table: GenericTable }) {
  const cols = table.columns ?? [];
  const rows = table.rows ?? [];

  if (!cols.length || !rows.length) return null;

  return (
    <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-3">
      <div className="text-lg font-semibold">{table.title ?? "Table"}</div>

      <div className="overflow-auto rounded-xl border border-zinc-100 bg-zinc-50">
        <table className="w-full text-sm">
          <thead className="text-xs text-zinc-500">
            <tr className="border-b border-zinc-200">
              {cols.map((c, i) => (
                <th
                  key={i}
                  className={`font-medium p-3 ${i === 0 ? "text-left" : "text-right"}`}
                >
                  {c}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, ri) => (
              <tr key={ri} className="border-b border-zinc-200 last:border-b-0">
                {r.map((cell: any, ci: number) => {
                  const n = num(cell);
                  const content =
                    typeof cell === "string"
                      ? cell
                      : n != null
                      ? fmt(n, 6)
                      : safeStr(cell);
                  return (
                    <td
                      key={ci}
                      className={`p-3 ${ci === 0 ? "text-left font-medium text-zinc-800" : "text-right"}`}
                    >
                      {content}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="text-xs text-zinc-500">
        Note: table contents are exported by the Python builder; formatting is intentionally compact.
      </div>
    </div>
  );
}

const Stat = ({ label, value }: { label: string; value?: string }) => (
  <div className="rounded-xl bg-zinc-50 p-3 border border-zinc-100">
    <div className="text-xs text-zinc-500">{label}</div>
    <div className="text-sm font-semibold">{value ?? "—"}</div>
  </div>
);

const MiniCallout = ({ title, children }: { title: string; children: React.ReactNode }) => (
  <div className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
    <div className="text-sm font-semibold">{title}</div>
    <div className="text-sm text-zinc-700 leading-relaxed">{children}</div>
  </div>
);

export default function ResearchStudyClient({ study }: { study: ResearchStudy }) {
  const series = study.results?.series;

  const seriesOptions = useMemo(() => buildSeriesOptions(series), [series]);
  const defaultKey = seriesOptions[0]?.key ?? "score_mean";
  const [activeSeriesKey, setActiveSeriesKey] = useState<string>(defaultKey);

  const activeSeries = useMemo(
    () => seriesOptions.find((s) => s.key === activeSeriesKey) ?? seriesOptions[0] ?? null,
    [seriesOptions, activeSeriesKey]
  );

  const tables: GenericTable[] = useMemo(() => {
    const t: GenericTable[] = [];
    const raw = (study.results as any)?.tables;
    if (Array.isArray(raw)) {
      for (const x of raw) {
        if (x && typeof x === "object" && Array.isArray(x.columns) && Array.isArray(x.rows)) t.push(x);
      }
    }
    // also accept nested famamacbeth.table if your builder stores it there
    const fm = (study.results as any)?.famamacbeth?.table;
    if (fm && Array.isArray(fm.columns) && Array.isArray(fm.rows)) t.push(fm);

    return t;
  }, [study.results]);

  const hasSections = Array.isArray((study as any).sections) && (study as any).sections.length > 0;

  const jsonHref = useMemo(() => {
    const slug = (study as any)?.slug;
    return slug ? `/research/${slug}.json` : null;
  }, [study]);

  return (
    <div className="space-y-6">
      {/* “paper header” panel */}
      <section className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-4">
        <div className="flex flex-col md:flex-row md:items-start md:justify-between gap-4">
          <div className="space-y-1">
            <div className="text-xs text-zinc-500">Study</div>
            <div className="text-lg font-semibold text-zinc-900">{study.title}</div>
            <div className="text-sm text-zinc-600">{study.summary}</div>
            <div className="text-xs text-zinc-500">Updated: {study.updated_at ?? "—"}</div>
          </div>

          <div className="flex items-center gap-3">
            {jsonHref ? (
              <a href={jsonHref} className="text-sm underline text-zinc-700 hover:text-zinc-900">
                Download JSON
              </a>
            ) : null}
            <a
              href="/research/index.json"
              className="text-sm underline text-zinc-700 hover:text-zinc-900"
            >
              index.json
            </a>
          </div>
        </div>

        {/* quick stats */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {study.key_stats?.map((s) => (
            <Stat key={s.label} label={s.label} value={s.value} />
          )) ?? null}
          <Stat label="Sample ticker" value={study.results?.sample_ticker} />
          <Stat label="Tickers (panel)" value={study.results?.n_tickers?.toString()} />
          <Stat label="Obs (panel)" value={study.results?.n_obs_panel?.toString()} />
        </div>
      </section>

      {/* methodology + conclusions */}
      <section className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {study.methodology?.length ? (
          <div id="methodology" className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
            <h2 className="text-lg font-semibold">Methodology</h2>
            <ul className="list-disc pl-5 text-sm text-zinc-700 space-y-1">
              {study.methodology.map((m, i) => (
                <li key={i}>{m}</li>
              ))}
            </ul>
          </div>
        ) : (
          <MiniCallout title="Methodology">
            No methodology text exported yet. Consider adding a structured “Data / Specification / Inference / Caveats”
            block in the builder output.
          </MiniCallout>
        )}

        {study.conclusions?.length ? (
          <div id="conclusions" className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-2">
            <h2 className="text-lg font-semibold">Key findings</h2>
            <ul className="list-disc pl-5 text-sm text-zinc-700 space-y-1">
              {study.conclusions.map((c, i) => (
                <li key={i}>{c}</li>
              ))}
            </ul>
            <div className="text-xs text-zinc-500 pt-2 border-t border-zinc-100">
              Interpretations are descriptive; not investment advice.
            </div>
          </div>
        ) : (
          <MiniCallout title="Key findings">
            No conclusions exported yet. Exporting 2–4 bullet conclusions per study makes this page feel “paper-grade”.
          </MiniCallout>
        )}
      </section>

      {/* study sections (Data / Specification / Limitations), if exported */}
      {hasSections ? (
        <section id="sections" className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-4">
          <h2 className="text-lg font-semibold">Study sections</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {(study as any).sections.map((sec: any, i: number) => (
              <div key={i} className="rounded-2xl border border-zinc-200 bg-zinc-50 p-4 space-y-2">
                <div className="text-sm font-semibold">{sec.title ?? `Section ${i + 1}`}</div>
                {typeof sec.text === "string" && sec.text.trim().length ? (
                  <div className="text-sm text-zinc-700 whitespace-pre-wrap">{sec.text}</div>
                ) : null}
                {Array.isArray(sec.bullets) && sec.bullets.length ? (
                  <ul className="list-disc pl-5 text-sm text-zinc-700 space-y-1">
                    {sec.bullets.map((b: string, bi: number) => (
                      <li key={bi}>{b}</li>
                    ))}
                  </ul>
                ) : null}
              </div>
            ))}
          </div>
        </section>
      ) : null}

      {/* series viewer (tabbed) */}
      <section id="series" className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-4">
        <div className="flex flex-col md:flex-row md:items-baseline md:justify-between gap-3">
          <div className="space-y-1">
            <h2 className="text-lg font-semibold">Sample time-series</h2>
            <div className="text-xs text-zinc-500">
              These charts are from the sample ticker exported in the JSON (not the full panel).
            </div>
          </div>

          <div className="flex flex-wrap gap-2">
            {seriesOptions.map((s) => (
              <button
                key={s.key}
                onClick={() => setActiveSeriesKey(s.key)}
                className={[
                  "text-xs px-3 py-1.5 rounded-full border transition",
                  activeSeriesKey === s.key
                    ? "border-zinc-900 bg-zinc-900 text-white"
                    : "border-zinc-200 bg-white text-zinc-700 hover:border-zinc-300",
                ].join(" ")}
              >
                {s.title.replace(" (sample)", "")}
              </button>
            ))}
          </div>
        </div>

        <div className="rounded-2xl border border-zinc-200 bg-zinc-50 p-4 space-y-2">
          <div className="flex items-baseline justify-between">
            <div className="text-sm font-semibold text-zinc-900">{activeSeries?.title ?? "Series"}</div>
            <div className="text-xs text-zinc-500">{activeSeries?.subtitle ?? ""}</div>
          </div>

          {activeSeries?.data?.length ? (
            <div className="text-zinc-900">
              <Sparkline data={activeSeries.data} className="text-zinc-900" />
            </div>
          ) : (
            <div className="text-sm text-zinc-500">
              No series available. (If this is sentiment, ensure the builder exports <code className="px-1 py-0.5 rounded bg-zinc-100">results.series.score_mean</code>.)
            </div>
          )}
        </div>
      </section>

      {/* regression tables */}
      <section id="regressions" className="space-y-4">
        <div className="flex items-baseline justify-between">
          <h2 className="text-lg font-semibold">Regression outputs</h2>
          <div className="text-xs text-zinc-500">Compact tables; see Appendix for raw JSON</div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <RegressionTable title="Time-series (HAC)" model={study.results?.time_series as any} />
          <RegressionTable title="Panel FE (clustered)" model={study.results?.panel_fe as any} />
        </div>
      </section>

      {/* exported tables (Fama–MacBeth / DL / Placebo / etc.) */}
      {tables.length ? (
        <section id="tables" className="space-y-4">
          <div className="flex items-baseline justify-between">
            <h2 className="text-lg font-semibold">Tables</h2>
            <div className="text-xs text-zinc-500">Exported by the Python builder</div>
          </div>
          <div className="grid grid-cols-1 gap-4">
            {tables.map((t, i) => (
              <GenericTableCard key={i} table={t} />
            ))}
          </div>
        </section>
      ) : null}

      {/* reproducibility + appendix */}
      <section id="appendix" className="rounded-2xl border border-zinc-200 bg-white p-5 space-y-3">
        <h2 className="text-lg font-semibold">Appendix</h2>

        <details className="rounded-xl bg-zinc-50 border border-zinc-100 p-4">
          <summary className="cursor-pointer text-sm font-semibold text-zinc-800">Reproducibility</summary>
          <div className="text-sm text-zinc-700 mt-2">
            Studies are generated by a CLI and exported as static JSON under{" "}
            <code className="px-1 py-0.5 rounded bg-zinc-100">apps/web/public/research</code>.
          </div>
          <pre className="text-xs overflow-auto mt-3 rounded-xl bg-white border border-zinc-200 p-3">
python src/market_sentiment/cli/build_research.py --data-root data --out-dir apps/web/public/research
          </pre>
          <div className="text-xs text-zinc-500 mt-2">
            Make sure the builder runs <span className="font-semibold">before</span> Next.js export in GitHub Actions.
          </div>
        </details>

        <details className="rounded-xl bg-zinc-50 border border-zinc-100 p-4">
          <summary className="cursor-pointer text-sm font-semibold text-zinc-800">Raw exported JSON</summary>
          <pre className="text-xs overflow-auto mt-3">
{JSON.stringify(
  {
    series: study.results?.series ?? null,
    time_series: study.results?.time_series ?? null,
    panel_fe: study.results?.panel_fe ?? null,
    quantiles: (study.results as any)?.quantiles ?? null,
    tables: (study.results as any)?.tables ?? null,
    famamacbeth: (study.results as any)?.famamacbeth ?? null,
  },
  null,
  2
)}
          </pre>
        </details>
      </section>
    </div>
  );
}
