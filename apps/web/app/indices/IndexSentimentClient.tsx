"use client";

import { useMemo, useState } from "react";
import { useLanguage } from "../../components/LanguageProvider";

export type DailyRow = { date: string; sentiment_equal_weighted: number; observed_tickers: number; constituent_coverage: number };
export type IndexSeries = { code: string; name: string; weighting: string; member_count: number; companies_with_history: number; companies_with_observed_sentiment: number; daily: DailyRow[] };
type Locale = "en" | "zh";

function finite(value: unknown): number | null { const n = Number(value); return Number.isFinite(n) ? n : null; }
function tone(value: number | null) { return value == null ? "text-neutral-400" : value > 0 ? "text-emerald-300" : value < 0 ? "text-rose-300" : "text-neutral-300"; }
function mean(values: number[]) { return values.length ? values.reduce((a, b) => a + b, 0) / values.length : null; }

function SentimentChart({ rows, locale }: { rows: DailyRow[]; locale: Locale }) {
  const data = rows.filter((row) => finite(row.sentiment_equal_weighted) != null).slice(-520);
  if (!data.length) return <div className="grid h-72 place-items-center text-sm text-neutral-600">{locale === "zh" ? "暂无情绪历史。" : "No sentiment history."}</div>;
  const width = 1000, height = 330, left = 48, right = 18, top = 18, bottom = 42;
  const plotW = width - left - right, plotH = height - top - bottom;
  const x = (i: number) => left + (plotW * i) / Math.max(1, data.length - 1);
  const y = (v: number) => top + ((1 - v) / 2) * plotH;
  const points = data.map((row, i) => `${x(i).toFixed(1)},${y(row.sentiment_equal_weighted).toFixed(1)}`).join(" ");
  const dateTicks = [0, Math.floor((data.length - 1) / 2), data.length - 1];
  return <svg viewBox={`0 0 ${width} ${height}`} className="h-auto w-full" data-no-translate="true">{[-1,-0.5,0,0.5,1].map((tick) => <g key={tick}><line x1={left} x2={width-right} y1={y(tick)} y2={y(tick)} stroke="currentColor" className="text-white" strokeOpacity={tick===0?0.16:0.06}/><text x={left-10} y={y(tick)} textAnchor="end" dominantBaseline="middle" className="fill-neutral-600" fontSize="11">{tick.toFixed(1)}</text></g>)}<polyline points={points} fill="none" stroke="currentColor" className="text-emerald-400" strokeWidth="2.2" vectorEffect="non-scaling-stroke"/>{dateTicks.map((i) => <text key={i} x={x(i)} y={height-12} textAnchor={i===0?"start":i===data.length-1?"end":"middle"} className="fill-neutral-600" fontSize="11">{data[i]?.date}</text>)}</svg>;
}

export default function IndexSentimentClient({ indexes }: { indexes: IndexSeries[] }) {
  const { locale } = useLanguage();
  const t = (en: string, zh: string) => locale === "zh" ? zh : en;
  const numberFormat = useMemo(() => new Intl.NumberFormat(locale === "zh" ? "zh-CN" : "en-US"), [locale]);
  const [selected, setSelected] = useState(indexes[0]?.code ?? "SP500");
  const current = indexes.find((row) => row.code === selected) ?? indexes[0];
  const metrics = useMemo(() => {
    if (!current) return null;
    const daily = current.daily.filter((row) => finite(row.sentiment_equal_weighted) != null);
    const latest = daily.at(-1) ?? null, previous = daily.at(-2) ?? null;
    return { latest, change: latest && previous ? latest.sentiment_equal_weighted - previous.sentiment_equal_weighted : null, ma7: mean(daily.slice(-7).map((row) => row.sentiment_equal_weighted)) };
  }, [current]);
  if (!current || !metrics) return <div className="card p-5 text-neutral-500">{t("No index sentiment artifact.","暂无指数情绪数据。")}</div>;
  return <div className="space-y-6" data-no-translate="true">
    <div className="flex flex-wrap gap-2">{indexes.map((index) => <button key={index.code} type="button" onClick={() => setSelected(index.code)} className={`rounded-xl border px-3 py-2 text-xs transition ${selected===index.code?"border-emerald-400/30 bg-emerald-400/10 text-emerald-300":"border-white/10 bg-white/[0.025] text-neutral-500 hover:text-neutral-300"}`}>{index.name}</button>)}</div>
    <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
      <div className="kpi"><div className="kpi-label">{t("Latest sentiment","最新情绪")}</div><div className={`kpi-value ${tone(metrics.latest?.sentiment_equal_weighted??null)}`}>{metrics.latest?metrics.latest.sentiment_equal_weighted.toFixed(4):"—"}</div><div className="kpi-sub">{metrics.latest?.date??"—"}</div></div>
      <div className="kpi"><div className="kpi-label">{t("Change","变化")}</div><div className={`kpi-value ${tone(metrics.change)}`}>{metrics.change==null?"—":`${metrics.change>0?"+":""}${metrics.change.toFixed(4)}`}</div><div className="kpi-sub">{t("vs prior observed index day","相对上一可观测指数日")}</div></div>
      <div className="kpi"><div className="kpi-label">MA7</div><div className={`kpi-value ${tone(metrics.ma7)}`}>{metrics.ma7==null?"—":metrics.ma7.toFixed(4)}</div><div className="kpi-sub">{t("Last 7 observed index days","最近 7 个可观测指数日")}</div></div>
      <div className="kpi"><div className="kpi-label">{t("Observed companies","已观测公司")}</div><div className="kpi-value">{metrics.latest?.observed_tickers==null?"—":numberFormat.format(metrics.latest.observed_tickers)}</div><div className="kpi-sub">{t("of","共")} {numberFormat.format(current.member_count)} {t("members","家公司")}</div></div>
      <div className="kpi"><div className="kpi-label">{t("Coverage","覆盖率")}</div><div className="kpi-value">{metrics.latest?`${(metrics.latest.constituent_coverage*100).toFixed(1)}%`:"—"}</div><div className="kpi-sub">{t("Fresh observations only","仅真实观测")}</div></div>
    </section>
    <section className="ambient-panel p-4 md:p-6"><div className="mb-4"><div className="eyebrow">{t("Historical sentiment","历史情绪")}</div><h2 className="section-title mt-1">{current.name}</h2><p className="section-copy">{t("Equal-weight sentiment across constituents with fresh observed company news on each date. Missing company-days are excluded, never filled with zero.","每天对有真实新闻情绪观测的成分公司做等权聚合。没有新闻的公司日会被排除，绝不会填成 0。")}</p></div><SentimentChart rows={current.daily} locale={locale}/></section>
    <section className="card p-5 text-sm leading-6 text-neutral-400"><strong className="text-neutral-200">{t("Coverage note: ","覆盖说明：")}</strong>{t("S&P 500, MidCap 400 and SmallCap 600 use their retained constituent tiers. S&P Composite 1500 combines those tiers. Broad U.S. Equity uses the extended company layer, including additional official IWV holdings when available; it is a holdings-based broad-market proxy, not exact Russell 3000 membership.","标普 500、中盘 400 和小盘 600 使用各自成分层；Composite 1500 合并这三个层级。Broad U.S. Equity 使用扩展公司层，并在可用时加入官方 IWV 持仓；它是基于持仓的广义市场代理，并不等同于 Russell 3000 的精确成分名单。")}</section>
  </div>;
}
