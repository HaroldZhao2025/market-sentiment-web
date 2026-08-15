"use client";

import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from "react";

type Locale = "en" | "zh";
type LanguageContextValue = { locale: Locale; setLocale: (locale: Locale) => void };

const STORAGE_KEY = "sentiment-intelligence-locale";
const LanguageContext = createContext<LanguageContextValue>({ locale: "en", setLocale: () => undefined });

const ZH: Record<string, string> = {
  "Market": "市场",
  "Companies": "公司",
  "S&P 500": "标普 500",
  "Research Lab": "研究实验室",
  "Screener": "筛选器",
  "Events": "事件",
  "Attribution": "归因",
  "Portfolio": "投资组合",
  "Research": "研究",
  "Agent": "代理接口",
  "Data": "数据",
  "Methodology": "方法论",
  "More": "更多",
  "Intelligence": "市场智能",
  "Research & platform": "研究与平台",
  "Ask": "问市场",
  "Explore": "浏览",
  "Ask the Market": "市场问答",
  "Agent Interface": "代理接口",
  "Machine-readable data": "机器可读数据",
  "Not investment advice": "不构成投资建议",
  "Market evidence, not market noise": "市场证据，而非市场噪音",
  "Sentiment Intelligence · U.S. company evidence and S&P 500 research platform": "Sentiment Intelligence · 美国公司证据与标普 500 研究平台",

  "Live market state": "实时市场状态",
  "What changed in the market?": "市场发生了什么变化？",
  "News sentiment, price reaction, coverage, and empirical signals in one auditable S&P 500 intelligence layer.": "在一个可审计的标普 500 智能层中，同时查看新闻情绪、价格反应、覆盖率与实证信号。",
  "Current regime": "当前市场状态",
  "Insufficient signal": "信号不足",
  "Waiting for observed market sentiment.": "等待可观测的市场情绪数据。",
  "Broadly constructive": "整体偏积极",
  "Positive market sentiment is holding or improving.": "市场正面情绪保持稳定或继续改善。",
  "Positive, cooling": "仍偏积极，但在降温",
  "Sentiment remains positive but momentum has weakened.": "情绪仍为正，但动量已经减弱。",
  "Broadly defensive": "整体偏防御",
  "Negative sentiment is persistent or deteriorating.": "负面情绪持续存在或进一步恶化。",
  "Negative, recovering": "仍偏负面，但在修复",
  "Sentiment remains below zero but is improving.": "情绪仍低于零，但正在改善。",
  "Mixed / transitional": "混合 / 过渡状态",
  "The aggregate signal is close to neutral or changing direction.": "综合信号接近中性，或正在改变方向。",
  "S&P 500 close": "标普 500 收盘价",
  "Cap-weighted sentiment": "市值加权情绪",
  "7D sentiment average": "7 日情绪均值",
  "Smooths daily news noise": "平滑每日新闻噪音",
  "Signal coverage": "信号覆盖率",
  "Observed tickers —": "已观测公司 —",
  "Unique news —": "独立新闻 —",
  "How this is calculated": "计算方法",
  "Why it matters": "为什么重要",
  "Evidence before narrative.": "先看证据，再讲故事。",
  "Large-language models can summarize news. This project focuses on what they cannot reliably invent: deduplicated observations, market-cap attribution, price divergence, reproducible backtests, and source-level scores.": "大语言模型可以总结新闻；本项目更关注不能凭空生成的可验证证据：去重后的观测、市值归因、价格背离、可复现回测以及来源级评分。",
  "Observed signal": "可观测信号",
  "No-news is treated as missing, not neutral zero.": "没有新闻代表缺失，而不是中性零值。",
  "Market context": "市场背景",
  "Sentiment is read beside price reaction and coverage.": "情绪需要与价格反应和覆盖率一起解读。",
  "Researchable": "可研究",
  "The same data layer feeds portfolio and empirical studies.": "同一数据层同时服务于投资组合和实证研究。",
  "Attention map": "关注度地图",
  "Where the signal moved": "哪些地方的信号发生了变化",
  "Daily sentiment change tells you where the information environment shifted, while divergence highlights where news and price disagree.": "每日情绪变化反映信息环境在哪里发生了转移，而背离则突出新闻与价格走势不一致的公司。",
  "Improving sentiment": "情绪改善",
  "Deteriorating sentiment": "情绪恶化",
  "News / price divergence": "新闻 / 价格背离",
  "Explore the universe": "浏览公司范围",
  "S&P 500 signal explorer": "标普 500 信号浏览器",
  "Search and screen the universe instead of reading a static ticker list.": "通过搜索与筛选探索公司，而不是阅读静态股票列表。",
  "Open heatmap →": "打开热力图 →",
  "Strategy layer": "策略层",
  "Track how the signal behaves after execution lag, sizing, turnover, and transaction costs.": "跟踪信号在执行滞后、仓位规模、换手率与交易成本之后的表现。",
  "Equity": "净值",
  "Sharpe": "夏普比率",
  "No current holdings snapshot": "暂无当前持仓快照",
  "Evidence layer": "证据层",
  "Inspect empirical relationships, forward returns, robustness, and signal diagnostics generated from the live dataset.": "查看由实时数据生成的实证关系、未来收益、稳健性与信号诊断。",
  "Open research →": "打开研究 →",
  "Agent-ready layer": "代理可用层",
  "Discover stable JSON endpoints for tickers, the index, portfolio results, and research artifacts.": "查看股票、指数、投资组合结果与研究产物的稳定 JSON 数据端点。",
  "Explore data contract →": "查看数据契约 →",
  "No qualifying observations in the latest snapshot.": "最新快照中没有符合条件的观测。",

  "U.S. company universe": "美国公司范围",
  "Search large, mid and small-cap names in one market view. S&P 500 index calculations remain separate.": "在同一个市场视图中搜索大盘、中盘和小盘公司；标普 500 指数计算仍保持独立。",
  "Complete earnings calls": "完整财报电话会",
  "Structured call analytics available": "已提供结构化电话会分析",
  "Call coverage": "电话会覆盖率",
  "Of the current company universe": "占当前公司范围的比例",
  "Search company or ticker": "搜索公司或股票代码",
  "All": "全部",
  "Complete": "完整",
  "Partial": "部分",
  "Link only": "仅链接",
  "Searching": "搜索中",
  "Alphabetical": "按字母排序",
  "1D return": "1 日收益",
  "Sentiment": "情绪",
  "News attention": "新闻关注度",
  "Table": "表格",
  "Cards": "卡片",
  "Company": "公司",
  "Universe": "范围",
  "Sector": "行业板块",
  "Price": "价格",
  "News": "新闻",
  "Earnings call": "财报电话会",
  "Call ready": "电话会可用",
  "Partial call": "部分电话会",
  "Transcript link": "文字稿链接",
  "Company data is refreshing.": "公司数据正在刷新。",
  "The core S&P 500 pages remain available while the broader universe is rebuilt.": "在更广泛公司数据重建期间，核心标普 500 页面仍可正常使用。",
  "Latest extended snapshot": "最新扩展快照",

  "Ticker intelligence": "个股智能",
  "Price reaction, observed news sentiment, article-level evidence, and deterministic event themes.": "价格反应、可观测新闻情绪、文章级证据与确定性事件主题。",
  "Overlay": "叠加",
  "Separate": "分开",
  "Latest sentiment": "最新情绪",
  "Sentiment change": "情绪变化",
  "Latest observation vs previous observation": "最新观测相对上一观测",
  "Smoothed signal, not a return forecast": "平滑信号，并非收益预测",
  "1D price return": "1 日价格收益",
  "Why sentiment changed": "情绪为何变化",
  "Evidence behind the current signal": "当前信号背后的证据",
  "Article scores are grouped with deterministic keyword rules. This is an auditable explanation layer, not an LLM-generated narrative.": "文章评分按照确定性关键词规则分组。这是可审计的解释层，而不是由大模型生成的叙述。",
  "Recent article mean": "近期文章均值",
  "Dominant event theme": "主要事件主题",
  "No scored theme": "暂无已评分主题",
  "Positive evidence": "正面证据",
  "Strongest recent positive drivers shown below": "下方展示近期最强的正面驱动因素",
  "News evidence": "新闻证据",
  "Positive drivers": "正面驱动",
  "Negative drivers": "负面驱动",
  "Positive driver": "正面驱动",
  "Negative driver": "负面驱动",
  "Signal history": "信号历史",
  "Price and sentiment": "价格与情绪",
  "Article evidence": "文章证据",
  "Recent scored headlines": "近期已评分新闻标题",
  "Each score is the article-level FinBERT probability difference P(positive) − P(negative).": "每个评分都是文章级 FinBERT 概率差：P(正面) − P(负面)。",
  "Date": "日期",
  "Headline": "标题",
  "Theme": "主题",
  "Source": "来源",
  "No observation": "暂无观测",
  "Strong Positive": "强正面",
  "Positive": "正面",
  "Strong Negative": "强负面",
  "Negative": "负面",
  "Neutral": "中性",
  "Earnings & guidance": "财报与指引",
  "Product & AI": "产品与 AI",
  "Regulation & legal": "监管与法律",
  "Deals & capital": "交易与资本",
  "Operations & demand": "运营与需求",
  "Analyst & market": "分析师与市场",
  "Other company news": "其他公司新闻",

  "Contribution": "贡献",
  "Return": "收益",
  "Weight": "权重",
  "Coverage": "覆盖率",
  "Industry": "行业",
  "Change": "变化",
  "Current": "当前",
  "Open": "打开",
  "Back": "返回",
  "Next": "下一页",
  "Previous": "上一页",
};

const textOriginal = new WeakMap<Text, string>();
const attributeOriginal = new WeakMap<Element, Map<string, string>>();

function translateDynamic(core: string): string | null {
  let match = core.match(/^(\d+) observed tickers$/);
  if (match) return `${match[1]} 家已观测公司`;
  match = core.match(/^(\d+) unique news items$/);
  if (match) return `${match[1]} 条独立新闻`;
  match = core.match(/^As of (.+)$/);
  if (match) return `截至 ${match[1]}`;
  match = core.match(/^(\d+) companies · page (\d+) of (\d+)$/);
  if (match) return `${match[1]} 家公司 · 第 ${match[2]} / ${match[3]} 页`;
  match = core.match(/^Call coverage (.+) UTC$/);
  if (match) return `电话会覆盖数据 ${match[1]} UTC`;
  match = core.match(/^Updated (.+) UTC$/);
  if (match) return `更新于 ${match[1]} UTC`;
  match = core.match(/^(\d+) scored headlines displayed$/);
  if (match) return `当前展示 ${match[1]} 条已评分标题`;
  match = core.match(/^(\d+) long \/ (\d+) short$/);
  if (match) return `${match[1]} 多头 / ${match[2]} 空头`;
  match = core.match(/^Period total · (\d+) recent rows scored$/);
  if (match) return `期间总计 · ${match[1]} 条近期记录已评分`;
  match = core.match(/^(\d+) scored headline(s?)$/);
  if (match) return `${match[1]} 条已评分标题`;
  return null;
}

function translateText(raw: string): string {
  if (!raw.trim()) return raw;
  const leading = raw.match(/^\s*/)?.[0] ?? "";
  const trailing = raw.match(/\s*$/)?.[0] ?? "";
  const core = raw.trim().replace(/\s+/g, " ");
  const translated = ZH[core] ?? translateDynamic(core);
  return translated ? `${leading}${translated}${trailing}` : raw;
}

function shouldSkip(text: Text): boolean {
  const parent = text.parentElement;
  if (!parent) return true;
  if (parent.closest("[data-no-translate='true']")) return true;
  return ["SCRIPT", "STYLE", "CODE", "PRE", "TEXTAREA"].includes(parent.tagName);
}

function translateAttributes(root: ParentNode, locale: Locale) {
  const elements = root.querySelectorAll?.("[placeholder], [title], [aria-label]") ?? [];
  elements.forEach((element) => {
    if (element.closest("[data-no-translate='true']")) return;
    const originals = attributeOriginal.get(element) ?? new Map<string, string>();
    ["placeholder", "title", "aria-label"].forEach((name) => {
      const current = element.getAttribute(name);
      if (current == null) return;
      if (!originals.has(name)) originals.set(name, current);
      const original = originals.get(name) ?? current;
      element.setAttribute(name, locale === "zh" ? translateText(original) : original);
    });
    attributeOriginal.set(element, originals);
  });
}

function translateTree(root: ParentNode, locale: Locale) {
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
  let node = walker.nextNode();
  while (node) {
    const text = node as Text;
    if (!shouldSkip(text)) {
      const current = text.nodeValue ?? "";
      const known = textOriginal.get(text);
      if (known == null) {
        textOriginal.set(text, current);
      } else if (locale === "zh" && current !== translateText(known)) {
        textOriginal.set(text, current);
      }
      const original = textOriginal.get(text) ?? current;
      const next = locale === "zh" ? translateText(original) : original;
      if (current !== next) text.nodeValue = next;
    }
    node = walker.nextNode();
  }
  translateAttributes(root, locale);
}

export function LanguageProvider({ children }: { children: ReactNode }) {
  const [locale, setLocale] = useState<Locale>("en");

  useEffect(() => {
    const stored = window.localStorage.getItem(STORAGE_KEY);
    if (stored === "zh" || stored === "en") setLocale(stored);
  }, []);

  useEffect(() => {
    window.localStorage.setItem(STORAGE_KEY, locale);
    document.documentElement.lang = locale === "zh" ? "zh-CN" : "en";

    let frame = 0;
    let observer: MutationObserver;
    const apply = () => {
      cancelAnimationFrame(frame);
      frame = requestAnimationFrame(() => {
        observer.disconnect();
        translateTree(document.body, locale);
        observer.observe(document.body, { subtree: true, childList: true, characterData: true, attributes: true, attributeFilter: ["placeholder", "title", "aria-label"] });
      });
    };

    observer = new MutationObserver(() => apply());
    apply();
    return () => {
      cancelAnimationFrame(frame);
      observer.disconnect();
    };
  }, [locale]);

  const value = useMemo(() => ({ locale, setLocale }), [locale]);
  return <LanguageContext.Provider value={value}>{children}</LanguageContext.Provider>;
}

export function useLanguage() {
  return useContext(LanguageContext);
}

export function LanguageToggle() {
  const { locale, setLocale } = useLanguage();
  return (
    <div data-no-translate="true" className="flex items-center rounded-xl border border-white/10 bg-white/[0.03] p-1 text-[11px] font-semibold">
      <button
        type="button"
        onClick={() => setLocale("en")}
        aria-pressed={locale === "en"}
        className={`rounded-lg px-2.5 py-1.5 transition ${locale === "en" ? "bg-white/10 text-white" : "text-neutral-500 hover:text-neutral-300"}`}
      >
        EN
      </button>
      <button
        type="button"
        onClick={() => setLocale("zh")}
        aria-pressed={locale === "zh"}
        className={`rounded-lg px-2.5 py-1.5 transition ${locale === "zh" ? "bg-white/10 text-white" : "text-neutral-500 hover:text-neutral-300"}`}
      >
        中文
      </button>
    </div>
  );
}
