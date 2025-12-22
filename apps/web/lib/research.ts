// apps/web/lib/research.ts
import fs from "node:fs/promises";
import path from "node:path";

export type ResearchIndexItem = {
  slug: string;
  title: string;
  summary: string;
  updated_at: string; // ISO date
  status?: "live" | "draft";
  tags?: string[];
  key_stats?: { label: string; value: string }[];
};

export type ResearchStudy = ResearchIndexItem & {
  methodology?: string[];
  results?: {
    sample_ticker?: string;
    n_tickers?: number;
    n_obs_panel?: number;

    // series for charts
    series?: {
      dates: string[];
      y_ret?: number[];
      y_ret_fwd1?: number[];
      abs_ret?: number[];
      score_mean?: number[];
      n_total?: number[];
    };

    // regressions
    time_series?: Record<string, any>;
    panel_fe?: Record<string, any>;
    quantiles?: Record<string, any>;
  };
  notes?: string[];
};

const RESEARCH_DIR = path.join(process.cwd(), "public", "research");

async function readJson<T>(filePath: string): Promise<T> {
  const raw = await fs.readFile(filePath, "utf-8");
  return JSON.parse(raw) as T;
}

export async function loadResearchIndex(): Promise<ResearchIndexItem[]> {
  const file = path.join(RESEARCH_DIR, "index.json");
  return readJson<ResearchIndexItem[]>(file);
}

export async function loadResearchStudy(slug: string): Promise<ResearchStudy> {
  const file = path.join(RESEARCH_DIR, `${slug}.json`);
  return readJson<ResearchStudy>(file);
}
