// apps/web/lib/research.ts
import fs from "node:fs/promises";
import fssync from "node:fs";
import path from "node:path";

export type ResearchIndexItem = {
  slug: string;
  title: string;
  summary: string;
  updated_at: string;
  status?: "live" | "draft";
  tags?: string[];
  key_stats?: { label: string; value: string }[];
  highlight?: string;
  category?: string;
};

export type ResearchOverviewSection = {
  id: string;
  title: string;
  description?: string;
  conclusions?: string[];
  slugs: string[];
};

export type ResearchOverviewMeta = {
  updated_at?: string;
  n_studies?: number;
  n_tickers?: number;
  n_obs_panel?: number;
  date_range?: [string, string];
};

export type ResearchOverview = {
  meta?: ResearchOverviewMeta;
  sections: ResearchOverviewSection[];
};

export type ResearchStudy = ResearchIndexItem & {
  methodology?: string[];
  sections?: { title: string; bullets?: string[]; text?: string }[];
  conclusions?: string[];

  results?: {
    sample_ticker?: string;
    n_tickers?: number;
    n_obs_panel?: number;

    series?: Record<string, any>;

    time_series?: Record<string, unknown>;
    panel_fe?: Record<string, unknown>;
    quantiles?: Record<string, unknown>;

    famamacbeth?: Record<string, unknown>;
    tables?: Array<{
      title: string;
      columns: string[];
      rows: any[][];
    }>;

    [k: string]: unknown;
  };

  notes?: string[];
};

function exists(p: string) {
  try {
    return fssync.existsSync(p);
  } catch {
    return false;
  }
}

function findResearchDir(): string | null {
  const c = process.cwd();
  const candidates = [
    path.resolve(c, "public", "research"),
    path.resolve(c, "apps", "web", "public", "research"),
    path.resolve(c, "..", "public", "research"),
    path.resolve(c, "..", "..", "apps", "web", "public", "research"),
  ];

  for (const d of candidates) {
    if (exists(path.join(d, "index.json"))) return d;
  }
  for (const d of candidates) {
    if (exists(d)) return d;
  }
  return null;
}

async function safeReadJson<T>(absPath: string): Promise<T | null> {
  try {
    const raw = await fs.readFile(absPath, "utf-8");
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

export async function loadResearchIndex(): Promise<ResearchIndexItem[]> {
  const dir = findResearchDir();
  if (!dir) return [];
  const data = await safeReadJson<ResearchIndexItem[]>(path.join(dir, "index.json"));
  return Array.isArray(data) ? data : [];
}

export async function loadResearchOverviewFull(): Promise<ResearchOverview> {
  const dir = findResearchDir();
  if (!dir) return { sections: [] };

  const data = await safeReadJson<ResearchOverview>(path.join(dir, "overview.json"));
  const sections = Array.isArray(data?.sections) ? data!.sections : [];
  const meta = data?.meta ?? undefined;

  return { meta, sections };
}

export async function loadResearchStudy(slug: string): Promise<ResearchStudy> {
  const dir = findResearchDir();
  if (!dir) {
    return {
      slug,
      title: slug,
      summary: "Research artifacts not found (research build may not have run yet).",
      updated_at: "",
      status: "draft",
      conclusions: ["Research JSON not available yet. Please run the research build step."],
    };
  }

  const file = path.join(dir, `${slug}.json`);
  const data = await safeReadJson<ResearchStudy>(file);

  if (data && typeof data === "object") return data;

  return {
    slug,
    title: slug,
    summary: "Study JSON not found.",
    updated_at: "",
    status: "draft",
    conclusions: [`Missing file: ${path.basename(file)}.`],
  };
}
