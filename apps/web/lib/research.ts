// apps/web/lib/research.ts
import fs from "node:fs/promises";
import path from "node:path";

export type ResearchIndexItem = {
  slug: string;
  title: string;
  summary: string;
  updated_at: string;
  status?: string;
  tags?: string[];
  key_stats?: { label: string; value: string }[];
  highlight?: string;
  category?: string;
};

export type ResearchOverview = {
  meta?: Record<string, any>;
  sections: ResearchSection[];
};

export type ResearchSection = {
  id: string;
  title: string;
  description?: string;
  conclusions?: string[];
  slugs: string[];
};

export type ResearchTable = {
  title: string;
  columns: string[];
  rows: any[][];
};

export type ResearchStudy = {
  slug: string;
  title: string;
  category?: string;
  summary: string;
  updated_at: string;
  status?: string;
  tags?: string[];
  key_stats?: { label: string; value: string }[];
  highlight?: string;
  methodology?: string[];
  sections?: { title: string; bullets?: string[] }[];
  conclusions?: string[];
  results?: any;
  notes?: string[];
};

async function exists(p: string) {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}

async function resolveResearchDir(): Promise<string> {
  // robust for monorepo vs apps/web cwd
  const cands = [
    path.join(process.cwd(), "apps", "web", "public", "research"),
    path.join(process.cwd(), "public", "research"),
  ];
  for (const p of cands) {
    if (await exists(p)) return p;
  }
  // default to first (will throw later on read)
  return cands[0];
}

async function readJsonFile<T>(absPath: string, fallback: T): Promise<T> {
  try {
    const raw = await fs.readFile(absPath, "utf-8");
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

export async function loadResearchIndex(): Promise<ResearchIndexItem[]> {
  const dir = await resolveResearchDir();
  const p = path.join(dir, "index.json");
  return readJsonFile<ResearchIndexItem[]>(p, []);
}

export async function loadResearchOverview(): Promise<ResearchOverview> {
  const dir = await resolveResearchDir();
  const p = path.join(dir, "overview.json");
  return readJsonFile<ResearchOverview>(p, { sections: [] });
}

/**
 * Backward-compatible helper: returns only sections array (your page currently expects this).
 */
export async function loadResearchOverviewFull(): Promise<ResearchSection[]> {
  const ov = await loadResearchOverview();
  return (ov?.sections ?? []) as ResearchSection[];
}

export async function loadResearchStudy(slug: string): Promise<ResearchStudy> {
  const dir = await resolveResearchDir();
  const p = path.join(dir, `${slug}.json`);
  const fallback: ResearchStudy = {
    slug,
    title: slug,
    summary: "Missing research artifact. The builder may not have generated this study for the deployment.",
    updated_at: "",
    status: "draft",
    results: {},
  };
  return readJsonFile<ResearchStudy>(p, fallback);
}
