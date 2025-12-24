// apps/web/lib/research.ts
import fs from "node:fs/promises";
import fssync from "node:fs";
import path from "node:path";

export type ResearchIndexItem = {
  slug: string;
  title: string;
  summary: string;
  updated_at: string; // ISO date (YYYY-MM-DD)
  status?: "live" | "draft";
  tags?: string[];
  key_stats?: { label: string; value: string }[];

  // optional: builder may include this (safe to ignore if absent)
  highlight?: string;
};

export type ResearchStudy = ResearchIndexItem & {
  methodology?: string[];
  conclusions?: string[]; // ✅ NEW (your ResearchStudyClient uses this)

  results?: {
    sample_ticker?: string;
    n_tickers?: number;
    n_obs_panel?: number;

    series?: {
      dates?: string[];
      y_ret?: number[];
      y_ret_fwd1?: number[];
      abs_ret?: number[];
      score_mean?: number[];
      n_total?: number[];
    };

    time_series?: Record<string, unknown>;
    panel_fe?: Record<string, unknown>;
    quantiles?: Record<string, unknown>;
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
  // Try to handle both cases:
  // - build runs inside apps/web (cwd=apps/web) -> public/research is correct
  // - build runs at repo root (cwd=root) -> apps/web/public/research is correct
  const c = process.cwd();

  const candidates = [
    path.resolve(c, "public", "research"),
    path.resolve(c, "apps", "web", "public", "research"),
    path.resolve(c, "..", "public", "research"),
    path.resolve(c, "..", "..", "apps", "web", "public", "research"),
  ];

  for (const d of candidates) {
    const idx = path.join(d, "index.json");
    if (exists(idx)) return d;
  }

  // even if index.json doesn't exist yet, still return a directory if it exists
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

  const file = path.join(dir, "index.json");
  const data = await safeReadJson<ResearchIndexItem[]>(file);

  // export-safe: never throw
  return Array.isArray(data) ? data : [];
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

  // export-safe fallback
  return {
    slug,
    title: slug,
    summary: "Study JSON not found.",
    updated_at: "",
    status: "draft",
    conclusions: [`Missing file: ${path.basename(file)}.`],
  };
}
