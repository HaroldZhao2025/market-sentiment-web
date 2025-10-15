// apps/web/lib/paths.ts

/**
 * Build a URL under the configured basePath (for GitHub Pages or other prefixes).
 * If NEXT_PUBLIC_BASE_PATH is empty, it falls back to root.
 */
export function assetPath(p: string): string {
  const base = (process.env.NEXT_PUBLIC_BASE_PATH || "").replace(/\/+$/, "");
  const rel = p.replace(/^\/+/, "");
  return base ? `${base}/${rel}` : `/${rel}`;
}

/**
 * Convenience helper for assets in /public/data
 */
export function dataPath(file: string): string {
  return assetPath(`data/${file}`);
}
