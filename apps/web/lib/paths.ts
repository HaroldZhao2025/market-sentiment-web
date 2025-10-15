// apps/web/lib/paths.ts
export function assetPath(p: string): string {
  const base = (process.env.NEXT_PUBLIC_BASE_PATH ?? "").replace(/\/+$/, "");
  const rel = p.replace(/^\/+/, "");
  // Always return a leading slash for local dev; prepend basePath for GH Pages
  return base ? `${base}/${rel}` : `/${rel}`;
}
