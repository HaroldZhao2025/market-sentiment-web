// apps/web/lib/paths.ts
export function assetPath(p: string): string {
  const base = (process.env.NEXT_PUBLIC_BASE_PATH || "").replace(/\/+$/, "");
  const rel = p.replace(/^\/+/, "");
  return base ? `${base}/${rel}` : `/${rel}`;
}
