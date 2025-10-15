// apps/web/lib/paths.ts
export function assetPath(p: string): string {
  const base = (process.env.NEXT_PUBLIC_BASE_PATH || "").replace(/\/+$/, "");
  const rel = p.replace(/^\/+/, "");
  return base ? `${base}/${rel}` : `/${rel}`;
}

export function dataPath(file: string): string {
  // Static export serves from /public; at runtime the JSONs are relative to basePath.
  // We always point into /public/data via basePath.
  return assetPath(`data/${file.replace(/^\/+/, "")}`);
}
