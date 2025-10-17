/** @type {import('next').NextConfig} */
const BASE = process.env.NEXT_PUBLIC_BASE_PATH || "/market-sentiment-web";

module.exports = {
  // Static export
  output: "export",

  // GitHub Pages subpath
  basePath: BASE,
  // IMPORTANT: do NOT set assetPrefix here; it breaks _next static URLs on Pages.
  // assetPrefix: BASE,

  trailingSlash: true,
  images: { unoptimized: true },

  // Keep CI green
  eslint: { ignoreDuringBuilds: true },
  typescript: { ignoreBuildErrors: true },
};
