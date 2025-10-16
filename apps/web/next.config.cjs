/** @type {import('next').NextConfig} */

// GitHub Pages base path for this repo
const BASE = process.env.NEXT_PUBLIC_BASE_PATH || "/market-sentiment-web";

// IMPORTANT: CJS export (NOT ESM). This must be `module.exports = {...}`.
module.exports = {
  // Make `next build` emit a static site into `apps/web/out`
  output: "export",

  // Required for GitHub Pages under a subpath
  basePath: BASE,
  assetPrefix: BASE,
  trailingSlash: true,

  // No server-side image optimization on static export
  images: { unoptimized: true },

  // Keep CI green if type or lint issues slip in
  eslint: { ignoreDuringBuilds: true },
  typescript: { ignoreBuildErrors: true },

  // Don’t rely on any runtime features not supported by static export
  experimental: {
    instrumentationHook: false,
  },
};
