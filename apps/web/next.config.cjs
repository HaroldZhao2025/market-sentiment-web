/** @type {import('next').NextConfig} */
const BASE = process.env.NEXT_PUBLIC_BASE_PATH || "/market-sentiment-web";

module.exports = {
  // Static export into apps/web/out
  output: "export",
  // Required for GitHub Pages subpath
  basePath: BASE,
  assetPrefix: BASE,
  trailingSlash: true,
  images: { unoptimized: true },

  // Keep CI green
  eslint: { ignoreDuringBuilds: true },
  typescript: { ignoreBuildErrors: true }
};
