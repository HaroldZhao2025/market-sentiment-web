/** @type {import('next').NextConfig} */
const BASE = process.env.NEXT_PUBLIC_BASE_PATH || "/market-sentiment-web";

module.exports = {
  output: "export",
  basePath: BASE,
  // DO NOT set assetPrefix here; breaks _next URLs on Pages
  trailingSlash: true,
  images: { unoptimized: true },
  eslint: { ignoreDuringBuilds: true },
  typescript: { ignoreBuildErrors: true }
};
