/** @type {import('next').NextConfig} */
const BASE = process.env.NEXT_PUBLIC_BASE_PATH || "/market-sentiment-web";

module.exports = {
  output: "export",                 // makes 'next export' write to 'out/'
  basePath: BASE,
  assetPrefix: BASE,
  trailingSlash: true,
  images: { unoptimized: true },
  eslint: { ignoreDuringBuilds: true },
  typescript: { ignoreBuildErrors: true }
};
