/** @type {import('next').NextConfig} */
const isGH = process.env.NEXT_PUBLIC_BASE_PATH || "";
module.exports = {
  output: "export",
  basePath: isGH, // e.g. "/market-sentiment-web"
  assetPrefix: isGH ? `${isGH}/` : undefined,
  trailingSlash: true,
  images: { unoptimized: true },
  reactStrictMode: true,
};
