// apps/web/next.config.cjs
/** @type {import('next').NextConfig} */
const NEXT_PUBLIC_BASE_PATH = process.env.NEXT_PUBLIC_BASE_PATH || "";

module.exports = {
  output: "export",
  basePath: NEXT_PUBLIC_BASE_PATH || undefined,
  images: { unoptimized: true },
  trailingSlash: true,
  experimental: {
    // keep App Router static export friendly
    manualClientBasePath: true,
  },
};
