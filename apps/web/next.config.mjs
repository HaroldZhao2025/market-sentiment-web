/** @type {import('next').NextConfig} */
const basePath = process.env.NEXT_PUBLIC_BASE_PATH || ""; // set in CI for GH Pages

const nextConfig = {
  output: "export",       // writes to ./out
  trailingSlash: true,    // needed for static hosting
  basePath,               // e.g. "/market-sentiment-web" on Pages, "" locally
};

export default nextConfig;
