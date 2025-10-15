/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'export',        // write static site to ./out on build
  trailingSlash: true,     // friendlier paths on GitHub Pages
  // If you later want to host under a subpath and keep absolute links,
  // set NEXT_PUBLIC_BASE_PATH in Actions → Variables and uncomment below:
  // basePath: process.env.NEXT_PUBLIC_BASE_PATH || '',
};

export default nextConfig;

