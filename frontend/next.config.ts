import type { NextConfig } from "next";
import path from "path";

const isStaticExport = process.env.STATIC_EXPORT === '1'
const apiProxyUrl = process.env.API_PROXY_URL || 'http://127.0.0.1:8000'

const nextConfig: NextConfig = {
  output: isStaticExport ? 'export' : 'standalone',
  trailingSlash: false,
  turbopack: {
    root: path.join(__dirname, '../'),
  },
  ...(!isStaticExport && {
    async rewrites() {
      return [
        {
          source: '/api/:path*',
          destination: `${apiProxyUrl}/api/:path*`,
        },
      ]
    },
    async headers() {
      // Default Next.js sets `Cache-Control: s-maxage=31536000` on prerendered
      // HTML, which causes Fastly to cache the anonymous SSR output for a
      // year. The HTML is auth-dependent (anonymous vs analyst vs admin) so
      // edge caching it is wrong — every visitor would get the same shell
      // and the redirect-to-/share-login would never run.
      //
      // Override to no-store for ALL routes except hashed static assets under
      // /_next/static/* and /_next/image, which are safe to cache forever
      // (their filenames are content-hashed).
      return [
        {
          source: '/((?!_next/static|_next/image|favicon.ico).*)',
          headers: [
            { key: 'Cache-Control', value: 'private, no-store, must-revalidate' },
          ],
        },
      ]
    },
  }),
};

export default nextConfig;
