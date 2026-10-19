import type { NextConfig } from "next";
import path from "path";

const isStaticExport = process.env.STATIC_EXPORT === '1'

const nextConfig: NextConfig = {
  experimental: {
    optimizePackageImports: ['lucide-react', '@tanstack/react-query', '@tanstack/react-table'],
  },
  output: isStaticExport ? 'export' : 'standalone',
  trailingSlash: false,
  // Allow the Playwright suite (R-3a) to point next dev at a separate
  // dist tree so its lockfile doesn't collide with the main `next dev`
  // on port 13002. NEXT_DIST_DIR is only set by the Playwright config;
  // in every other context the default `.next/` is used.
  ...(process.env.NEXT_DIST_DIR ? { distDir: process.env.NEXT_DIST_DIR } : {}),
  ...(!isStaticExport && {
    // /api/*, /js/*, /rum-beacon proxying moved to proxy.ts -- `rewrites()`
    // is called once at `next build` and its destination is frozen into
    // the routes manifest, which doesn't work when the backend's hostname
    // is only known at deploy time (Helm). See proxy.ts's comment for the
    // full story; this was confirmed live, not theoretical.
    async headers() {
      // Default Next.js sets `Cache-Control: s-maxage=31536000` on prerendered
      // HTML, which causes Fastly to cache the anonymous SSR output for a
      // year. The HTML is auth-dependent (anonymous vs analyst vs admin) so
      // edge caching it is wrong — every visitor would get the same shell
      // and the redirect-to-/share-login would never run.
      //
      // Override with `private, no-cache` for ALL routes except hashed static
      // assets under /_next/static/* and /_next/image, which are safe to cache
      // forever (their filenames are content-hashed).
      //
      // `private` prevents Fastly/CDN from caching (the original concern).
      // `no-cache` (not `no-store`) allows the Next.js Router Cache to retain
      // prefetched RSC payloads for instant client-side navigation — `no-store`
      // caused Next.js to bypass its Router Cache entirely, forcing a fresh
      // server round-trip on every link click.
      return [
        {
          // /geo/* are static reference datasets (world.topojson is ~108KB
          // raw / ~39KB gzip, shipped once and effectively immutable for a
          // year). Browsers hit it from NetworkMap, ShieldingMap,
          // ChoroplethMap and ImpossibleDistanceModal — without caching,
          // every page load that mounts a map re-downloads the full payload.
          // 24h public cache covers the lifetime of a typical session
          // without requiring content-hashing.
          source: '/geo/:path*',
          headers: [
            { key: 'Cache-Control', value: 'public, max-age=86400, immutable' },
          ],
        },
        {
          source: '/((?!_next/static|_next/image|favicon.ico|geo/).*)',
          headers: [
            { key: 'Cache-Control', value: 'private, no-cache, must-revalidate' },
          ],
        },
      ]
    },
  }),
};

export default nextConfig;
