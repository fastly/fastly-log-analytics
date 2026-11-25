import type { Metadata } from "next";
import { Suspense } from "react";
import { Inter } from "next/font/google";
import { cookies, headers } from "next/headers";
import "./globals.css";
import QueryProvider, { ReloadLoopGuard, StoreHydrator, WebVitalsReporter } from "@/components/QueryProvider";
import { BootstrapHydration } from "@/components/BootstrapHydration";
import { ThemeProvider } from "next-themes";
import { AppLayout } from "@/components/AppLayout";
import { ServerFooter } from "@/components/ServerFooter";
import { SIDEBAR_COLLAPSED_COOKIE } from "@/lib/utils";
import { ErrorBoundaryWithRouteReset } from "@/components/AnalyticsCard";

const inter = Inter({ subsets: ["latin"], display: "swap", preload: false });

export const metadata: Metadata = {
  title: "Fastly Log Analytics",
  description: "Modern log analytics, powered by Fastly Object Storage",
};

// force-dynamic is REQUIRED for the per-request SSR fetch of
// /api/bootstrap below. Without it Next.js would statically generate
// the layout at build time (when the backend isn't reachable) and
// the dehydrated state would be permanently empty. Earlier comments
// here documented the removal of force-dynamic for the modulepreload
// optimization — that trade-off is reversed now that the layout has
// real per-request work to do.
export const dynamic = "force-dynamic";

export default async function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  // P-2 (audit): the unconditional <link rel="modulepreload"> for the
  // 1.4MB Plotly chunk was emitted on every route — including
  // /share-login (unauthenticated) and /admin/* (no charts). It used
  // high-priority browser scheduling and wasted ~150KB gzip of
  // bandwidth + paint contention on those pages. The pathname-gated
  // <PlotlyPrewarm> / <MapPrewarm> in AppLayout (see needsPlotlyPrewarm
  // / needsMapPrewarm) already trigger the dynamic import on the
  // chart-bearing routes that actually render Plotly / MapLibre, so
  // removing the layout-level preload is a net win: non-chart pages
  // stop wasting bandwidth, chart pages still fetch the chunk via the
  // prewarmer (slight ~100-200ms later than the preload-hot path, but
  // still well before the user-visible chart render).

  // Read the sidebar-collapsed cookie server-side so SSR paints the
  // correct width on first render. Without this, the client useState
  // initializer (which reads the cookie in the browser) would flip the
  // sidebar from expanded → collapsed during hydration, producing a
  // visible flash on every page load for users with a collapsed pref.
  const isStaticExport = process.env.STATIC_EXPORT === "1";
  const initialSidebarCollapsed = isStaticExport
    ? false
    : (await cookies()).get(SIDEBAR_COLLAPSED_COOKIE)?.value === "1";

  // Per-request CSP nonce, set by proxy.ts. Next.js auto-applies it to
  // its own injected <script>/<link> tags; we have to pass it manually to
  // next-themes (its theme-bootstrap inline script doesn't read the
  // header) so the strict script-src nonce policy doesn't drop it.
  const requestHeaders = isStaticExport ? new Headers() : await headers();
  const nonce = requestHeaders.get("x-nonce") ?? undefined;
  const host = requestHeaders.get("host");
  const proxiedByCaddy = requestHeaders.get("x-proxied-by-caddy") === "true";

  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        {/* world.topo.json preload + plotly modulepreload both moved out of
            this root layout. world.topo.json lives in <AppLayout> (emits
            only on map-using routes /dashboard, /network); plotly chunk
            loads on-demand via <PlotlyPrewarm> on chart-bearing routes.
            Removing the always-on global emits saves ~400KB raw / ~150KB
            gzip of bandwidth on every non-chart page (/share-login,
            /admin/*, /alerts, /usage, /logs). See P-1 / P-2 in the
            2026-06-15 audit. */}
      </head>
      <body className={`${inter.className} antialiased`} suppressHydrationWarning>
        {/* Skip-to-content link: first focusable element, visually hidden
            until keyboard-focused. Without it, keyboard users have to tab
            through the entire sidebar nav on every page load before
            reaching the page body. */}
        <a
          href="#main"
          className="sr-only focus:not-sr-only focus:fixed focus:top-2 focus:left-2 focus:z-50 focus:rounded focus:bg-background focus:px-3 focus:py-2 focus:text-sm focus:font-medium focus:shadow focus:ring-2 focus:ring-primary"
        >
          Skip to main content
        </a>
        <ThemeProvider
          attribute="class"
          defaultTheme="system"
          enableSystem
          disableTransitionOnChange
          nonce={nonce}
        >
          {/* Reload-loop breaker: if the SAME path hard-loads too many times in
              a short window (a stale-tab post-deploy reload loop), render a
              recovery prompt instead of the app subtree below — not mounting the
              app is what stops the loop from re-arming. Wraps QueryProvider so a
              tripped state also halts the WebVitals POSTs that flooded RUM. */}
          <ReloadLoopGuard>
          <QueryProvider>
            <Suspense fallback={null}>
              <BootstrapHydration host={host} proxiedByCaddy={proxiedByCaddy} nonce={nonce} />
            </Suspense>
            {/* Rehydrate the persisted Zustand stores (service / timezone /
                debug) AFTER mount. They use persist({ skipHydration:true })
                so the first client render matches the server's default
                state — without this the synchronous localStorage read at
                module load diverges from SSR and throws React #418 across
                the sidebar nav + services table. */}
            <StoreHydrator />
            {/* WebVitalsReporter renders nothing; it just registers the
                useReportWebVitals callback that POSTs LCP/INP/CLS/FCP/
                TTFB to /api/web-vitals as they fire. Mounting inside
                QueryProvider keeps it under the same React tree as the
                rest of the app so the existing fetch wrapper / origin
                handling Just Works. */}
            <WebVitalsReporter />
            <AppLayout
              initialCollapsed={initialSidebarCollapsed}
              serverFooter={
                <Suspense fallback={<div aria-hidden="true" className="mt-12 h-[50px]" />}>
                  <ServerFooter />
                </Suspense>
              }
            >
              <ErrorBoundaryWithRouteReset>{children}</ErrorBoundaryWithRouteReset>
            </AppLayout>
          </QueryProvider>
          </ReloadLoopGuard>
        </ThemeProvider>
      </body>
    </html>
  );
}
