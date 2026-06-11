import type { Metadata } from "next";
import { Inter } from "next/font/google";
import { QueryClient, dehydrate } from "@tanstack/react-query";
import type { DehydratedState } from "@tanstack/react-query";
import "./globals.css";
import QueryProvider from "@/components/QueryProvider";
import ThemeProvider from "@/components/ThemeProvider";
import { AppLayout } from "@/components/AppLayout";
import { TooltipProvider } from "@/components/ui/tooltip";
import { ErrorBoundary } from "@/components/ErrorBoundary";
import { getPreloadChunks } from "@/lib/preload-manifest";
import { fetchBootstrapServerSide } from "@/lib/ssr/bootstrap";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Fastly Log Analytics",
  description: "Modern log analytics for Fastly Object Storage",
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
  // Modulepreload links from the build-time manifest (returns [] at
  // SSG-time since the manifest is generated AFTER next build).
  const preloadChunks = getPreloadChunks();

  // Per-request SSR fetch of /api/bootstrap. Pre-seeds React Query so
  // useBootstrap (and every hook that reads bootstrap.* via
  // queryClient.getQueryData) finds data already cached on first
  // render — the share banner, header badge, etc. land in the
  // initial HTML paint instead of after the client-side fetch.
  //
  // SECURITY: the SSR helper sets X-Remote-Analyst:1 whenever the
  // inbound request carries the X-Proxied-By-Caddy marker, so the
  // backend correctly scopes the response to the analyst session
  // (or returns the anonymous stub) instead of falling back to its
  // loopback=admin default. See backend/utils/remote_access.py:264
  // and lib/ssr/bootstrap.ts for the full topology.
  //
  // Failure path: helper returns null on any error (network blip,
  // 5xx, timeout, missing API_PROXY_URL). Layout renders without
  // HydrationBoundary state and the existing client-side useBootstrap
  // path takes over unchanged. Never a broken page.
  const bootstrap = await fetchBootstrapServerSide();
  let dehydratedState: DehydratedState | null = null;
  if (bootstrap) {
    const client = new QueryClient();
    client.setQueryData(["bootstrap"], bootstrap);
    // Mirror the dependent-cache seeds from useBootstrap.queryFn so
    // hooks gated on the bootstrap status flip find their slice in
    // cache too. Key shapes live at frontend/hooks/useBootstrap.ts.
    const sid = (bootstrap as { active_service_id?: string | null })?.active_service_id;
    if (sid) {
      const b = bootstrap as Record<string, unknown>;
      if (Array.isArray(b.views)) {
        client.setQueryData(["views", sid], b.views);
      }
      if (b.log_fields_catalog) {
        client.setQueryData(["log-fields-catalog", sid], b.log_fields_catalog);
      }
      if (b.sync_status) {
        client.setQueryData(["sync-status", sid], b.sync_status);
      }
      if (b.log_extents) {
        client.setQueryData(["log-extents", sid], b.log_extents);
      }
    }
    dehydratedState = dehydrate(client);
  }

  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        {preloadChunks.map((href) => (
          <link key={href} rel="modulepreload" href={href} />
        ))}
        {/* world.geojson preload moved into <AppLayout> so it only emits
            on map-using routes (/dashboard, /network). Previously this
            was a global <link rel="preload">, which fired on every page
            including /share-login — wasting ~251KB of bandwidth for the
            unauthenticated share-login flow. */}
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
        >
          <QueryProvider dehydratedState={dehydratedState}>
            <TooltipProvider delay={0} closeDelay={0}>
              <AppLayout>
                <ErrorBoundary>{children}</ErrorBoundary>
              </AppLayout>
            </TooltipProvider>
          </QueryProvider>
        </ThemeProvider>
      </body>
    </html>
  );
}
