// Server-only SSR fetcher for the /logs page. Pre-fetches the heavy
// 500-row cron-runs slice so the Cron Activity table renders without
// paying the client round-trip on cold load.
//
// /logs ALSO benefits from the bootstrap-seeded data shipped by
// useBootstrap (cron_schedule, cron_runs_first_page=10-row delta,
// last_sync header). This RSC seeds the LARGER 500-row history table
// that the Cron tab's main grid renders — bootstrap intentionally
// doesn't carry that payload (#13 audit decision: heavy table-shaped
// payloads belong on the per-page hot path, not the bootstrap one).
//
// Trust topology + Caddy-marker fail-closed defense live in
// ./_transport.ts (shared with alerts/usage_log/tos/bootstrap). /logs is
// admin-only on the wire (`/api/cron-runs` is in the analyst-blocked
// prefix list), so without the Caddy marker the shared helper refuses and
// the page client-fetches.

import { parseSsrJson, ssrUpstreamGet } from './_transport'

export async function fetchCronRunsServerSide(
  serviceId: string | undefined,
): Promise<unknown | null> {
  // Without service_id the backend's get_source dependency 400s — bail and
  // let the client useQuery handle the post-mount activeServiceId. (The
  // shared helper handles the missing-API_PROXY_URL case.)
  if (!serviceId) return null

  // Match the client's default (taskFilter='all', statusFilter='all',
  // page=1, per_page=500) so the SSR-cached entry's key matches the
  // useQuery key on first render. Any filter change refetches under a
  // different key — the SSR seed is only consumed on the cold-load state.
  // The cron-runs endpoint reads service_id from x-fastly-service-id, so
  // pass it as an extra header.
  return parseSsrJson(
    await ssrUpstreamGet({
      path: '/api/cron-runs?page=1&per_page=500',
      logPrefix: 'ssr/logs',
      injectAdminToken: true,
      extraHeaders: { 'X-Fastly-Service-Id': serviceId },
    }),
    'ssr/logs',
  )
}
