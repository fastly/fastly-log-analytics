// Server-only SSR fetcher for the /alerts page. Returns null on any
// failure so the calling RSC falls back to a client-side fetch — never
// let SSR errors propagate into a broken page render.
//
// Trust topology + Caddy-drift defense live in ./_transport.ts. /alerts
// is admin-only on the wire (proxy.ts 307s analysts before the page
// renders) so injectAdminToken is on — the loopback admin branch needs
// the ADMIN_SHARED_SECRET header when the backend gate is configured.

import type { components } from '@/types/api.generated'

import { parseSsrJson, ssrUpstreamGet } from './_transport'

type AlertListResponse = components['schemas']['AlertListResponse']

export async function fetchAlertsServerSide(
  serviceId: string | undefined,
): Promise<AlertListResponse | null> {
  // Service-scoped read when a service_id is known (from URL or
  // x-fastly-service-id passthrough). Falls back to the all-alerts
  // endpoint when absent — same shape from the backend (filtered by
  // analyst session if applicable).
  const path = serviceId
    ? `/api/alerts/${encodeURIComponent(serviceId)}`
    : '/api/alerts/'

  // null-on-any-failure (see app/alerts/page.tsx) is provided by the shared
  // parseSsrJson tail — non-2xx and malformed 2xx bodies degrade to client fetch.
  return parseSsrJson<AlertListResponse>(
    await ssrUpstreamGet({ path, logPrefix: 'ssr/alerts', injectAdminToken: true }),
    'ssr/alerts',
  )
}
