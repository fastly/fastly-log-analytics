import { HydrationBoundary } from '@tanstack/react-query'

import { fetchBootstrapServerSide } from '@/lib/ssr/bootstrap'
import { fetchInsightsServerSide } from '@/lib/ssr/insights'
import { firstParam, seedDehydratedState } from '@/lib/ssr/seed'
import InsightsClient from './_sections/InsightsClient'

// Per-request RSC shell for /insights. Pre-fetches the DEFAULT insights
// selection server-side and dehydrates it so the anomaly cards render from
// cache on first paint instead of paying the client round-trip + skeleton
// flash on cold load. This is the only "heavy analytics" page that is
// SSR-seedable today, because its first-paint query key is now-INDEPENDENT
// and filter-independent (['insights', sid, windowHours, baselineHours] with
// filters:{}). The aggregates pages key on absolute, client-now()-anchored
// start/end times the server can't reproduce, so they stay client-fetch.
//
// KEY-MATCH (load-bearing): the seed key MUST byte-match InsightsBody's client
// key. windowHours/baselineHours are resolved by fetchInsightsServerSide via
// the SAME pure functions the client's useInsightsDefaults uses, over the SAME
// earliest_log_at the bootstrap response carries (which useBootstrap seeds into
// ['log-extents', sid] so the client computes the identical default on its
// first render). activeServiceId is the bootstrap's active_service_id, which is
// also what the store/useEffectiveServiceId resolves to on the hydrated path.
//
// force-dynamic because the fetchers read cookies + the Caddy marker via
// next/headers — the seed must be per-request and audience-scoped.
//
// Failure path mirrors admin/trends: either SSR helper returns null (timeout /
// non-2xx incl. analyst 401/403 / malformed / missing API_PROXY_URL / no
// Caddy-marker on a non-loopback Host), seedDehydratedState(null) yields a null
// state, and InsightsClient's useQuery picks up the cold client fetch unchanged.
export const dynamic = 'force-dynamic'

export default async function InsightsPage({
  searchParams,
}: {
  searchParams: Promise<{ service?: string | string[] }>
}) {
  const params = await searchParams
  const bootstrap = await fetchBootstrapServerSide()
  const serviceId =
    firstParam(params.service) ??
    (bootstrap as { active_service_id?: string | null } | null)?.active_service_id ??
    undefined
  const logExtents = (bootstrap as { log_extents?: unknown } | null)?.log_extents

  const seed = await fetchInsightsServerSide(serviceId, logExtents)

  // Seed under the EXACT client key. seedDehydratedState returns null when
  // seed is null, so a failed prefetch degrades to the client-fetch path.
  const dehydratedState = seed
    ? seedDehydratedState(
        ['insights', serviceId, seed.windowHours, seed.baselineHours],
        seed.data,
      )
    : null

  return (
    <HydrationBoundary state={dehydratedState}>
      <InsightsClient />
    </HydrationBoundary>
  )
}
