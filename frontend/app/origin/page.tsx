import { HydrationBoundary } from '@tanstack/react-query'

import { fetchBootstrapServerSide } from '@/lib/ssr/bootstrap'
import {
  fetchOriginServerSide,
  ORIGIN_SSR_DEFAULTS,
  ORIGIN_SSR_SECTIONS,
} from '@/lib/ssr/origin'
import { seedDehydratedState } from '@/lib/ssr/seed'
import OriginClient from './_sections/OriginClient'

// Per-request RSC shell for /origin. Pre-fetches the DEFAULT origin aggregates
// selection server-side and dehydrates it so the origin cards render from cache
// on first paint instead of paying the client round-trip + skeleton flash on
// cold load. This is the SECOND "heavy analytics" page made SSR-seedable (after
// /insights), and the TEMPLATE for security/performance/dashboard.
//
// WHY origin can be seeded (it couldn't before): the origin first-paint query
// key used to carry absolute start/end timestamps anchored to the client's
// `new Date()` at paint — server-unreproducible. We ported the network
// `range_token`/`anchor` relative-range wire contract to /api/origin/aggregates
// and re-keyed the client first-paint query on (rangeToken, anchor). Both are
// server-reproducible, so the seed key below byte-matches the client key.
//
// KEY-MATCH (load-bearing — a mismatch double-fetches, strictly worse than no
// SSR): the seed key MUST byte-match OriginReportContent's first-paint key:
//   ['origin', 'aggregates', serviceId, rangeToken, anchor, filterPayload,
//    bucketMinutes, originMetric, originPercentile, ORIGIN_SECTIONS]
// On a cold load every element is server-reproducible:
//   - serviceId       = bootstrap.active_service_id (also what the store /
//                       useEffectiveServiceId resolves to on the hydrated path)
//   - rangeToken      = 'auto'  (no ?range= → filterStore.relativeRange is null)
//   - anchor          = quantizeAnchor(now) on the 60s grid (client pins the
//                       same; the only divergence is the rare minute-boundary
//                       straddle → one client refetch, never a leak/crash)
//   - filterPayload   = {}      (no filters on a cold load)
//   - bucketMinutes   = 60      (the default 24h display span → "1 hour")
//   - originMetric    = 'ttfb', originPercentile = 'p95'  (useState defaults)
//   - ORIGIN_SECTIONS = ORIGIN_SSR_SECTIONS (kept in lockstep)
// React Query hashes keys structurally, so an equal {} / equal array match.
//
// force-dynamic because the fetchers read cookies + the Caddy marker via
// next/headers — the seed must be per-request and audience-scoped. Without it
// Next would SSG this page with a permanently-empty dehydrated state.
//
// Failure path mirrors insights/trends: either SSR helper returns null (timeout
// / non-2xx incl. analyst 401/403 / malformed / missing API_PROXY_URL / no
// Caddy-marker on a non-loopback Host — the transport gate fails CLOSED),
// seedDehydratedState(null) yields a null state, and OriginClient's useQuery
// picks up the cold client fetch unchanged.
export const dynamic = 'force-dynamic'

export default async function OriginPage() {
  const bootstrap = await fetchBootstrapServerSide()
  const serviceId =
    (bootstrap as { active_service_id?: string | null } | null)?.active_service_id ?? undefined

  // Pin a single render instant so the seed body anchor and the seed KEY anchor
  // agree (resolveOriginDefaultKey + fetchOriginServerSide both floor it).
  const now = new Date()
  const seed = await fetchOriginServerSide(serviceId, now)

  // Seed under the EXACT client first-paint key. seedDehydratedState returns
  // null when seed is null, so a failed prefetch degrades to the client fetch.
  const dehydratedState = seed
    ? seedDehydratedState(
        [
          'origin',
          'aggregates',
          serviceId,
          seed.rangeToken,
          seed.anchor,
          {},
          ORIGIN_SSR_DEFAULTS.bucketMinutes,
          ORIGIN_SSR_DEFAULTS.metric,
          ORIGIN_SSR_DEFAULTS.percentile,
          ORIGIN_SSR_SECTIONS,
        ],
        seed.data,
      )
    : null

  return (
    <HydrationBoundary state={dehydratedState}>
      <OriginClient />
    </HydrationBoundary>
  )
}
