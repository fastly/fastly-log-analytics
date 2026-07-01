import { HydrationBoundary } from '@tanstack/react-query'

import { fetchBootstrapServerSide } from '@/lib/ssr/bootstrap'
import {
  fetchDashboardServerSide,
  DASHBOARD_SSR_DEFAULTS,
  DASHBOARD_SSR_SECTIONS,
} from '@/lib/ssr/dashboard'
import { seedDehydratedState } from '@/lib/ssr/seed'
import DashboardClient from './_sections/DashboardClient'

// Per-request RSC shell for /dashboard (the highest-traffic route). Pre-fetches
// the DEFAULT dashboard bundle server-side and dehydrates it so the traffic
// chart, geo map and aggregation cards render from cache on first paint instead
// of paying the client round-trip + skeleton flash on cold load. Replicates the
// /origin SSR template (commit 319c1b0).
//
// The bundle is COMPOSITE (aggregates + top_bots in one response) and the client
// reads both off bundleQuery.data, so this single seeded key paints the ENTIRE
// cold load from cache — no follow-up top-bots fetch.
//
// KEY-MATCH (load-bearing — a mismatch double-fetches, strictly worse than no
// SSR): the seed key MUST byte-match DashboardBody's first-paint bundle key:
//   ['dashboard','bundle', serviceId, rangeKey, anchor, filterPayload, metric, interval, fields, sections]
// On a cold load every element is server-reproducible:
//   - serviceId    = bootstrap.active_service_id
//   - rangeKey     = '24h'  (no ?range= → filterStore.relativeRange null +
//                    isAutoRange true → resolveRangeWire's token default; a custom
//                    absolute range would key on 'abs:<start>|<end>' instead, but
//                    SSR only ever seeds the cold-load default)
//   - anchor       = quantizeAnchor(now) on the 60s grid (client pins the same;
//                    only divergence is the rare minute-boundary straddle → one
//                    client refetch, never a leak/crash)
//   - filterPayload= {}      (no filters on a cold load)
//   - metric       = 'requests'  (DashboardBody's useState default)
//   - interval     = '1 hour'    (useReportConfig at the 24h default span)
//   - fields       = undefined   (DashboardBody passes none)
//   - sections     = ['core','topten','bots']
// React Query hashes keys structurally (undefined fields → null, matching), so
// the seed and the client key resolve to the identical queryHash.
//
// force-dynamic because the fetchers read cookies + the Caddy marker via
// next/headers — the seed must be per-request and audience-scoped.
//
// Failure path: fetchDashboardServerSide returns null (timeout / non-2xx incl.
// analyst 401/403 / malformed / missing API_PROXY_URL / no Caddy-marker on a
// non-loopback Host — the transport gate fails CLOSED), seedDehydratedState(null)
// yields a null state, and DashboardBody's useDashboardBundle picks up the cold
// client fetch unchanged.
export const dynamic = 'force-dynamic'

export default async function DashboardPage() {
  const bootstrap = await fetchBootstrapServerSide()
  const serviceId =
    (bootstrap as { active_service_id?: string | null } | null)?.active_service_id ?? undefined

  // Pin a single render instant so the seed body anchor and the seed KEY anchor
  // agree (resolveDashboardDefaultKey + fetchDashboardServerSide both floor it).
  const now = new Date()
  const seed = await fetchDashboardServerSide(serviceId, now)

  const dehydratedState = seed
    ? seedDehydratedState(
        [
          'dashboard',
          'bundle',
          serviceId,
          seed.rangeToken,
          seed.anchor,
          {},
          DASHBOARD_SSR_DEFAULTS.metric,
          DASHBOARD_SSR_DEFAULTS.interval,
          undefined,
          [...DASHBOARD_SSR_SECTIONS],
        ],
        seed.data,
      )
    : null

  return (
    <HydrationBoundary state={dehydratedState}>
      <DashboardClient />
    </HydrationBoundary>
  )
}
