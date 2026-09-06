import { HydrationBoundary } from '@tanstack/react-query'

import { fetchBootstrapServerSide } from '@/lib/ssr/bootstrap'
import {
  fetchPerformanceServerSide,
  PERFORMANCE_SSR_DEFAULTS,
} from '@/lib/ssr/performance'
import { firstParam, seedDehydratedState } from '@/lib/ssr/seed'
import PerformanceClient from './_sections/PerformanceClient'

// Per-request RSC shell for /performance. Pre-fetches the unified aggregates
// selection server-side and dehydrates it so the cards render from cache on
// first paint instead of paying client round-trips + skeleton flashes.
// Replicates the /origin SSR template (commit 319c1b0).
//
// KEY-MATCH (load-bearing — a mismatch double-fetches): the seed key MUST
// byte-match PerformanceBody's first-paint key:
//   ['performance','aggregates', sid, rangeToken, anchor, filterPayload, 'p99']
// On a cold load: serviceId = the `?service=` URL param when present (a deep
// link or a same-tab nav whose href carries the currently-active service —
// see useUrlServiceSync), else bootstrap.active_service_id. MUST prefer the
// URL: seeding under bootstrap's default instead of the URL's service seeds
// the WRONG entry whenever they differ, and the client's own fetch (for the
// URL's real service) then silently replaces that wrong seed's data with no
// loading state (keepPreviousData) — a visible wrong-service data flash.
// rangeToken='24h', anchor=quantizeAnchor(snapped-end ?? now) on
// the 60s grid — snapped to bootstrap.log_extents when the service's latest log
// is >15min stale (lib/log-extents-snap.ts), otherwise just `now` — filterPayload
// ={}, sort='p99' (the literal default the page hard-codes). The performance keys
// carry NO bucket element. Divergence: the rare minute-boundary straddle, plus a
// rare 15-minute-staleness-boundary straddle — both self-heal via one client
// refetch, never a leak/crash.
//
// force-dynamic because the fetchers read cookies + the Caddy marker via
// next/headers. Failure path: fetchPerformanceServerSide returns null (any
// sub-fetch fails / fail-closed transport gate) → key unseeded → clean
// client fetch for both cards.
import { Suspense } from 'react'
import PerformanceLoading from './loading'

export const dynamic = 'force-dynamic'

export default async function PerformancePage({
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

  return (
    <Suspense fallback={<PerformanceLoading />}>
      <PerformancePageContent serviceId={serviceId} logExtents={logExtents} />
    </Suspense>
  )
}

async function PerformancePageContent({
  serviceId,
  logExtents,
}: {
  serviceId: string | undefined
  logExtents: unknown
}) {
  // Pin a single render instant so the seed body anchor and the seed KEY anchor
  // agree (resolvePerformanceDefaultKey + fetchPerformanceServerSide floor it).
  const now = new Date()
  const seed = await fetchPerformanceServerSide(serviceId, now, logExtents)

  // Seed the single query key under one dehydrated state.
  let dehydratedState = null
  if (seed) {
    const queryKey = [
      'performance',
      'aggregates',
      serviceId,
      seed.rangeToken,
      seed.anchor,
      {},
      PERFORMANCE_SSR_DEFAULTS.sortBy,
    ]
    dehydratedState = seedDehydratedState(queryKey, seed.performanceData)
  }

  return (
    <HydrationBoundary state={dehydratedState}>
      <PerformanceClient nowServerStr={now.toISOString()} />
    </HydrationBoundary>
  )
}
