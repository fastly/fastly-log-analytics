import { HydrationBoundary } from '@tanstack/react-query'

import { fetchBootstrapServerSide } from '@/lib/ssr/bootstrap'
import {
  fetchSecurityServerSide,
  SECURITY_SSR_DEFAULTS,
} from '@/lib/ssr/security'
import { firstParam, seedDehydratedState } from '@/lib/ssr/seed'
import SecurityClient from './_sections/SecurityClient'

// Per-request RSC shell for /security. Pre-fetches the DEFAULT security
// aggregates selection server-side and dehydrates it so the cards render from
// cache on first paint instead of paying the client round-trip + skeleton flash
// on cold load. Replicates the /origin SSR template (commit 319c1b0).
//
// KEY-MATCH (load-bearing — a mismatch double-fetches, strictly worse than no
// SSR): the seed key MUST byte-match SecurityBody's first-paint key:
//   ['security', 'aggregates', serviceId, rangeToken, anchor, filterPayload, bucketSeconds]
// On a cold load every element is server-reproducible:
//   - serviceId    = the `?service=` URL param when present (a deep link or a
//                    same-tab nav whose href carries the currently-active
//                    service — see useUrlServiceSync), else
//                    bootstrap.active_service_id. MUST prefer the URL: seeding
//                    under bootstrap's default instead of the URL's service
//                    seeds the WRONG entry whenever they differ, and the
//                    client's own fetch (for the URL's real service) then
//                    silently replaces that wrong seed's data with no loading
//                    state (keepPreviousData) — a visible wrong-service flash.
//   - rangeToken   = '24h'  (no ?range= → filterStore.relativeRange null)
//   - anchor       = quantizeAnchor(snapped-end ?? now) on the 60s grid — snaps to
//                    bootstrap.log_extents when the service's latest log is >15min
//                    stale (lib/log-extents-snap.ts, shared with FilterBar's
//                    client-side autoSetRange decision); otherwise just `now`.
//                    Divergence: the rare minute-boundary straddle, plus a rare
//                    15-minute-staleness-boundary straddle — both self-heal via
//                    one client refetch, never a leak/crash.
//   - filterPayload= {}      (no filters on a cold load)
//   - bucketSeconds= 3600    (default 24h span → "1 hour" → INTERVAL_SECONDS)
// React Query hashes keys structurally, so an equal {} match.
//
// force-dynamic because the fetchers read cookies + the Caddy marker via
// next/headers — the seed must be per-request and audience-scoped.
//
// Failure path: either SSR helper returns null (timeout / non-2xx incl. analyst
// 401/403 / malformed / missing API_PROXY_URL / no Caddy-marker on a
// non-loopback Host — the transport gate fails CLOSED), seedDehydratedState(null)
// yields a null state, and SecurityClient's useQuery picks up the cold client
// fetch unchanged.
import { Suspense } from 'react'
import SecurityLoading from './loading'

export const dynamic = 'force-dynamic'

export default async function SecurityPage({
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
    <Suspense fallback={<SecurityLoading />}>
      <SecurityPageContent serviceId={serviceId} logExtents={logExtents} />
    </Suspense>
  )
}

async function SecurityPageContent({
  serviceId,
  logExtents,
}: {
  serviceId: string | undefined
  logExtents: unknown
}) {
  // Pin a single render instant so the seed body anchor and the seed KEY anchor
  // agree (resolveSecurityDefaultKey + fetchSecurityServerSide both floor it).
  const now = new Date()
  const seed = await fetchSecurityServerSide(serviceId, now, logExtents)

  const dehydratedState = seed
    ? seedDehydratedState(
        [
          'security',
          'aggregates',
          serviceId,
          seed.rangeToken,
          seed.anchor,
          {},
          SECURITY_SSR_DEFAULTS.bucketSeconds,
        ],
        seed.data,
      )
    : null

  return (
    <HydrationBoundary state={dehydratedState}>
      <SecurityClient nowServerStr={now.toISOString()} />
    </HydrationBoundary>
  )
}
