import { HydrationBoundary } from '@tanstack/react-query'

import { fetchBootstrapServerSide } from '@/lib/ssr/bootstrap'
import {
  fetchSecurityServerSide,
  SECURITY_SSR_DEFAULTS,
  SECURITY_SSR_SECTIONS,
} from '@/lib/ssr/security'
import { seedDehydratedState } from '@/lib/ssr/seed'
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
//   - serviceId    = bootstrap.active_service_id
//   - rangeToken   = 'auto'  (no ?range= → filterStore.relativeRange null)
//   - anchor       = quantizeAnchor(now) on the 60s grid (client pins the same;
//                    only divergence is the rare minute-boundary straddle → one
//                    client refetch, never a leak/crash)
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
export const dynamic = 'force-dynamic'

export default async function SecurityPage() {
  const bootstrap = await fetchBootstrapServerSide()
  const serviceId =
    (bootstrap as { active_service_id?: string | null } | null)?.active_service_id ?? undefined

  // Pin a single render instant so the seed body anchor and the seed KEY anchor
  // agree (resolveSecurityDefaultKey + fetchSecurityServerSide both floor it).
  const now = new Date()
  const seed = await fetchSecurityServerSide(serviceId, now)

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
      <SecurityClient />
    </HydrationBoundary>
  )
}
