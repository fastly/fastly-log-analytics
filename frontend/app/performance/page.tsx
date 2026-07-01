import { HydrationBoundary } from '@tanstack/react-query'

import { fetchBootstrapServerSide } from '@/lib/ssr/bootstrap'
import {
  fetchPerformanceServerSide,
  PERFORMANCE_SSR_DEFAULTS,
} from '@/lib/ssr/performance'
import { seedDehydratedState } from '@/lib/ssr/seed'
import PerformanceClient from './_sections/PerformanceClient'

// Per-request RSC shell for /performance. Pre-fetches BOTH default aggregates
// selections (core + distributions) server-side and dehydrates them so the
// cards render from cache on first paint instead of paying two client
// round-trips + skeleton flashes. Replicates the /origin SSR template
// (commit 319c1b0); the only difference is TWO seeded keys (the page fires two
// section-scoped queries).
//
// KEY-MATCH (load-bearing — a mismatch double-fetches): the two seed keys MUST
// byte-match PerformanceBody's first-paint keys:
//   ['performance','aggregates','core', sid, rangeToken, anchor, filterPayload, 'p99']
//   ['performance','aggregates','distributions', sid, rangeToken, anchor, filterPayload, 'p99']
// On a cold load: rangeToken='auto', anchor=quantizeAnchor(now) on the 60s grid,
// filterPayload={}, sort='p99' (the literal default the page hard-codes). The
// performance keys carry NO bucket element. Only divergence is the rare
// minute-boundary anchor straddle → one client refetch, never a leak/crash.
//
// force-dynamic because the fetchers read cookies + the Caddy marker via
// next/headers. Failure path: fetchPerformanceServerSide returns null (any
// sub-fetch fails / fail-closed transport gate) → both keys unseeded → clean
// client fetch for both cards.
export const dynamic = 'force-dynamic'

export default async function PerformancePage() {
  const bootstrap = await fetchBootstrapServerSide()
  const serviceId =
    (bootstrap as { active_service_id?: string | null } | null)?.active_service_id ?? undefined

  // Pin a single render instant so the seed body anchor and the seed KEY anchor
  // agree (resolvePerformanceDefaultKey + fetchPerformanceServerSide floor it).
  const now = new Date()
  const seed = await fetchPerformanceServerSide(serviceId, now)

  // Seed BOTH keys under one dehydrated state. seedDehydratedState seeds a
  // single entry, so build the two-entry state inline (same QueryClient +
  // dehydrate idiom seedDehydratedState wraps) only when the seed succeeded.
  let dehydratedState = null
  if (seed) {
    const coreKey = [
      'performance',
      'aggregates',
      'core',
      serviceId,
      seed.rangeToken,
      seed.anchor,
      {},
      PERFORMANCE_SSR_DEFAULTS.sortBy,
    ]
    // Seed core first, then merge the distributions entry into the same state.
    const coreState = seedDehydratedState(coreKey, seed.coreData)
    const distKey = [
      'performance',
      'aggregates',
      'distributions',
      serviceId,
      seed.rangeToken,
      seed.anchor,
      {},
      PERFORMANCE_SSR_DEFAULTS.sortBy,
    ]
    const distState = seedDehydratedState(distKey, seed.distributionsData)
    // Both succeed together (fetchPerformanceServerSide is all-or-nothing), so
    // concatenate the single-entry queries arrays into one dehydrated state.
    if (coreState && distState) {
      dehydratedState = { ...coreState, queries: [...coreState.queries, ...distState.queries] }
    } else {
      dehydratedState = coreState ?? distState
    }
  }

  return (
    <HydrationBoundary state={dehydratedState}>
      <PerformanceClient />
    </HydrationBoundary>
  )
}
