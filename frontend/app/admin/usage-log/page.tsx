import { HydrationBoundary } from '@tanstack/react-query'

import { fetchUsageLogServerSide } from '@/lib/ssr/usage_log'
import { firstParam, seedDehydratedState } from '@/lib/ssr/seed'
import { toQueryDate } from './_sections/shared'
import UsageLogClient from './_sections/UsageLogClient'

// Per-request RSC shell for /admin/usage-log. Pre-fetches the
// 50-row HEAD usage-log slice server-side and dehydrates it into a
// HydrationBoundary so the client island's headQuery finds its data
// on first render instead of paying a client round-trip + showing
// a loading skeleton. The aggregate stat cards land in the initial
// HTML paint instead of the "—" placeholder.
//
// The cache key incorporates the time window the client also derives
// (now − 24h, floored to the minute). When the SSR-computed window
// matches the client-computed one (overwhelmingly the common case —
// both run within the same minute), useQuery hits the seeded cache
// directly. On the rare cross-minute boundary the keys diverge by
// 60 s and the client refetches naturally; the SSR seed is never
// load-bearing for correctness, just for first-paint speed.
//
// force-dynamic is required because the fetcher reads cookies +
// Caddy markers via next/headers; the seed must be per-request.

export const dynamic = 'force-dynamic'

const HEAD_PAGE_SIZE = 50
const DEFAULT_PRESET_HOURS = 24

export default async function UsageLogPage({
  searchParams,
}: {
  searchParams: Promise<{ service?: string | string[] }>
}) {
  const params = await searchParams
  const serviceId = firstParam(params.service)

  // Mirror the client's window math (floor-to-minute) so the SSR
  // cache key aligns with what useQuery looks up on first render.
  const nowMs = Date.now()
  const nowFlooredMs = Math.floor(nowMs / 60_000) * 60_000
  const end = toQueryDate(new Date(nowFlooredMs))
  const start = toQueryDate(new Date(nowFlooredMs - DEFAULT_PRESET_HOURS * 3600 * 1000))

  const headData = await fetchUsageLogServerSide({
    serviceId,
    start,
    end,
    pageSize: HEAD_PAGE_SIZE,
  })

  // Match the client's queryKey shape exactly — see UsageLogClient.headQuery.
  // Empty strings stand in for the three optional filter fields (usage_type /
  // process_context / operation_type) since this initial SSR fetch leaves
  // them blank.
  const dehydratedState = seedDehydratedState(
    ['usage-log', 'head', serviceId ?? null, start, end, '', '', ''],
    headData,
  )

  return (
    <HydrationBoundary state={dehydratedState}>
      <UsageLogClient />
    </HydrationBoundary>
  )
}
