import { HydrationBoundary } from '@tanstack/react-query'

import { fetchCronRunsServerSide } from '@/lib/ssr/logs'
import { firstParam, seedDehydratedState } from '@/lib/ssr/seed'
import LogsClient from './_sections/LogsClient'

// Per-request RSC shell for /logs. Pre-fetches the heavy 500-row
// cron-runs slice server-side and dehydrates it into a HydrationBoundary
// so the Cron Activity table renders without paying a client round-trip
// on cold load.
//
// Note: bootstrap (useBootstrap.queryFn) already seeds the lean
// 10-row delta (``cron_runs_first_page``), the schedule tiles
// (``cron_schedule``), and the last-sync header — those cover the
// non-table chrome and the floating dock. This RSC adds the
// 500-row history table specifically, which is intentionally NOT
// in bootstrap (#13 audit decision: heavy table-shaped payloads
// belong on the per-page hot path, not the bootstrap one).
//
// Failure path mirrors Phase V / X: SSR helper returns null on
// timeout / non-2xx, client useQuery picks up unchanged.
//
// force-dynamic because the fetcher reads cookies + Caddy marker
// via next/headers — the seed must be per-request.

export const dynamic = 'force-dynamic'

export default async function LogsPage({
  searchParams,
}: {
  searchParams: Promise<{ service?: string | string[] }>
}) {
  const params = await searchParams
  const serviceId = firstParam(params.service)

  const cronRunsData = await fetchCronRunsServerSide(serviceId)

  // Match LogsClient's useQuery key exactly:
  // ['admin', 'cron-logs', activeServiceId, taskFilter, statusFilter].
  // taskFilter/statusFilter default to 'all' on first mount; the SSR seed
  // is only consumed on the cold 'all'/'all' state. Any filter change
  // refetches under a different key.
  const dehydratedState = seedDehydratedState(
    ['admin', 'cron-logs', serviceId ?? null, 'all', 'all'],
    cronRunsData,
  )

  return (
    <HydrationBoundary state={dehydratedState}>
      <LogsClient />
    </HydrationBoundary>
  )
}
