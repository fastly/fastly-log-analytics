import { HydrationBoundary } from '@tanstack/react-query'

import { fetchCronRunsServerSide, fetchCronScheduleServerSide } from '@/lib/ssr/upstream'
import { firstParam, resolvePageServiceId, seedDehydratedState } from '@/lib/ssr/upstream'
import LogsClient from './_sections/LogsClient'
import { queryKeys } from '@/lib/query-keys'

// Per-request RSC shell for /logs. Pre-fetches the 500-row cron-runs slice
// and the cron-schedule tile matrix server-side and dehydrates both into
// HydrationBoundaries so the Cron Jobs grid and Recent Cron Activity table
// render without paying client round-trips or shifting layout on cold load.
//
// Failure path mirrors Phase V / X: SSR helpers return null on
// timeout / non-2xx, client useQuery picks up unchanged.
//
// force-dynamic because the fetcher reads cookies + Caddy marker
// via next/headers — the seed must be per-request.

export const dynamic = 'force-dynamic'

export default async function LogsPage({
  searchParams,
}: {
  searchParams: Promise<{ service?: string | string[]; tab?: string | string[] }>
}) {
  const params = await searchParams
  const serviceId = await resolvePageServiceId(params)
  const tab = firstParam(params.tab)

  const [cronRunsData, cronScheduleData] = await Promise.all([
    fetchCronRunsServerSide(serviceId),
    fetchCronScheduleServerSide(serviceId),
  ])

  // Match LogsClient's useQuery keys exactly:
  // - ['admin', 'cron-logs', activeServiceId, taskFilter, statusFilter]
  // - ['admin', 'cron-schedule', activeServiceId]
  const dehydratedRuns = seedDehydratedState(
    queryKeys.admin.cronLogs(serviceId ?? null, 'all', 'all'),
    cronRunsData,
  )
  const dehydratedSchedule = seedDehydratedState(
    queryKeys.admin.cronSchedule(serviceId ?? null),
    cronScheduleData,
  )

  return (
    <HydrationBoundary state={dehydratedRuns}>
      <HydrationBoundary state={dehydratedSchedule}>
        <LogsClient initialTab={tab} />
      </HydrationBoundary>
    </HydrationBoundary>
  )
}
