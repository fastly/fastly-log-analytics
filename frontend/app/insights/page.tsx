import { HydrationBoundary } from '@tanstack/react-query'

import { insightsReportQueryKey } from '@/lib/report-query-key'
import { isCloudMode } from '@/lib/cloud-engine/mode'
import {
  fetchBootstrapServerSide,
  fetchInsightsServerSide,
  resolvePageServiceId,
  seedDehydratedState,
  type ReportSearchParams,
} from '@/lib/ssr/upstream'
import InsightsClient from './_sections/InsightsClient'

export * from './_sections/InsightsClient'

export const dynamic = 'force-dynamic'

function firstParam(val: string | string[] | undefined): string | null {
  if (Array.isArray(val)) return val[0] ?? null
  return val ?? null
}

export default async function InsightsPage({
  searchParams,
}: {
  searchParams: Promise<ReportSearchParams>
}) {
  const params: ReportSearchParams =
    process.env.STATIC_EXPORT === '1' ? {} : await searchParams
  const serviceId = await resolvePageServiceId(params)
  const initialWindow = firstParam(params.window)
  const initialBaseline = firstParam(params.baseline)
  // Insights does not depend on start_time/end_time/range; seed SSR whenever no
  // report filters are active so cold loads render insight cards at FCP.
  const canSeed = !params.filters && !isCloudMode()
  const bootstrap = canSeed ? await fetchBootstrapServerSide() : null
  const seed = canSeed
    ? await fetchInsightsServerSide(
        serviceId,
        bootstrap?.log_extents,
        initialWindow,
        initialBaseline,
      )
    : null
  const dehydratedState = seed
    ? seedDehydratedState(
        insightsReportQueryKey({
          serviceId,
          windowHours: seed.windowHours,
          baselineHours: seed.baselineHours,
        }),
        seed.data,
      )
    : null

  return (
    <HydrationBoundary state={dehydratedState}>
      <InsightsClient
        initialWindow={seed?.windowHours}
        initialBaseline={seed?.baselineHours}
      />
    </HydrationBoundary>
  )
}
