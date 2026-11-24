import { HydrationBoundary } from '@tanstack/react-query'

import { insightsReportQueryKey } from '@/lib/report-query-key'
import { isCloudMode } from '@/lib/cloud-engine/mode'
import {
  fetchBootstrapServerSide,
  fetchInsightsServerSide,
  resolveReportPageParams,
  seedDehydratedState,
  type ReportSearchParams,
} from '@/lib/ssr/upstream'
import InsightsClient from './_sections/InsightsClient'

export * from './_sections/InsightsClient'

export const dynamic = 'force-dynamic'

export default async function InsightsPage({
  searchParams,
}: {
  searchParams: Promise<ReportSearchParams>
}) {
  const { serviceId, isDefaultView } = await resolveReportPageParams(searchParams)
  const canSeed = isDefaultView && !isCloudMode()
  const bootstrap = canSeed ? await fetchBootstrapServerSide() : null
  const seed = canSeed
    ? await fetchInsightsServerSide(serviceId, bootstrap?.log_extents)
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
