import { Suspense } from 'react'
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
import InsightsLoading from './loading'
import InsightsClient from './_sections/InsightsClient'

export * from './_sections/InsightsClient'

export const dynamic = 'force-dynamic'

function firstParam(val: string | string[] | undefined): string | null {
  if (Array.isArray(val)) return val[0] ?? null
  return val ?? null
}

export default function InsightsPage({
  searchParams,
}: {
  searchParams: Promise<ReportSearchParams>
}) {
  return (
    <Suspense fallback={<InsightsLoading />}>
      <InsightsPageContent searchParams={searchParams} />
    </Suspense>
  )
}

export async function InsightsPageContent({
  searchParams,
}: {
  searchParams: Promise<ReportSearchParams>
}) {
  const params: ReportSearchParams =
    process.env.STATIC_EXPORT === '1' ? {} : await searchParams
  const initialWindow = firstParam(params.window)
  const initialBaseline = firstParam(params.baseline)
  // Insights does not depend on start_time/end_time/range; seed SSR whenever no
  // report filters are active so cold loads render insight cards at FCP.
  const canSeed = !params.filters && !isCloudMode()
  const bootstrap = canSeed ? await fetchBootstrapServerSide() : null
  const urlServiceId = firstParam(params.service)
  const serviceId = canSeed
    ? (urlServiceId && /^[A-Za-z0-9_-]{1,64}$/.test(urlServiceId)
        ? urlServiceId
        : bootstrap?.active_service_id ?? undefined)
    : await resolvePageServiceId(params)
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
