import { Suspense } from 'react'
import { HydrationBoundary } from '@tanstack/react-query'

import { isCloudMode } from '@/lib/cloud-engine/mode'
import { analyticsReportQueryKey } from '@/lib/report-query-key'
import {
  fetchPerformanceServerSide,
  PERFORMANCE_SSR_DEFAULTS,
  resolveReportPageParams,
  seedDehydratedState,
  type ReportSearchParams,
} from '@/lib/ssr/upstream'
import PerformanceClient from './_sections/PerformanceClient'
import Loading from './loading'

export * from './_sections/PerformanceClient'

export const dynamic = 'force-dynamic'

export default function PerformancePage({
  searchParams,
}: {
  searchParams: Promise<ReportSearchParams>
}) {
  return (
    <Suspense fallback={<Loading />}>
      <PerformanceHydratedSection searchParams={searchParams} />
    </Suspense>
  )
}

async function PerformanceHydratedSection({
  searchParams,
}: {
  searchParams: Promise<ReportSearchParams>
}) {
  const { serviceId, isDefaultView } = await resolveReportPageParams(searchParams)
  const now = new Date()
  const seed =
    isDefaultView && !isCloudMode()
      ? await fetchPerformanceServerSide(serviceId, now)
      : null
  const dehydratedState = seed
    ? seedDehydratedState(
        analyticsReportQueryKey({
          namespace: ['performance', 'aggregates'],
          serviceId,
          rangeToken: seed.rangeToken,
          anchor: seed.anchor,
          filterPayload: {},
          keyParts: [PERFORMANCE_SSR_DEFAULTS.sortBy],
        }),
        seed.data,
      )
    : null

  return (
    <HydrationBoundary state={dehydratedState}>
      <PerformanceClient nowServerStr={now.toISOString()} />
    </HydrationBoundary>
  )
}
