import { Suspense } from 'react'
import { HydrationBoundary } from '@tanstack/react-query'

import {
  fetchStreamingServerSide,
  resolveReportPageParams,
  seedDehydratedState,
  type ReportSearchParams,
} from '@/lib/ssr/upstream'
import StreamingLoading from './loading'
import StreamingClient from './_sections/StreamingClient'
import { analyticsReportQueryKey } from '@/lib/report-query-key'
import { isCloudMode } from '@/lib/cloud-engine/mode'

export const dynamic = 'force-dynamic'

export default function StreamingPage({
  searchParams,
}: {
  searchParams: Promise<ReportSearchParams>
}) {
  return (
    <Suspense fallback={<StreamingLoading />}>
      <StreamingPageContent searchParams={searchParams} />
    </Suspense>
  )
}

async function StreamingPageContent({
  searchParams,
}: {
  searchParams: Promise<ReportSearchParams>
}) {
  const { serviceId, isDefaultView } = await resolveReportPageParams(searchParams)
  const now = new Date()
  const seed =
    isDefaultView && !isCloudMode()
      ? await fetchStreamingServerSide(serviceId, now)
      : null
  const seedData = seed?.data as { available?: boolean; has_data?: boolean } | null | undefined
  const dehydratedState =
    seed && seedData?.available && seedData?.has_data
      ? seedDehydratedState(
          analyticsReportQueryKey({
            namespace: ['cmcd', 'aggregates'],
            serviceId,
            rangeToken: seed.rangeToken,
            anchor: seed.anchor,
            filterPayload: {},
          }),
          seed.data,
        )
      : null

  return (
    <HydrationBoundary state={dehydratedState}>
      <StreamingClient nowServerStr={now.toISOString()} />
    </HydrationBoundary>
  )
}
