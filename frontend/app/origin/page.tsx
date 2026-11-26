import { Suspense } from 'react'
import { HydrationBoundary } from '@tanstack/react-query'

import { isCloudMode } from '@/lib/cloud-engine/mode'
import { analyticsReportQueryKey } from '@/lib/report-query-key'
import {
  fetchOriginServerSide,
  ORIGIN_SSR_DEFAULTS,
  ORIGIN_SSR_SECTIONS,
  resolveReportPageParams,
  seedDehydratedState,
  type ReportSearchParams,
} from '@/lib/ssr/upstream'
import OriginClient from './_sections/OriginClient'
import Loading from './loading'

export * from './_sections/OriginClient'

export const dynamic = 'force-dynamic'

export default function OriginPage({
  searchParams,
}: {
  searchParams: Promise<ReportSearchParams>
}) {
  return (
    <Suspense fallback={<Loading />}>
      <OriginHydratedSection searchParams={searchParams} />
    </Suspense>
  )
}

async function OriginHydratedSection({
  searchParams,
}: {
  searchParams: Promise<ReportSearchParams>
}) {
  const { serviceId, isDefaultView } = await resolveReportPageParams(searchParams)
  const now = new Date()
  const seed =
    isDefaultView && !isCloudMode()
      ? await fetchOriginServerSide(serviceId, now)
      : null
  const dehydratedState = seed
    ? seedDehydratedState(
        analyticsReportQueryKey({
          namespace: ['origin', 'aggregates'],
          serviceId,
          rangeToken: seed.rangeToken,
          anchor: seed.anchor,
          filterPayload: {},
          keyParts: [
            ORIGIN_SSR_DEFAULTS.bucketMinutes,
            ORIGIN_SSR_DEFAULTS.metric,
            ORIGIN_SSR_DEFAULTS.percentile,
            ORIGIN_SSR_SECTIONS,
          ],
        }),
        seed.data,
      )
    : null

  return (
    <HydrationBoundary state={dehydratedState}>
      <OriginClient nowServerStr={now.toISOString()} />
    </HydrationBoundary>
  )
}
