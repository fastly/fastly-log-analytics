import { Suspense } from 'react'
import { HydrationBoundary } from '@tanstack/react-query'

import {
  fetchAssetsServerSide,
  resolveReportPageParams,
  seedDehydratedState,
  type ReportSearchParams,
} from '@/lib/ssr/upstream'
import AssetsClient from './_sections/AssetsClient'
import AssetsLoading from './loading'

export const dynamic = 'force-dynamic'

export default function AssetsShieldPage({
  searchParams,
}: {
  searchParams: Promise<ReportSearchParams>
}) {
  return (
    <Suspense fallback={<AssetsLoading />}>
      <AssetsHydratedSection searchParams={searchParams} />
    </Suspense>
  )
}

async function AssetsHydratedSection({
  searchParams,
}: {
  searchParams: Promise<ReportSearchParams>
}) {
  const { serviceId, isDefaultView } = await resolveReportPageParams(searchParams)
  const now = new Date()
  const seed = isDefaultView ? await fetchAssetsServerSide(serviceId, now) : null
  const dehydratedState = seed
    ? seedDehydratedState(
        ['assets', 'aggregates', serviceId, seed.rangeToken, seed.anchor, {}],
        seed.data,
      )
    : null

  return (
    <HydrationBoundary state={dehydratedState}>
      <AssetsClient nowServerStr={now.toISOString()} />
    </HydrationBoundary>
  )
}
