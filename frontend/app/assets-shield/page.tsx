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

export default async function AssetsShieldPage({
  searchParams,
}: {
  searchParams: Promise<ReportSearchParams>
}) {
  const { serviceId, isDefaultView } = await resolveReportPageParams(searchParams)

  return (
    <Suspense fallback={<AssetsLoading />}>
      <AssetsHydratedSection serviceId={serviceId} isDefaultView={isDefaultView} />
    </Suspense>
  )
}

async function AssetsHydratedSection({
  serviceId,
  isDefaultView,
}: {
  serviceId: string | undefined
  isDefaultView: boolean
}) {
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
