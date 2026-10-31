import { Suspense } from 'react'
import { HydrationBoundary } from '@tanstack/react-query'

import { fetchAssetsServerSide, resolvePageServiceId, seedDehydratedState } from '@/lib/ssr/upstream'
import AssetsClient from './_sections/AssetsClient'
import AssetsLoading from './loading'

export const dynamic = 'force-dynamic'

export default async function AssetsShieldPage({
  searchParams,
}: {
  searchParams: Promise<{
    service?: string | string[]
    start_time?: string | string[]
    end_time?: string | string[]
    range?: string | string[]
    filters?: string | string[]
  }>
}) {
  const params = await searchParams
  const serviceId = await resolvePageServiceId(params)
  const isDefaultView =
    !params.start_time && !params.end_time && !params.range && !params.filters

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
