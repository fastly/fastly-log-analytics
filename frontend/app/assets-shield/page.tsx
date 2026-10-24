import { Suspense } from 'react'
import { HydrationBoundary } from '@tanstack/react-query'

import { fetchAssetsServerSide } from '@/lib/ssr/upstream'
import { seedDehydratedState } from '@/lib/ssr/seed'
import { resolvePageServiceId } from '@/lib/ssr/service'
import AssetsClient from './_sections/AssetsClient'
import AssetsLoading from './loading'

export const dynamic = 'force-dynamic'

export default async function AssetsShieldPage({
  searchParams,
}: {
  searchParams: Promise<{ service?: string | string[] }>
}) {
  const params = await searchParams
  const serviceId = await resolvePageServiceId(params)

  return (
    <Suspense fallback={<AssetsLoading />}>
      <AssetsHydratedSection serviceId={serviceId} />
    </Suspense>
  )
}

async function AssetsHydratedSection({
  serviceId,
}: {
  serviceId: string | undefined
}) {
  const now = new Date()
  const seed = await fetchAssetsServerSide(serviceId, now)

  const dehydratedState = seed
    ? seedDehydratedState(
        [
          'assets',
          'aggregates',
          serviceId,
          seed.rangeToken,
          seed.anchor,
          {},
        ],
        seed.data,
      )
    : null

  return (
    <HydrationBoundary state={dehydratedState}>
      <AssetsClient nowServerStr={now.toISOString()} />
    </HydrationBoundary>
  )
}
