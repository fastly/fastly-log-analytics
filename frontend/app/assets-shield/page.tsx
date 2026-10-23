import { HydrationBoundary } from '@tanstack/react-query'

import { fetchBootstrapServerSide } from '@/lib/ssr/bootstrap'
import { fetchAssetsServerSide } from '@/lib/ssr/assets'
import { seedDehydratedState } from '@/lib/ssr/seed'
import { resolveServerServiceId } from '@/lib/ssr/service'
import AssetsClient from './_sections/AssetsClient'

export const dynamic = 'force-dynamic'

export default async function AssetsShieldPage({
  searchParams,
}: {
  searchParams: Promise<{ service?: string | string[] }>
}) {
  const params = await searchParams
  const bootstrap = await fetchBootstrapServerSide()
  const serviceId = resolveServerServiceId(params, bootstrap)

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
      <AssetsClient />
    </HydrationBoundary>
  )
}
