import { HydrationBoundary } from '@tanstack/react-query'

import { fetchBootstrapServerSide } from '@/lib/ssr/bootstrap'
import { fetchAssetsServerSide } from '@/lib/ssr/assets'
import { firstParam, seedDehydratedState } from '@/lib/ssr/seed'
import AssetsClient from './_sections/AssetsClient'

export const dynamic = 'force-dynamic'

export default async function AssetsShieldPage({
  searchParams,
}: {
  searchParams: Promise<{ service?: string | string[] }>
}) {
  const params = await searchParams
  const bootstrap = await fetchBootstrapServerSide()
  const serviceId =
    firstParam(params.service) ??
    (bootstrap as { active_service_id?: string | null } | null)?.active_service_id ??
    undefined
  const logExtents = (bootstrap as { log_extents?: unknown } | null)?.log_extents

  const now = new Date()
  const seed = await fetchAssetsServerSide(serviceId, now, logExtents)

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
