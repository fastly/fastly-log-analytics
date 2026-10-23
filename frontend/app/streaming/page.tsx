import { Suspense } from 'react'
import { HydrationBoundary } from '@tanstack/react-query'

import { fetchBootstrapServerSide } from '@/lib/ssr/bootstrap'
import { fetchStreamingServerSide } from '@/lib/ssr/streaming'
import { seedDehydratedState } from '@/lib/ssr/seed'
import { resolveServerServiceId } from '@/lib/ssr/service'
import StreamingLoading from './loading'
import StreamingClient from './_sections/StreamingClient'

export const dynamic = 'force-dynamic'

export default async function StreamingPage({
  searchParams,
}: {
  searchParams: Promise<{ service?: string | string[] }>
}) {
  const params = await searchParams
  const bootstrap = await fetchBootstrapServerSide()
  const serviceId = resolveServerServiceId(params, bootstrap)

  return (
    <Suspense fallback={<StreamingLoading />}>
      <StreamingPageContent serviceId={serviceId} />
    </Suspense>
  )
}

async function StreamingPageContent({
  serviceId,
}: {
  serviceId: string | undefined
}) {
  const now = new Date()
  const seed = await fetchStreamingServerSide(serviceId, now)

  const dehydratedState = seed
    ? seedDehydratedState(
        [
          'cmcd',
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
      <StreamingClient nowServerStr={now.toISOString()} />
    </HydrationBoundary>
  )
}
