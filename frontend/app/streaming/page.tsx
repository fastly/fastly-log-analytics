import { Suspense } from 'react'
import { HydrationBoundary } from '@tanstack/react-query'

import { fetchStreamingServerSide } from '@/lib/ssr/upstream'
import { resolvePageServiceId, seedDehydratedState } from '@/lib/ssr/upstream'
import StreamingLoading from './loading'
import StreamingClient from './_sections/StreamingClient'

export const dynamic = 'force-dynamic'

export default async function StreamingPage({
  searchParams,
}: {
  searchParams: Promise<{ service?: string | string[] }>
}) {
  const params = await searchParams
  const serviceId = await resolvePageServiceId(params)

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
  const seedData = seed?.data as { available?: boolean; has_data?: boolean } | null | undefined

  const dehydratedState =
    seed && seedData?.available && seedData?.has_data
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
