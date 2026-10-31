import { Suspense } from 'react'
import { HydrationBoundary } from '@tanstack/react-query'

import { fetchStreamingServerSide, resolvePageServiceId, seedDehydratedState } from '@/lib/ssr/upstream'
import StreamingLoading from './loading'
import StreamingClient from './_sections/StreamingClient'

export const dynamic = 'force-dynamic'

export default async function StreamingPage({
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
    <Suspense fallback={<StreamingLoading />}>
      <StreamingPageContent serviceId={serviceId} isDefaultView={isDefaultView} />
    </Suspense>
  )
}

async function StreamingPageContent({
  serviceId,
  isDefaultView,
}: {
  serviceId: string | undefined
  isDefaultView: boolean
}) {
  const now = new Date()
  const seed = isDefaultView ? await fetchStreamingServerSide(serviceId, now) : null
  const seedData = seed?.data as { available?: boolean; has_data?: boolean } | null | undefined
  const dehydratedState =
    seed && seedData?.available && seedData?.has_data
      ? seedDehydratedState(
          ['cmcd', 'aggregates', serviceId, seed.rangeToken, seed.anchor, {}],
          seed.data,
        )
      : null

  return (
    <HydrationBoundary state={dehydratedState}>
      <StreamingClient nowServerStr={now.toISOString()} />
    </HydrationBoundary>
  )
}
