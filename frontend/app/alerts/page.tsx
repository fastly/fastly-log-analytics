import { HydrationBoundary } from '@tanstack/react-query'

import { fetchAlertsServerSide } from '@/lib/ssr/alerts'
import { firstParam, seedDehydratedState } from '@/lib/ssr/seed'
import AlertsClient from './_sections/AlertsClient'

// Per-request RSC shell. We fetch the alerts list server-side and
// dehydrate it into a HydrationBoundary so AlertsClient's useQuery
// reads the cache on first render instead of paying a client round-trip
// to `/api/alerts/{service_id}` and rendering the loading skeleton.
// SSR failure (timeout, 5xx) returns null from the fetcher — the
// client falls back to the existing useQuery path, so the page never
// breaks on an upstream blip.
//
// force-dynamic is required because the fetcher reads the inbound
// cookie jar + Caddy marker via next/headers; static prerender would
// cache the dehydrated state for the wrong audience.
export const dynamic = 'force-dynamic'

// Service id flows through the ?service= query param (FilterBar /
// useUrlServiceSync writes it). When absent the fetcher hits
// /api/alerts/ (all alerts in scope), matching AlertsClient's
// useQuery fallback shape.
export default async function AlertsPage({
  searchParams,
}: {
  searchParams: Promise<{ service?: string | string[] }>
}) {
  const params = await searchParams
  const serviceId = firstParam(params.service)

  const alertsRes = await fetchAlertsServerSide(serviceId)

  // Mirror the queryKey AlertsClient uses (['alerts', activeServiceId]) so
  // its useQuery finds the pre-seeded payload — same shape as the
  // client-side fetch. The store's activeServiceId is URL-synced (see
  // useUrlServiceSync), so reading the same `?service=` param here keeps
  // server + client cache keys aligned.
  const dehydratedState = seedDehydratedState(['alerts', serviceId ?? null], alertsRes)

  return (
    <HydrationBoundary state={dehydratedState}>
      <AlertsClient />
    </HydrationBoundary>
  )
}
