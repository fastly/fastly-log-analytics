import { HydrationBoundary } from '@tanstack/react-query'

import { fetchAlertsServerSide } from '@/lib/ssr/alerts'
import { fetchBootstrapServerSide } from '@/lib/ssr/bootstrap'
import { firstParam, seedDehydratedState } from '@/lib/ssr/seed'
import { resolveServerServiceId } from '@/lib/ssr/service'
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
// useUrlServiceSync writes it). When absent, fall back to the active
// service from bootstrap so the SSR seed key matches AlertsClient's
// useEffectiveServiceId() resolution on cold loads.
export default async function AlertsPage({
  searchParams,
}: {
  searchParams: Promise<{ service?: string | string[] }>
}) {
  const params = await searchParams
  const urlServiceId = firstParam(params.service)
  const bootstrap = urlServiceId ? null : await fetchBootstrapServerSide()
  const serviceId = resolveServerServiceId(params, bootstrap, {
    fallbackFirstService: true,
  })

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
