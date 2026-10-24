// Server-only SSR fetcher for the /rum status page. Returns null on any
// failure so the calling RSC falls back to a client-side fetch — never
// let SSR errors propagate into a broken page render.
//
// Trust topology + Caddy-drift defense live in ./_transport.ts.

import type { Schema } from '@/types/api'

import { parseSsrJson, ssrUpstreamGet } from './_transport'

type RumStatusResponse = Schema<'RumStatusResponse'>

export async function fetchRumStatusServerSide(
  serviceId: string | undefined,
): Promise<RumStatusResponse | null> {
  if (!serviceId) return null

  const path = `/api/services/${encodeURIComponent(serviceId)}/rum/status`

  return parseSsrJson<RumStatusResponse>(
    await ssrUpstreamGet({ path, logPrefix: 'ssr/rum-status' }),
    'ssr/rum-status',
  )
}
