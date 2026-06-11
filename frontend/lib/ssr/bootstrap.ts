// Server-only by virtue of `cookies()` / `headers()` from next/headers,
// which throw if imported from a client component or browser bundle.
// Avoids adding the `server-only` package as a hard dep.
import { cookies, headers } from 'next/headers'

import type { components } from '@/types/api.generated'

type BootstrapResponse = components['schemas']['BootstrapResponse']

// Per-request SSR fetch of /api/bootstrap. Returns null on any failure
// so the calling layout falls back to client-side fetching — never let
// SSR errors propagate into a broken page render.
//
// Auth model contract (mirrors backend/utils/remote_access.py):
//   - admin SSH-tunnel:        no X-Proxied-By-Caddy on inbound, none forwarded → backend treats as local admin
//   - public Caddy → analyst:  X-Proxied-By-Caddy: true on inbound, forwarded → backend treats as remote analyst
// Forward the header only when the inbound request had it, otherwise we'd
// upgrade an admin request into an analyst-restricted one.
//
// API_PROXY_URL is set in docker-compose.yml ("http://backend:8000") and
// docker-compose.prod.yml ("http://127.0.0.1:8000"). Mirrors what
// lib/api.ts:41 reads on the non-browser code path.
const TIMEOUT_MS = 1500

export async function fetchBootstrapServerSide(): Promise<BootstrapResponse | null> {
  const base = process.env.API_PROXY_URL
  if (!base) {
    // No backend reachable from the SSR runtime — common in dev when
    // running `next dev` without the docker compose stack. Skip
    // silently; the client fetch will pick up.
    return null
  }

  try {
    const [cookieJar, headerBag] = await Promise.all([cookies(), headers()])
    const cookieHeader = cookieJar.toString()
    const proxiedByCaddy = headerBag.get('x-proxied-by-caddy')

    const upstreamHeaders: Record<string, string> = {
      Accept: 'application/json',
    }
    if (cookieHeader) upstreamHeaders.Cookie = cookieHeader
    if (proxiedByCaddy) upstreamHeaders['X-Proxied-By-Caddy'] = proxiedByCaddy

    const res = await fetch(`${base}/api/bootstrap`, {
      method: 'GET',
      headers: upstreamHeaders,
      cache: 'no-store',
      signal: AbortSignal.timeout(TIMEOUT_MS),
    })

    if (!res.ok) {
      console.warn(`[ssr/bootstrap] upstream returned ${res.status}; falling back to client fetch`)
      return null
    }

    return (await res.json()) as BootstrapResponse
  } catch (err) {
    // AbortError on timeout, TypeError on connection refused, etc. All
    // non-fatal — page still renders, client picks up.
    console.warn('[ssr/bootstrap] fetch failed; falling back to client fetch:', err)
    return null
  }
}
