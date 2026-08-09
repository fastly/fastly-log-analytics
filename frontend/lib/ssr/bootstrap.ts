// Server-only by virtue of `cookies()` / `headers()` from next/headers
// (transitively, via ./_transport), which throw if imported from a
// client component or browser bundle.
//
// Trust topology (CRITICAL — the previous attempt at SSR leaked admin
// data to anonymous public visitors because it got this wrong):
//
//   Inbound request                  →  SSR upstream classification
//   ─────────────────────────────────────────────────────────────────
//   admin SSH-tunnel (no Caddy hdr)  →  no X-Remote-Analyst         →  backend treats as admin (correct)
//   public Caddy (X-Proxied-By-Caddy)→  X-Remote-Analyst: 1         →  backend treats as remote analyst (correct)
//
// The shared transport (./_transport.ts) implements that gate; this
// helper just maps the upstream response to the BootstrapResponse type.

import { headers } from 'next/headers'

import type { components } from '@/types/api.generated'

import { isLoopbackHost, parseSsrJson, ssrUpstreamGet } from './_transport'

type BootstrapResponse = components['schemas']['BootstrapResponse']

// In-process coalescing of the ADMIN SSR bootstrap fetch. layout.tsx is
// force-dynamic and awaits this on every server render, so a reload storm on
// the admin tunnel fans out one upstream /api/bootstrap per render, each
// holding a 5s node socket — an outage amplifier (2026-06-23). A shared
// in-flight promise + tiny TTL collapse concurrent admin renders into one
// upstream call.
//
// SAFETY: this is gated to the loopback-admin classification ONLY (no
// X-Proxied-By-Caddy marker + loopback Host — the exact admin branch in
// _transport's trust gate). Analyst renders (Caddy-proxied) are per-session
// and are NEVER cached/coalesced here: sharing them across sessions would
// leak one analyst's scoped data to another. Admin is a single identity, so
// sharing its bootstrap across concurrent admin renders is safe.
const _ADMIN_SSR_TTL_MS = 2000
const _adminInflightMap = new Map<string, Promise<BootstrapResponse | null>>()
const _adminCachedMap = new Map<string, { at: number; value: BootstrapResponse | null }>()

function _fetchBootstrapUpstream(serviceId?: string | null): Promise<BootstrapResponse | null> {
  // Returns null on ANY failure (no resp / non-2xx / malformed 2xx body) so
  // the root layout never throws a SyntaxError out of this server-component
  // path; the shared parseSsrJson reproduces that guard + warn contract.
  const path = serviceId ? `/api/bootstrap?service_id=${serviceId}` : '/api/bootstrap'
  return ssrUpstreamGet({ path, logPrefix: 'ssr/bootstrap', injectAdminToken: true }).then((resp) =>
    parseSsrJson<BootstrapResponse>(resp, 'ssr/bootstrap'),
  )
}

export async function fetchBootstrapServerSide(): Promise<BootstrapResponse | null> {
  // Classify exactly as _transport's trust gate does: admin == no Caddy
  // marker AND loopback Host. Only the admin path is coalesced/cached.
  const hdrs = await headers()
  const proxiedByCaddy = hdrs.get('x-proxied-by-caddy')
  const inboundHost = hdrs.get('host')
  const isAdmin = !proxiedByCaddy && !!inboundHost && isLoopbackHost(inboundHost)
  const serviceId = hdrs.get('x-service-id')

  if (!isAdmin) {
    // Analyst / anonymous: per-session, never shared.
    return _fetchBootstrapUpstream(serviceId)
  }

  const cacheKey = serviceId || '__default__'
  const now = Date.now()
  const cached = _adminCachedMap.get(cacheKey)
  if (cached && now - cached.at < _ADMIN_SSR_TTL_MS) {
    return cached.value
  }
  const inflight = _adminInflightMap.get(cacheKey)
  if (inflight) return inflight

  const promise = (async () => {
    try {
      const value = await _fetchBootstrapUpstream(serviceId)
      _adminCachedMap.set(cacheKey, { at: Date.now(), value })
      return value
    } finally {
      _adminInflightMap.delete(cacheKey)
    }
  })()
  _adminInflightMap.set(cacheKey, promise)
  return promise
}
