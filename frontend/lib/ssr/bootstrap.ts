// Server-only by virtue of `cookies()` / `headers()` from next/headers,
// which throw if imported from a client component or browser bundle.
// Avoids adding the `server-only` package as a hard dep.
import { request as httpRequest } from 'node:http'
import { request as httpsRequest } from 'node:https'

import { cookies, headers } from 'next/headers'

import type { components } from '@/types/api.generated'

type BootstrapResponse = components['schemas']['BootstrapResponse']

// Per-request SSR fetch of /api/bootstrap. Returns null on any failure
// so the calling layout falls back to client-side fetching — never let
// SSR errors propagate into a broken page render.
//
// Trust topology (CRITICAL — the previous attempt at SSR leaked admin
// data to anonymous public visitors because it got this wrong):
//
//   Inbound request                  →  SSR upstream classification
//   ─────────────────────────────────────────────────────────────────
//   admin SSH-tunnel (no Caddy hdr)  →  no X-Remote-Analyst         →  backend treats as admin (correct)
//   public Caddy (X-Proxied-By-Caddy)→  X-Remote-Analyst: 1         →  backend treats as remote analyst (correct)
//
// We CANNOT just forward `X-Proxied-By-Caddy` — backend's
// `is_request_remote` (backend/utils/remote_access.py) classifies on
// `request.client.host` first. The SSR runtime hits the backend over
// loopback (`API_PROXY_URL=http://backend:8000`), so the backend sees
// a loopback peer and the Caddy header is ignored. The result on the
// public path is a full admin bootstrap response shipped into the
// public HTML.
//
// `X-Remote-Analyst: 1` IS honored from a loopback peer — that's
// exactly the "future deployments where the analyst surface is served
// via a same-host proxy" path called out in the remote_access.py
// docstring. The backend gates it further on `is_sharing_active()`
// so even a stale/wrong header on a service that isn't sharing can't
// flip the classification.
//
// Cookies pass through verbatim. For the analyst path, the
// analyst_session_id cookie identifies the session; for admin SSH
// tunnel, there's no cookie to forward and the loopback peer alone
// is enough for the admin classification.
//
// Why node:http instead of fetch(): Node's `fetch()` always
// overrides the `Host` header from the URL, ignoring any user-
// provided value. That defeats the inbound-Host forwarding the
// backend's `_remote_host_allowed` gate needs to accept the
// X-Remote-Analyst path. node:http preserves arbitrary headers
// verbatim, which is exactly what we want here.

// Bootstrap can take 1-3s under cron contention on prod (full FOS
// scan + iceberg manifest walk). 5s is generous but bounded — past
// that we'd rather fall through to client fetch and let the page
// paint with a loading skeleton than block SSR indefinitely.
const TIMEOUT_MS = 5000

interface RawResponse {
  statusCode: number
  body: string
}

function rawRequest(
  urlStr: string,
  reqHeaders: Record<string, string>,
  timeoutMs: number,
): Promise<RawResponse> {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr)
    const lib = url.protocol === 'https:' ? httpsRequest : httpRequest
    const req = lib(
      {
        hostname: url.hostname,
        port: url.port || (url.protocol === 'https:' ? 443 : 80),
        path: `${url.pathname}${url.search}`,
        method: 'GET',
        headers: reqHeaders,
        timeout: timeoutMs,
      },
      (res) => {
        const chunks: Buffer[] = []
        res.on('data', (c: Buffer) => chunks.push(c))
        res.on('end', () =>
          resolve({ statusCode: res.statusCode ?? 0, body: Buffer.concat(chunks).toString('utf8') }),
        )
      },
    )
    req.on('error', reject)
    req.on('timeout', () => {
      req.destroy(new Error(`SSR upstream timeout after ${timeoutMs}ms`))
    })
    req.end()
  })
}

export async function fetchBootstrapServerSide(): Promise<BootstrapResponse | null> {
  const base = process.env.API_PROXY_URL
  if (!base) {
    // No backend reachable from the SSR runtime — pure `next dev`
    // without docker compose. Skip silently; client fetch will pick up.
    return null
  }

  try {
    const [cookieJar, headerBag] = await Promise.all([cookies(), headers()])
    const cookieHeader = cookieJar.toString()
    const proxiedByCaddy = headerBag.get('x-proxied-by-caddy')
    const inboundHost = headerBag.get('host')

    const upstreamHeaders: Record<string, string> = {
      Accept: 'application/json',
    }
    if (cookieHeader) upstreamHeaders.Cookie = cookieHeader
    if (proxiedByCaddy) {
      // Inbound came through Caddy → remote visitor. Promote the
      // loopback SSR fetch to remote-analyst classification so the
      // backend scopes the response to the analyst session (or
      // returns the anonymous stub if no valid session cookie). See
      // backend/utils/remote_access.py:264.
      upstreamHeaders['X-Remote-Analyst'] = '1'
      // Forward the inbound Host header. Backend's _remote_host_allowed
      // (remote_access.py:296) requires remote-classified requests to
      // carry the public endpoint hostname — otherwise it rejects with
      // 400 host_not_allowed. Without this, the upstream fetch's
      // implicit Host header (`backend:8000`) fails the gate and SSR
      // silently falls back to the client fetch.
      if (inboundHost) upstreamHeaders.Host = inboundHost
    }

    const { statusCode, body } = await rawRequest(`${base}/api/bootstrap`, upstreamHeaders, TIMEOUT_MS)
    if (statusCode < 200 || statusCode >= 300) {
      console.warn(`[ssr/bootstrap] upstream returned ${statusCode}; falling back to client fetch`)
      return null
    }
    return JSON.parse(body) as BootstrapResponse
  } catch (err) {
    console.warn('[ssr/bootstrap] fetch failed; falling back to client fetch:', err)
    return null
  }
}
