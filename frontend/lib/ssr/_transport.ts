// Shared transport for the server-side fetchers under lib/ssr/.
//
// Each SSR helper (bootstrap, alerts, tos, logs, usage_log) was previously
// a near-verbatim copy of:
//
//   - isLoopbackHost (Host header parsing for IPv4 + IPv6 + named hosts)
//   - rawRequest (node:http / node:https GET that preserves the Host
//     header — fetch() would overwrite it from the URL, defeating
//     backend's _remote_host_allowed gate)
//   - Caddy-marker trust gate: forward only when the X-Proxied-By-Caddy
//     marker is present (analyst, came through Caddy) OR the inbound Host
//     is explicitly loopback (the admin SSH-tunnel). A no-marker request
//     with a non-loopback or absent Host is the Caddy-drift / SSRF
//     fingerprint and is refused — forwarding it would leak the backend's
//     admin-from-loopback classification into public HTML. (See the gate
//     in ssrUpstreamGet.)
//   - Cookie + X-Remote-Analyst + Host propagation
//
// All callers do this identically — verified by reading their
// pre-extraction shapes. Centralising here means future changes (e.g.
// adding a new trust-topology gate, propagating a new header) update
// one place instead of five.
//
// Per-caller variation lives entirely in the response handling: tos.ts
// special-cases 401 → 'unauthenticated', alerts.ts wants the admin
// shared-secret injected on the loopback path. Both are expressible as
// arguments + post-call branching.
// Required mTLS gateway mode replaces the loopback Host shortcut with an
// independently verified server-only credential; public traffic stays analyst.

import { timingSafeEqual } from 'node:crypto'
import { request as httpRequest } from 'node:http'
import { request as httpsRequest } from 'node:https'

import { cookies, headers } from 'next/headers'

import { DEBUG_RESPONSES_COOKIE } from '@/lib/debug-cookie'

// Bootstrap can take 1-3s under cron contention on prod (full FOS scan
// + iceberg manifest walk). 5s is generous but bounded — past that
// we'd rather fall through to client fetch and let the page paint with
// a loading skeleton than block SSR indefinitely. The shorter SSR
// endpoints (tos, alerts) inherit the same ceiling for consistency.
export const TIMEOUT_MS = 5000

export function isLoopbackHost(host: string): boolean {
  // Strip port for comparison. IPv6 loopback "[::1]:port" wraps the
  // literal in brackets per RFC 3986; bare-IPv4 / hostname forms split
  // on the first colon.
  const bare = host.startsWith('[')
    ? host.slice(1, host.indexOf(']'))
    : host.split(':', 1)[0]
  const lc = bare.toLowerCase()
  return lc === 'localhost' || lc === '127.0.0.1' || lc === '::1'
}

export function classifySsrRequest(headerBag: Pick<Headers, 'get'>): {
  admin: boolean
  gatewayAuthenticated: boolean
  refusal: string | null
} {
  const proxiedByCaddy = headerBag.get('x-proxied-by-caddy')
  const inboundHost = headerBag.get('host')
  const gatewayToken = headerBag.get('x-admin-gateway-token')
  const gatewaySecret = process.env.ADMIN_GATEWAY_SECRET || ''
  const suppliedGateway = Buffer.from(gatewayToken || '')
  const expectedGateway = Buffer.from(gatewaySecret)
  const gatewayAuthenticated = !proxiedByCaddy
    && gatewaySecret.length >= 32
    && suppliedGateway.length === expectedGateway.length
    && timingSafeEqual(suppliedGateway, expectedGateway)
  let refusal: string | null = null
  if (gatewayToken !== null && !gatewayAuthenticated) {
    refusal = 'invalid admin gateway credential'
  } else if (gatewayAuthenticated && !inboundHost) {
    refusal = 'admin gateway request without Host'
  } else if (gatewayAuthenticated && (
    headerBag.get('sec-fetch-site') === 'cross-site'
    || (headerBag.get('origin') !== null
      && headerBag.get('origin')?.toLowerCase() !== `https://${inboundHost?.toLowerCase()}`)
  )) {
    refusal = 'cross-origin admin gateway request'
  } else if (process.env.ADMIN_GATEWAY_REQUIRED === '1' && !gatewayAuthenticated && !proxiedByCaddy) {
    refusal = 'authenticated admin gateway required'
  } else if (!gatewayAuthenticated && !proxiedByCaddy && (!inboundHost || !isLoopbackHost(inboundHost))) {
    refusal = `no X-Proxied-By-Caddy marker and non-loopback/absent Host=${inboundHost ?? '<none>'}`
  }
  return { admin: !refusal && !proxiedByCaddy, gatewayAuthenticated, refusal }
}

export interface SSRUpstreamResponse {
  statusCode: number
  body: string
}

/**
 * Shared response tail for the lib/ssr fetchers. Reproduces the
 * `!resp` guard, the 2xx range check (warn + null), and the
 * JSON.parse-in-try/catch (warn + null) that every fetcher previously
 * inlined verbatim. Callers with extra status branches (tos.ts's
 * 401→'unauthenticated') handle them before delegating here.
 *
 * Returns null on ANY failure so the calling RSC degrades to a
 * client-side fetch rather than throwing out of the server component.
 */
export function parseSsrJson<T = unknown>(
  resp: SSRUpstreamResponse | null,
  logPrefix: string,
): T | null {
  if (!resp) return null
  if (resp.statusCode < 200 || resp.statusCode >= 300) {
    console.warn(`[${logPrefix}] upstream returned ${resp.statusCode}; falling back to client fetch`)
    return null
  }
  try {
    return JSON.parse(resp.body) as T
  } catch {
    console.warn(`[${logPrefix}] malformed upstream body; falling back to client fetch`)
    return null
  }
}

interface SSRGetOpts {
  /** Path on the backend, e.g. ``/api/bootstrap``. */
  path: string
  /** Prefix used in console.warn lines, e.g. ``ssr/bootstrap``. */
  logPrefix: string
  /**
   * When true, the loopback admin branch (no Caddy marker + loopback Host)
   * attaches ``X-Admin-Token`` from ``ADMIN_SHARED_SECRET`` so SSR fetches
   * reach the admin-gated routes the analyst path never sees. Skipped on the
   * Caddy-proxied branch because the backend reads admin-token only from
   * loopback peers.
   */
  injectAdminToken?: boolean
  /**
   * Extra request headers merged into the upstream call (e.g.
   * ``X-Fastly-Service-Id`` for the per-service cron-runs fetch). Critical
   * trust headers (Host / X-Remote-Analyst / X-Admin-Token) are applied
   * after these and win on conflict.
   */
  extraHeaders?: Record<string, string>
  /**
   * HTTP method. Defaults to ``GET`` so every existing GET caller is
   * unchanged. ``POST`` is the only other supported verb — it exists solely
   * so the analytics endpoints that take a request body (e.g. ``/api/insights``)
   * can be SSR-prefetched. The method does NOT influence the trust gate:
   * the Caddy-marker classification + X-Remote-Analyst / Host promotion run
   * identically before the request is dispatched, so a POST carries the exact
   * same analyst-clamp the GET path applies — it can never widen trust.
   */
  method?: 'GET' | 'POST'
  /**
   * JSON-serialisable request body for ``POST``. Ignored on ``GET``. When
   * present, the transport serialises it and sets Content-Type/Content-Length;
   * it is sent as the request payload AFTER the trust gate has already
   * classified the request, so the body cannot affect audience scoping.
   */
  body?: unknown
}

export async function ssrUpstreamGet({
  path,
  logPrefix,
  injectAdminToken = false,
  extraHeaders,
  method = 'GET',
  body,
}: SSRGetOpts): Promise<SSRUpstreamResponse | null> {
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
    const gatewaySecret = process.env.ADMIN_GATEWAY_SECRET || ''
    const { gatewayAuthenticated, refusal } = classifySsrRequest(headerBag)
    if (refusal) {
      console.warn(`[${logPrefix}] refusal: ${refusal} — falling back to client fetch`)
      return null
    }
    // DiagnosticsPanel writes this when either debug toggle is on, so SSR's
    // own upstream fetch can carry x-debug-responses too — otherwise the
    // Query Debugging panel never sees queries that ran during SSR (see
    // lib/debug-cookie.ts). Read here, applied on the admin branch below.
    const debugResponsesRequested = cookieJar.get(DEBUG_RESPONSES_COOKIE)?.value === '1'

    const upstreamHeaders: Record<string, string> = {
      Accept: 'application/json',
    }
    if (cookieHeader) upstreamHeaders.Cookie = cookieHeader
    const ua = headerBag.get('user-agent')
    if (ua) upstreamHeaders['user-agent'] = ua
    const platform = headerBag.get('sec-ch-ua-platform')
    if (platform) upstreamHeaders['sec-ch-ua-platform'] = platform

    // Caller-supplied non-trust headers first; the trust headers below win.
    if (extraHeaders) {
      const trustHeaders = new Set(['host', 'x-remote-analyst', 'x-admin-token', 'x-admin-gateway-token'])
      for (const [key, value] of Object.entries(extraHeaders)) {
        if (!trustHeaders.has(key.toLowerCase())) upstreamHeaders[key] = value
      }
    }
    if (gatewayAuthenticated) {
      upstreamHeaders['X-Admin-Gateway-Token'] = gatewaySecret
      if (inboundHost) upstreamHeaders.Host = inboundHost
    }
    if (proxiedByCaddy) {
      // Inbound came through Caddy → remote visitor. Promote the
      // loopback SSR fetch to remote-analyst classification so the
      // backend scopes the response to the analyst session.
      upstreamHeaders['X-Remote-Analyst'] = '1'
      // Backend's _remote_host_allowed (remote_access.py:296) requires
      // remote-classified requests to carry the public endpoint
      // hostname — otherwise 400 host_not_allowed.
      if (inboundHost) upstreamHeaders.Host = inboundHost
    }

    // Admin shared-secret pickup on the loopback admin branch only.
    // The Caddy-proxied branch is by definition analyst-classified and
    // the backend reads X-Admin-Token only on the admin classification.
    if (injectAdminToken && !proxiedByCaddy && !gatewayAuthenticated) {
      const adminToken = (process.env.ADMIN_SHARED_SECRET || '').trim()
      if (adminToken) {
        upstreamHeaders['X-Admin-Token'] = adminToken
      }
    }

    // Debug-query visibility is admin-only, same branch as X-Admin-Token
    // above (independent of whether this particular caller injects the
    // admin token) — never sent on the Caddy-proxied/analyst branch, which
    // the backend would strip anyway (RemoteAccessMiddleware).
    if (debugResponsesRequested && !proxiedByCaddy) {
      upstreamHeaders['x-debug-responses'] = '1'
    }

    // Serialise the POST body (if any) and stamp its content headers. This
    // runs AFTER the trust gate + analyst/admin promotion above, so the
    // request is already audience-classified; the payload cannot alter scope.
    // GET requests carry no body (serialisedBody stays undefined).
    let serialisedBody: string | undefined
    if (method === 'POST' && body !== undefined) {
      serialisedBody = JSON.stringify(body)
      upstreamHeaders['Content-Type'] = 'application/json'
      upstreamHeaders['Content-Length'] = String(Buffer.byteLength(serialisedBody))
    }

    return await rawRequest(`${base}${path}`, upstreamHeaders, TIMEOUT_MS, method, serialisedBody)
  } catch (err) {
    console.warn(`[${logPrefix}] fetch failed; falling back to client fetch:`, err)
    return null
  }
}

function rawRequest(
  urlStr: string,
  reqHeaders: Record<string, string>,
  timeoutMs: number,
  method: 'GET' | 'POST' = 'GET',
  body?: string,
): Promise<SSRUpstreamResponse> {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr)
    const lib = url.protocol === 'https:' ? httpsRequest : httpRequest
    const req = lib(
      {
        hostname: url.hostname,
        port: url.port || (url.protocol === 'https:' ? 443 : 80),
        path: `${url.pathname}${url.search}`,
        method,
        headers: reqHeaders,
        timeout: timeoutMs,
      },
      (res) => {
        const chunks: Buffer[] = []
        res.on('data', (c: Buffer) => chunks.push(c))
        res.on('end', () =>
          resolve({
            statusCode: res.statusCode ?? 0,
            body: Buffer.concat(chunks).toString('utf8'),
          }),
        )
      },
    )
    req.on('error', reject)
    req.on('timeout', () => {
      req.destroy(new Error(`SSR upstream timeout after ${timeoutMs}ms`))
    })
    if (method === 'POST' && body !== undefined) req.write(body)
    req.end()
  })
}
