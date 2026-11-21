import { NextResponse, NextRequest } from 'next/server'
import { ACTIVE_SERVICE_COOKIE } from './lib/utils'

// Two responsibilities live here:
//
//  1. Admin/analyst path gating: an analyst hitting /admin (or any
//     other admin-only route) gets a 307 back to '/'. The Caddy-injected
//     X-Proxied-By-Caddy header is the trust signal — present means the
//     request came through the public Caddy path (analyst), absent means
//     the SSH-tunnel admin path that bypasses Caddy. This closes the
//     prior Host-header spoof where an attacker sent `Host: localhost`
//     and was misclassified as local-admin: the Host header is sender-
//     controlled; X-Proxied-By-Caddy is set by the trust boundary
//     itself. The 307 (not just a backend 403) keeps the URL coherent
//     so screen readers don't announce the blocked page title before a
//     JS redirect runs.
//
//  2. Per-request nonce CSP: drops 'unsafe-inline' from script-src so
//     an injected <script> can't execute even if some upstream
//     sanitiser regresses. Next.js's App Router automatically threads
//     the nonce we set on the ``x-nonce`` request header into its
//     hydration / RSC bootstrap scripts, so callers don't need to touch
//     <Script> nodes themselves. 'strict-dynamic' lets the nonced
//     runtime load further chunks (which carry no nonce attribute);
//     'unsafe-inline' stays as a fallback for browsers that don't
//     understand nonces — modern browsers honouring CSP3 ignore it in
//     the presence of a nonce.

const ANALYST_BLOCKED_PREFIXES = ['/admin', '/alerts', '/usage', '/high-scale', '/logs']
const PROXIED_BY_CADDY_HEADER = 'x-proxied-by-caddy'
const PUBLIC_GATEWAY_HEADER = 'x-fla-public-gateway'
const FRONTEND_PROXY_HEADER = 'x-fla-frontend-proxy'
const ANALYST_SESSION_COOKIE = 'analyst_session_id'
const SERVICE_ID_RE = /^[A-Za-z0-9_-]{1,64}$/
const PLAUSIBLE_SESSION_RE = /^[A-Za-z0-9_-]{16,128}$/

function readCookieValue(request: NextRequest, name: string): string | null {
  if (request.cookies?.get) {
    const entry = request.cookies.get(name)
    if (entry !== undefined) return entry.value
  }
  const rawCookie = request.headers.get('cookie')
  if (!rawCookie) return null
  for (const part of rawCookie.split(';')) {
    const eq = part.indexOf('=')
    if (eq === -1) continue
    const k = part.slice(0, eq).trim()
    if (k === name) return part.slice(eq + 1).trim()
  }
  return null
}

function hasNonEmptyHeader(headers: Headers, name: string): boolean {
  const val = headers.get(name)
  return Boolean(val && val.trim() !== '')
}

function isLoopbackHost(host: string): boolean {
  const bare = host.startsWith('[') ? host.slice(1, host.indexOf(']')) : host.split(':', 1)[0]
  const lc = bare.toLowerCase()
  return lc === 'localhost' || lc === '127.0.0.1' || lc === '::1'
}

function timingSafeStringEqual(a: string, b: string): boolean {
  const enc = new TextEncoder()
  const ba = enc.encode(a)
  const bb = enc.encode(b)
  if (ba.byteLength !== bb.byteLength) return false
  let diff = 0
  for (let i = 0; i < ba.byteLength; i++) diff |= ba[i] ^ bb[i]
  return diff === 0
}

function isGatewayAuthenticatedRequest(request: NextRequest): boolean {
  const { headers } = request
  const isPublicMarker =
    hasNonEmptyHeader(headers, PROXIED_BY_CADDY_HEADER) ||
    hasNonEmptyHeader(headers, PUBLIC_GATEWAY_HEADER) ||
    hasNonEmptyHeader(headers, FRONTEND_PROXY_HEADER) ||
    hasNonEmptyHeader(headers, 'x-remote-analyst')
  if (isPublicMarker) return false

  const effectiveHost =
    headers.get('host') || request.nextUrl?.host || (request.url ? new URL(request.url).host : '')
  const gatewayToken = headers.get('x-admin-gateway-token') ?? headers.get('x-fla-admin-gateway')
  const gatewaySecret = process.env.ADMIN_GATEWAY_SECRET || ''
  const crossOrigin =
    headers.get('sec-fetch-site') === 'cross-site' ||
    (headers.get('origin') !== null &&
      headers.get('origin')?.toLowerCase() !== `https://${effectiveHost.toLowerCase()}`)
  return (
    Boolean(effectiveHost) &&
    !crossOrigin &&
    gatewaySecret.length >= 32 &&
    timingSafeStringEqual(gatewayToken || '', gatewaySecret)
  )
}

function isLoopbackIpLiteral(ip: string): boolean {
  const lc = ip.trim().toLowerCase()
  if (/^[0:]+1$/.test(lc)) return true
  const v4 = lc.startsWith('::ffff:') ? lc.slice(7) : lc
  return v4 === '127.0.0.1' || v4.startsWith('127.') || v4 === '7f00:1'
}

function hasExternalForwardedFor(headers: Headers): boolean {
  const raw = headers.get('x-forwarded-for')
  if (!raw || raw.trim() === '') return false
  const trimmed = raw.trim()
  // Next.js `next start` injects a single loopback x-forwarded-for (e.g. `127.0.0.1`,
  // `::1`, or `::ffff:127.0.0.1`) on local loopback requests. Mirror `apiSsrProxy.ts`
  // and `backend/utils/remote_access.py`: any multi-hop chain or non-loopback IP is external.
  return trimmed.includes(',') || !isLoopbackIpLiteral(trimmed)
}

function shouldGateAdminRequest(request: NextRequest): boolean {
  if (isGatewayAuthenticatedRequest(request)) return false

  const { headers } = request
  const isPublicMarker =
    hasNonEmptyHeader(headers, PROXIED_BY_CADDY_HEADER) ||
    hasNonEmptyHeader(headers, PUBLIC_GATEWAY_HEADER) ||
    hasNonEmptyHeader(headers, FRONTEND_PROXY_HEADER) ||
    hasNonEmptyHeader(headers, 'x-remote-analyst')
  const effectiveHost =
    headers.get('host') || request.nextUrl?.host || (request.url ? new URL(request.url).host : '')
  const gatewayToken = headers.get('x-admin-gateway-token') ?? headers.get('x-fla-admin-gateway')

  const hasExternalProxyHeaders =
    hasExternalForwardedFor(headers) ||
    hasNonEmptyHeader(headers, 'fastly-client-ip') ||
    hasNonEmptyHeader(headers, 'true-client-ip') ||
    hasNonEmptyHeader(headers, 'x-real-ip') ||
    hasNonEmptyHeader(headers, 'forwarded')

  return (
    isPublicMarker ||
    hasExternalProxyHeaders ||
    gatewayToken !== null ||
    process.env.ADMIN_GATEWAY_REQUIRED === '1' ||
    !effectiveHost ||
    !isLoopbackHost(effectiveHost)
  )
}

// Same-origin /api/*, /js/* proxying for the production/
// standalone build (dev mode fetches the backend directly cross-origin,
// see getApiBase() and this file's CSP connect-src comment below, so this
// path is never hit there). Used to live as next.config.ts's `rewrites()`,
// which is called once at `next build` and its destination frozen into
// .next/routes-manifest.json -- the standalone server reads that static
// manifest at runtime rather than re-invoking rewrites(), so it can't see
// a backend hostname only known at `helm install` time (Kubernetes).
// Confirmed live: the manifest had the hardcoded 'http://caddy:80'
// fallback baked in even though the container's actual runtime
// API_PROXY_URL was correct. Proxy functions run fresh per request in the
// Node.js runtime, so moving it here reads the real env on every call.
const API_PROXY_PREFIXES = ['/api/', '/js/']

// Skip CSP rebuild on static-asset paths — they don't render HTML and
// a stray Content-Security-Policy header on every chunk download is
// just wire noise. The matcher below also excludes them; the
// early-return here is belt-and-braces.
const STATIC_PATH_RE = /^\/(_next\/static|_next\/image|favicon\.ico|geo\/|public\/|maplibre-gl-worker\.mjs|maplibre-gl-shared\.mjs)/

// Dev-only CSP escape hatches:
//   1. WebSocket: Turbopack (and webpack-mode) HMR connects to the dev
//      server via ws://. Per CSP spec 'self' covers same-origin XHR but
//      Chromium will reject ws:// even to the same host:port. Allow
//      ws:/wss: in dev.
//   2. 'unsafe-eval': Turbopack's HMR runtime executes module patches
//      via eval. Prod has no HMR so this isn't needed.
//   3. Backend origin: in dev the frontend runs on its own port
//      (3000/3002) and the backend on 18002, so /api/* fetches are
//      cross-origin and 'self' refuses them. In prod they share the
//      Caddy-fronted origin. NEXT_PUBLIC_BACKEND_PORT mirrors the
//      env var lib/api.ts reads for the same routing.
const IS_DEV = process.env.NODE_ENV !== 'production'
const DEV_BACKEND_ORIGIN = (() => {
  if (!IS_DEV) return ''
  const port = process.env.NEXT_PUBLIC_BACKEND_PORT || '8000'
  return `http://127.0.0.1:${port} http://localhost:${port}`
})()

function buildCsp(nonce: string): string {
  const scriptSrc = IS_DEV
    ? `script-src 'self' 'nonce-${nonce}' 'strict-dynamic' 'unsafe-inline' 'unsafe-eval' blob:`
    : `script-src 'self' 'nonce-${nonce}' 'strict-dynamic' 'unsafe-inline' blob:`
  // Same-origin everywhere in prod: lib/api.ts:getApiBase() returns ""
  // for the browser path so admin SSH-tunnel and public deploys both
  // route via Caddy. Dev keeps its escape hatch for ws:// HMR and the
  // cross-origin backend port.
  const connectSrc = IS_DEV
    ? `connect-src 'self' ws: wss: ${DEV_BACKEND_ORIGIN}`
    : `connect-src 'self'`
  return [
    `default-src 'self'`,
    scriptSrc,
    `worker-src 'self' blob:`,
    `style-src 'self' 'unsafe-inline'`,
    `img-src 'self' data: blob:`,
    `font-src 'self' data:`,
    connectSrc,
    `frame-ancestors 'none'`,
    `base-uri 'self'`,
    `form-action 'self'`,
  ].join('; ')
}

export function proxy(request: NextRequest) {
  const { pathname } = request.nextUrl

  // No-op 204 fallback when /rum-beacon reaches the Next.js server directly
  // (e.g., via K8s nginx ingress on analyst share domains), matching
  // backend/routers/rum/ receive_rum_beacon_fallback().
  if (pathname === '/rum-beacon') {
    return new NextResponse(null, { status: 204 })
  }

  const gatewayAuthenticated = isGatewayAuthenticatedRequest(request)
  const stampWireForwardedProxy =
    !gatewayAuthenticated && hasNonEmptyHeader(request.headers, 'x-forwarded-for')

  // Bypass CSP/gating entirely -- those exist for HTML page renders, not
  // proxied API responses. Route Handlers under app/api/[...path]/route.ts
  // and app/js/[...path]/route.ts proxy to API_PROXY_URL with agent:false
  // and graceful SSE/disconnect handling (avoiding Next's internal httpxy
  // rewrite proxy which logs unhandled ECONNRESET on socket close).
  if (API_PROXY_PREFIXES.some(p => pathname.startsWith(p))) {
    if (stampWireForwardedProxy) {
      const requestHeaders = new Headers(request.headers)
      requestHeaders.set(FRONTEND_PROXY_HEADER, '1')
      return NextResponse.next({ request: { headers: requestHeaders } })
    }
    return NextResponse.next()
  }

  const rawPathname = request.url ? new URL(request.url).pathname : pathname
  const contentType = request.headers.get('content-type') || ''
  const isServerAction = request.headers.has('next-action') || (request.method === 'POST' && contentType.toLowerCase().includes('multipart/form-data'))
  const isDataRequest =
    (pathname.startsWith('/_next/data/') && ANALYST_BLOCKED_PREFIXES.some(p => pathname.endsWith(`${p}.json`) || pathname.includes(`${p}/`))) ||
    (rawPathname.startsWith('/_next/data/') && ANALYST_BLOCKED_PREFIXES.some(p => rawPathname.endsWith(`${p}.json`) || rawPathname.includes(`${p}/`)))

  const isGatedPath = ANALYST_BLOCKED_PREFIXES.some(p => pathname === p || pathname.startsWith(`${p}/`)) || isServerAction || isDataRequest

  if (isGatedPath) {
    if (shouldGateAdminRequest(request)) {
      if (isServerAction || isDataRequest) {
        // Empty 403 body — no CSP needed.
        return new NextResponse(null, { status: 403 })
      }
      const url = request.nextUrl.clone()
      url.pathname = '/'
      return NextResponse.redirect(url, 307)
    }
  }

  // CSP applies to every page render — gated or not, admin or analyst.
  // Static assets are skipped (no scripts to nonce on a font / chunk
  // download).
  if (STATIC_PATH_RE.test(pathname)) {
    return NextResponse.next()
  }

  const nonce = btoa(crypto.randomUUID())
  const requestHeaders = new Headers(request.headers)
  requestHeaders.set('x-nonce', nonce)
  if (stampWireForwardedProxy) {
    requestHeaders.set(FRONTEND_PROXY_HEADER, '1')
  }

  const rawServiceParam = request.nextUrl?.searchParams?.get('service') || (request.url ? new URL(request.url).searchParams.get('service') : null)
  const rawServiceCookie = readCookieValue(request, ACTIVE_SERVICE_COOKIE)
  const serviceParam = rawServiceParam && SERVICE_ID_RE.test(rawServiceParam) ? rawServiceParam : null
  const serviceCookie = rawServiceCookie && SERVICE_ID_RE.test(rawServiceCookie) ? rawServiceCookie : null
  const serviceId = serviceParam || serviceCookie

  if (serviceId) {
    requestHeaders.set('x-service-id', serviceId)
  }

  const response = NextResponse.next({ request: { headers: requestHeaders } })

  if (serviceParam && serviceParam !== serviceCookie && response.cookies) {
    response.cookies.set(ACTIVE_SERVICE_COOKIE, serviceParam, {
      path: '/',
      maxAge: 31536000,
      sameSite: 'lax',
      secure: !IS_DEV,
    })
  }

  const analystSessionVal = readCookieValue(request, ANALYST_SESSION_COOKIE)
  if (analystSessionVal !== null && !PLAUSIBLE_SESSION_RE.test(analystSessionVal) && response.cookies) {
    response.cookies.set(ANALYST_SESSION_COOKIE, '', {
      path: '/',
      maxAge: 0,
      httpOnly: true,
      sameSite: 'strict',
      secure: !IS_DEV,
    })
  }

  response.headers.set('Content-Security-Policy', buildCsp(nonce))
  response.headers.set('x-nonce', nonce)
  return response
}

// Broadened matcher: every page route gets the nonce CSP, while the
// admin-gating short-circuit only fires inside the proxy function for
// the gated prefixes / server actions / data requests. Static assets
// are excluded via negative-lookahead so the proxy isn't invoked for
// every chunk download. The explicit server-action matcher catches the
// `next-action` header on any path (Server Actions don't carry a
// distinguishing URL prefix).
export const config = {
  matcher: [
    {
      source: '/((?!_next/static|_next/image|favicon.ico|geo/|public/|maplibre-gl-worker\\.mjs|maplibre-gl-shared\\.mjs).*)',
    },
    {
      source: '/:path*',
      has: [{ type: 'header', key: 'next-action' }],
    },
  ],
}
