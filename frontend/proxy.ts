import { NextResponse, NextRequest } from 'next/server'

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

const ANALYST_BLOCKED_PREFIXES = ['/admin', '/alerts', '/usage', '/logs']
const PROXIED_BY_CADDY_HEADER = 'x-proxied-by-caddy'

// Skip CSP rebuild on static-asset paths — they don't render HTML and
// a stray Content-Security-Policy header on every chunk download is
// just wire noise. The matcher below also excludes them; the
// early-return here is belt-and-braces.
const STATIC_PATH_RE = /^\/(_next\/static|_next\/image|favicon\.ico|geo\/|public\/)/

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

  const contentType = request.headers.get('content-type') || ''
  const isServerAction = request.headers.has('next-action') || (request.method === 'POST' && contentType.toLowerCase().includes('multipart/form-data'))
  const isDataRequest = pathname.startsWith('/_next/data/') && ANALYST_BLOCKED_PREFIXES.some(p => pathname.endsWith(`${p}.json`) || pathname.includes(`${p}/`))

  const isGatedPath = ANALYST_BLOCKED_PREFIXES.some(p => pathname === p || pathname.startsWith(`${p}/`)) || isServerAction || isDataRequest

  if (isGatedPath) {
    const proxiedByCaddy = request.headers.get(PROXIED_BY_CADDY_HEADER)
    if (proxiedByCaddy === 'true') {
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

  const nonce = Buffer.from(crypto.randomUUID()).toString('base64')
  const requestHeaders = new Headers(request.headers)
  requestHeaders.set('x-nonce', nonce)

  const serviceId = request.nextUrl?.searchParams?.get('service') || (request.url ? new URL(request.url).searchParams.get('service') : null)
  if (serviceId) {
    requestHeaders.set('x-service-id', serviceId)
  }

  const response = NextResponse.next({ request: { headers: requestHeaders } })
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
      source: '/((?!_next/static|_next/image|favicon.ico|geo/|public/).*)',
    },
    {
      source: '/:path*',
      has: [{ type: 'header', key: 'next-action' }],
    },
  ],
}
