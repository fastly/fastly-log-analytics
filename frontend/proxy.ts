import { NextResponse, NextRequest } from 'next/server'

// Security: gate /admin on the Caddy-injected X-Proxied-By-Caddy
// header instead of the (forgeable) Host header.
//
// The Caddyfile sets `request_header X-Proxied-By-Caddy "true"` for every
// request that passes through Caddy. Caddy is the only path public
// traffic can take to Next.js — port 3000 is bound to 127.0.0.1 on the
// VM, so direct connections to it are impossible from off-host. The
// only other code path that reaches Next.js is the admin SSH tunnel,
// which forwards localhost:3000 over SSH and does NOT pass through Caddy
// — so legitimate admin requests have NO X-Proxied-By-Caddy header.
//
// Inverse logic: header present → remote visitor (via Caddy) → block
// /admin. Header absent → local-admin (SSH-tunnel or on-VM browser) →
// allow.
//
// This closes the prior bug where a remote attacker sent
// `Host: localhost` and the proxy classified them as local-admin.
// The Host header is sender-controlled; the new header is set by the
// trust boundary itself.

const ADMIN_PREFIXES = ['/admin']
const PROXIED_BY_CADDY_HEADER = 'x-proxied-by-caddy'

export function proxy(request: NextRequest) {
  const { pathname } = request.nextUrl

  const isServerAction = request.headers.has('next-action')
  const isDataRequest = pathname.startsWith('/_next/data/') && ADMIN_PREFIXES.some(p => pathname.endsWith(`${p}.json`) || pathname.includes(`${p}/`))

  const isAdminPath = ADMIN_PREFIXES.some(p => pathname === p || pathname.startsWith(`${p}/`)) || isServerAction || isDataRequest
  if (!isAdminPath) return NextResponse.next()

  // If the Caddy marker is present, this request came in through the
  // public path → remote visitor → block.
  const proxiedByCaddy = request.headers.get(PROXIED_BY_CADDY_HEADER)
  if (proxiedByCaddy === 'true') {
    if (isServerAction || isDataRequest) {
      return new NextResponse(null, { status: 403 })
    }
    const url = request.nextUrl.clone()
    url.pathname = '/'
    return NextResponse.redirect(url, 307)
  }

  return NextResponse.next()
}

// Limit proxy to admin paths, Next.js data requests for admin paths,
// and Server Actions. Everything else passes through with zero overhead.
export const config = {
  matcher: [
    '/admin/:path*',
    '/admin',
    '/_next/data/:path*/admin/:path*',
    '/_next/data/:path*/admin.json',
    {
      source: '/:path*',
      has: [
        { type: 'header', key: 'next-action' }
      ]
    }
  ],
}
