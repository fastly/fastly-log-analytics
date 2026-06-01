import { NextResponse, NextRequest } from 'next/server'

// Hostnames that mean "request came from local-admin path" — either the VM
// itself or an SSH-tunneled browser session. Anything else is treated as a
// remote visitor and blocked from admin page routes at the edge BEFORE any
// HTML renders. Client-side redirects aren't enough — they leak the page
// shell to anonymous remote visitors briefly.
const LOCAL_ADMIN_HOSTS = new Set(['localhost', '127.0.0.1', '[::1]'])

// Admin-only route prefixes. Match the backend's _ANALYST_BLOCKED_PREFIXES.
const ADMIN_PREFIXES = ['/admin']

function hostIsLocalAdmin(hostHeader: string | null): boolean {
  if (!hostHeader) return false
  const bare = hostHeader.split(':')[0].toLowerCase()
  return LOCAL_ADMIN_HOSTS.has(bare)
}

export function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl
  const isAdminPath = ADMIN_PREFIXES.some(p => pathname === p || pathname.startsWith(`${p}/`))
  if (!isAdminPath) return NextResponse.next()

  // Remote visitor hitting /admin → server-side redirect to / so the admin
  // shell is never sent over the wire. AppLayout then handles the further
  // redirect to /share-login client-side based on bootstrap state.
  if (!hostIsLocalAdmin(request.headers.get('host'))) {
    const url = request.nextUrl.clone()
    url.pathname = '/'
    return NextResponse.redirect(url, 307)
  }

  return NextResponse.next()
}

// Limit middleware to admin paths only. Everything else passes through with
// zero overhead.
export const config = {
  matcher: ['/admin/:path*', '/admin'],
}
