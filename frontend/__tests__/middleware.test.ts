/**
 * Security: Next.js middleware /admin gate.
 *
 * Tests pin the Caddy-marker policy from the v6 remediation plan:
 *   - Request with `X-Proxied-By-Caddy: true` → /admin redirects to /
 *     (it's a public remote visitor reaching us through Caddy).
 *   - Request without the marker → allow /admin (it's the SSH-tunnel
 *     admin path that bypasses Caddy entirely).
 *
 * Pinning the inverse logic is what prevents the regression where
 * someone "improves" the middleware to check the Host header and the
 * bypass returns.
 */

import { describe, it, expect, vi } from 'vitest'
import { proxy as middleware } from '../proxy'

function makeReq(url: string, headers: Record<string, string> = {}): any {
  const u = new URL(url)
  const hdrMap = new Headers(headers)
  return {
    nextUrl: {
      pathname: u.pathname,
      // Use a real URL clone so that setting .pathname on the result
      // actually mutates the URL (URL.pathname has a setter; a plain
      // object literal with a `pathname` property would not).
      clone: () => new URL(u.toString()),
      toString: () => u.toString(),
    },
    headers: hdrMap,
    url: url,
  }
}

describe('middleware /admin gate (security)', () => {
  it('redirects /admin to / when request came through Caddy', () => {
    const req = makeReq('http://localhost/admin', { 'x-proxied-by-caddy': 'true' })
    const res: any = middleware(req)
    // NextResponse.redirect produces a Response with a 307 status and
    // a Location header pointing to /.
    expect(res.status).toBe(307)
    const loc = res.headers.get('location') || ''
    // Strip the origin to compare only the path component.
    const path = new URL(loc).pathname
    expect(path).toBe('/')
  })

  it('allows /admin when request has no Caddy marker (SSH-tunnel path)', () => {
    const req = makeReq('http://localhost/admin')
    const res: any = middleware(req)
    // NextResponse.next() produces a Response without a redirect
    expect(res.status).toBe(200)
    expect(res.headers.get('location')).toBeNull()
  })

  it('redirects regardless of Host header value (cannot spoof in)', () => {
    // The pre-fix middleware looked at Host. A spoofed Host: localhost
    // from a remote visitor used to bypass the gate. Now the gate
    // ignores Host entirely — the Caddy marker is the only signal.
    const req = makeReq('http://localhost/admin', {
      host: 'localhost',
      'x-proxied-by-caddy': 'true',
    })
    const res: any = middleware(req)
    expect(res.status).toBe(307)
  })

  it('does NOT gate non-/admin paths', () => {
    const req = makeReq('http://localhost/dashboard', { 'x-proxied-by-caddy': 'true' })
    const res: any = middleware(req)
    expect(res.status).toBe(200)
  })

  it('blocks /admin sub-paths (e.g. /admin/services)', () => {
    const req = makeReq('http://localhost/admin/services', { 'x-proxied-by-caddy': 'true' })
    const res: any = middleware(req)
    expect(res.status).toBe(307)
  })

  it('allows /admin sub-paths from local (no marker)', () => {
    const req = makeReq('http://localhost/admin/services')
    const res: any = middleware(req)
    expect(res.status).toBe(200)
  })

  it('treats a header value other than "true" as not-from-Caddy', () => {
    // Defensive: only the exact string "true" enables the block. An
    // upstream that someday writes "yes" / "1" / etc. wouldn't
    // accidentally lock admins out.
    const req = makeReq('http://localhost/admin', { 'x-proxied-by-caddy': 'maybe' })
    const res: any = middleware(req)
    expect(res.status).toBe(200)
  })
})
