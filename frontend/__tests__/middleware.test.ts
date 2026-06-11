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

  it('blocks Next.js data requests targeting admin paths with a 403 status when from Caddy', () => {
    const req1 = makeReq('http://localhost/_next/data/build-id/admin.json', { 'x-proxied-by-caddy': 'true' })
    const res1: any = middleware(req1)
    expect(res1.status).toBe(403)

    const req2 = makeReq('http://localhost/_next/data/build-id/admin/settings.json', { 'x-proxied-by-caddy': 'true' })
    const res2: any = middleware(req2)
    expect(res2.status).toBe(403)
  })

  it('allows Next.js data requests targeting admin paths when local', () => {
    const req = makeReq('http://localhost/_next/data/build-id/admin.json')
    const res: any = middleware(req)
    expect(res.status).toBe(200)
  })

  it('blocks Server Actions with a 403 status when from Caddy', () => {
    const req = makeReq('http://localhost/', {
      'x-proxied-by-caddy': 'true',
      'next-action': 'some-action-id',
    })
    const res: any = middleware(req)
    expect(res.status).toBe(403)
  })

  it('allows Server Actions when local', () => {
    const req = makeReq('http://localhost/', {
      'next-action': 'some-action-id',
    })
    const res: any = middleware(req)
    expect(res.status).toBe(200)
  })

  // Audit 2026-06-11 H8: /alerts, /usage, /logs are analyst-blocked at the
  // backend (see backend/utils/remote_access.py:_ANALYST_BLOCKED_PREFIXES).
  // Previously the FE served them with 200 → page hydrated → client-side
  // redirected to /dashboard. The URL flash made the wrong page title get
  // announced to screen readers. The middleware now mirrors the /admin gate
  // for these prefixes so the redirect is server-side.
  describe.each(['/alerts', '/usage', '/logs'])('analyst-blocked prefix %s', (prefix) => {
    it(`redirects ${prefix} to / when request came through Caddy`, () => {
      const req = makeReq(`http://localhost${prefix}`, { 'x-proxied-by-caddy': 'true' })
      const res: any = middleware(req)
      expect(res.status).toBe(307)
      const loc = res.headers.get('location') || ''
      const path = new URL(loc).pathname
      expect(path).toBe('/')
    })

    it(`allows ${prefix} when request has no Caddy marker (admin path)`, () => {
      const req = makeReq(`http://localhost${prefix}`)
      const res: any = middleware(req)
      expect(res.status).toBe(200)
      expect(res.headers.get('location')).toBeNull()
    })

    it(`redirects ${prefix} sub-paths through Caddy`, () => {
      const req = makeReq(`http://localhost${prefix}/sub`, { 'x-proxied-by-caddy': 'true' })
      const res: any = middleware(req)
      expect(res.status).toBe(307)
    })

    it(`allows ${prefix} sub-paths from local`, () => {
      const req = makeReq(`http://localhost${prefix}/sub`)
      const res: any = middleware(req)
      expect(res.status).toBe(200)
    })

    it(`blocks Next.js data requests for ${prefix} with 403 when from Caddy`, () => {
      const req = makeReq(`http://localhost/_next/data/build-id${prefix}.json`, { 'x-proxied-by-caddy': 'true' })
      const res: any = middleware(req)
      expect(res.status).toBe(403)
    })
  })
})
