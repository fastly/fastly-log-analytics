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

import { afterEach, describe, it, expect, vi } from 'vitest'
import type { IncomingMessage, ServerResponse } from 'node:http'
import { NextResponse, NextRequest } from 'next/server'
import { proxy as middleware } from '../proxy'
import { proxyUpstreamRequest } from '@/lib/upstream-proxy'
import { GET as apiGet, POST as apiPost } from '@/app/api/[...path]/route'
import { GET as jsGet } from '@/app/js/[...path]/route'

function makeReq(url: string, headers: Record<string, string> = {}): NextRequest {
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
  } as NextRequest
}

describe('middleware /admin gate (security)', () => {
  it('redirects /admin to / when request came through Caddy', () => {
    const req = makeReq('http://localhost/admin', { 'x-proxied-by-caddy': 'true' })
    const res = middleware(req)
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
    const res = middleware(req)
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
    const res = middleware(req)
    expect(res.status).toBe(307)
  })

  it('does NOT gate non-/admin paths', () => {
    const req = makeReq('http://localhost/dashboard', { 'x-proxied-by-caddy': 'true' })
    const res = middleware(req)
    expect(res.status).toBe(200)
  })

  it('blocks /admin sub-paths (e.g. /admin/services)', () => {
    const req = makeReq('http://localhost/admin/services', { 'x-proxied-by-caddy': 'true' })
    const res = middleware(req)
    expect(res.status).toBe(307)
  })

  it('allows /admin sub-paths from local (no marker)', () => {
    const req = makeReq('http://localhost/admin/services')
    const res = middleware(req)
    expect(res.status).toBe(200)
  })

  it('treats a header value other than "true" as not-from-Caddy', () => {
    // Defensive: only the exact string "true" enables the block. An
    // upstream that someday writes "yes" / "1" / etc. wouldn't
    // accidentally lock admins out.
    const req = makeReq('http://localhost/admin', { 'x-proxied-by-caddy': 'maybe' })
    const res = middleware(req)
    expect(res.status).toBe(200)
  })

  it('blocks Next.js data requests targeting admin paths with a 403 status when from Caddy', () => {
    const req1 = makeReq('http://localhost/_next/data/build-id/admin.json', { 'x-proxied-by-caddy': 'true' })
    const res1 = middleware(req1)
    expect(res1.status).toBe(403)

    const req2 = makeReq('http://localhost/_next/data/build-id/admin/settings.json', { 'x-proxied-by-caddy': 'true' })
    const res2 = middleware(req2)
    expect(res2.status).toBe(403)
  })

  it('allows Next.js data requests targeting admin paths when local', () => {
    const req = makeReq('http://localhost/_next/data/build-id/admin.json')
    const res = middleware(req)
    expect(res.status).toBe(200)
  })

  it('blocks Server Actions with a 403 status when from Caddy', () => {
    const req = makeReq('http://localhost/', {
      'x-proxied-by-caddy': 'true',
      'next-action': 'some-action-id',
    })
    const res = middleware(req)
    expect(res.status).toBe(403)
  })

  it('allows Server Actions when local', () => {
    const req = makeReq('http://localhost/', {
      'next-action': 'some-action-id',
    })
    const res = middleware(req)
    expect(res.status).toBe(200)
  })

  // /logs is analyst-visible, so Caddy-proxied requests must NOT be
  // redirected or blocked.
  it('does not block /logs for analysts behind Caddy', () => {
    const page = middleware(makeReq('http://localhost/logs', { 'x-proxied-by-caddy': 'true' }))
    expect(page.status).toBe(200)
    expect(page.headers.get('location')).toBeNull()
    const data = middleware(
      makeReq('http://localhost/_next/data/build-id/logs.json', { 'x-proxied-by-caddy': 'true' }),
    )
    expect(data.status).toBe(200)
  })

  // Audit 2026-06-11 H8: /alerts and /usage are analyst-blocked at the
  // backend (see backend/utils/remote_access.py:_ANALYST_BLOCKED_PREFIXES).
  // Previously the FE served them with 200 → page hydrated → client-side
  // redirected to /dashboard. The URL flash made the wrong page title get
  // announced to screen readers. The middleware now mirrors the /admin gate
  // for these prefixes so the redirect is server-side.
  describe.each(['/alerts', '/usage'])('analyst-blocked prefix %s', (prefix) => {
    it(`redirects ${prefix} to / when request came through Caddy`, () => {
      const req = makeReq(`http://localhost${prefix}`, { 'x-proxied-by-caddy': 'true' })
      const res = middleware(req)
      expect(res.status).toBe(307)
      const loc = res.headers.get('location') || ''
      const path = new URL(loc).pathname
      expect(path).toBe('/')
    })

    it(`allows ${prefix} when request has no Caddy marker (admin path)`, () => {
      const req = makeReq(`http://localhost${prefix}`)
      const res = middleware(req)
      expect(res.status).toBe(200)
      expect(res.headers.get('location')).toBeNull()
    })

    it(`redirects ${prefix} sub-paths through Caddy`, () => {
      const req = makeReq(`http://localhost${prefix}/sub`, { 'x-proxied-by-caddy': 'true' })
      const res = middleware(req)
      expect(res.status).toBe(307)
    })

    it(`allows ${prefix} sub-paths from local`, () => {
      const req = makeReq(`http://localhost${prefix}/sub`)
      const res = middleware(req)
      expect(res.status).toBe(200)
    })

    it(`blocks Next.js data requests for ${prefix} with 403 when from Caddy`, () => {
      const req = makeReq(`http://localhost/_next/data/build-id${prefix}.json`, { 'x-proxied-by-caddy': 'true' })
      const res = middleware(req)
      expect(res.status).toBe(403)
    })
  })

  describe('service ID propagation', () => {
    it('sets x-service-id header when service query parameter is present', () => {
      const nextSpy = vi.spyOn(NextResponse, 'next')
      const req = makeReq('http://localhost/dashboard?service=my-service-id')
      const res = middleware(req)
      expect(res.status).toBe(200)
      expect(nextSpy).toHaveBeenCalled()
      const callArgs = nextSpy.mock.calls[0][0]
      expect(callArgs?.request?.headers?.get('x-service-id')).toBe('my-service-id')
      nextSpy.mockRestore()
    })

    it('does not set x-service-id header when service query parameter is absent', () => {
      const nextSpy = vi.spyOn(NextResponse, 'next')
      const req = makeReq('http://localhost/dashboard')
      const res = middleware(req)
      expect(res.status).toBe(200)
      expect(nextSpy).toHaveBeenCalled()
      const callArgs = nextSpy.mock.calls[0][0]
      expect(callArgs?.request?.headers?.get('x-service-id')).toBeNull()
      nextSpy.mockRestore()
    })
  })
})

afterEach(() => {
  delete process.env.API_PROXY_URL
  delete process.env.NEXT_PUBLIC_API_PROXY_URL
})

async function withUpstreamServer(
  handler: (req: IncomingMessage, res: ServerResponse) => void,
) {
  const http = await import('node:http')
  const server = http.createServer(handler)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const port = (server.address() as { port: number }).port
  process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
  return {
    port,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  }
}

describe('upstream API/JS proxy route handler (ECONNRESET resilience)', () => {
  it('middleware passes /api/* and /js/* to route handlers instead of NextResponse.rewrite', () => {
    const apiReq = new NextRequest('http://localhost:3002/api/admin/events/stream?service_id=demo')
    const apiRes = middleware(apiReq)
    expect(apiRes.status).toBe(200)
    expect(apiRes.headers.get('x-middleware-rewrite')).toBeNull()

    const jsReq = new NextRequest('http://localhost:3002/js/rum.js')
    const jsRes = middleware(jsReq)
    expect(jsRes.status).toBe(200)
    expect(jsRes.headers.get('x-middleware-rewrite')).toBeNull()
  })

  it('forwards GET and POST requests with query params, headers, body, and Set-Cookie using connection: close', async () => {
    let seenMethod = ''
    let seenUrl = ''
    let seenConn = ''
    let seenBody = ''
    const server = await withUpstreamServer((req, res) => {
      seenMethod = req.method || ''
      seenUrl = req.url || ''
      seenConn = req.headers.connection || ''
      const chunks: Buffer[] = []
      req.on('data', (c: Buffer) => chunks.push(c))
      req.on('end', () => {
        seenBody = Buffer.concat(chunks).toString('utf8')
        res.statusCode = 201
        res.setHeader('Content-Type', 'application/json')
        res.setHeader('Set-Cookie', ['a=1; Path=/', 'b=2; Path=/'])
        res.end(JSON.stringify({ ok: true, echoed: seenBody }))
      })
    })
    try {
      const req = new NextRequest('http://localhost:3002/api/query?range=1h', {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'x-custom': 'val' },
        body: JSON.stringify({ sql: 'SELECT 1' }),
      })
      const res = await apiPost(req)
      expect(res.status).toBe(201)
      expect(seenMethod).toBe('POST')
      expect(seenUrl).toBe('/api/query?range=1h')
      expect(seenConn).toBe('close')
      expect(await res.json()).toEqual({ ok: true, echoed: '{"sql":"SELECT 1"}' })
      expect(res.headers.getSetCookie()).toEqual(['a=1; Path=/', 'b=2; Path=/'])
    } finally {
      await server.close()
    }
  })

  it('retries idempotent GET once when upstream socket resets with ECONNRESET before headers', async () => {
    let attempts = 0
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const server = await withUpstreamServer((req, res) => {
      attempts += 1
      if (attempts === 1) {
        req.socket.destroy()
        return
      }
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/json')
      res.end('{"healed":true}')
    })
    try {
      const req = new NextRequest('http://localhost:3002/api/bootstrap')
      const res = await apiGet(req)
      expect(res.status).toBe(200)
      expect(await res.json()).toEqual({ healed: true })
      expect(attempts).toBe(2)
      expect(errorSpy).not.toHaveBeenCalled()
    } finally {
      errorSpy.mockRestore()
      await server.close()
    }
  })

  it('streams SSE responses and suppresses ECONNRESET logs on client abort or upstream disconnect', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const server = await withUpstreamServer((req, res) => {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
      })
      res.write('data: {"tick":1}\n\n')
      setTimeout(() => {
        req.socket.destroy()
      }, 20)
    })
    try {
      const abortController = new AbortController()
      const req = new Request('http://localhost:3002/api/admin/events/stream', {
        signal: abortController.signal,
      })
      const res = await proxyUpstreamRequest(req)
      expect(res.status).toBe(200)
      expect(res.headers.get('content-type')).toContain('text/event-stream')
      const reader = res.body!.getReader()
      const first = await reader.read()
      expect(new TextDecoder().decode(first.value)).toContain('data: {"tick":1}')
      const second = await reader.read()
      expect(second.done).toBe(true)
      abortController.abort()
      expect(errorSpy).not.toHaveBeenCalled()
    } finally {
      errorSpy.mockRestore()
      await server.close()
    }
  })

  it('proxies /js/* routes via the JS route handler', async () => {
    const server = await withUpstreamServer((_req, res) => {
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/javascript')
      res.end('window.__RUM__=1;')
    })
    try {
      const req = new NextRequest('http://localhost:3002/js/beacon.js')
      const res = await jsGet(req)
      expect(res.status).toBe(200)
      expect(await res.text()).toBe('window.__RUM__=1;')
    } finally {
      await server.close()
    }
  })
})
