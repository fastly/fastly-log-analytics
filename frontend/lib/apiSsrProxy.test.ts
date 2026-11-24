import type { IncomingMessage, ServerResponse } from 'node:http'
import { afterEach, describe, expect, it } from 'vitest'
import { classifySsrRequest, proxyUpstreamRequest, registerSsrHeaders, ssrUpstreamGet } from './apiSsrProxy'

async function withUpstreamServer(handler: (req: IncomingMessage, res: ServerResponse) => void) {
  const http = await import('node:http')
  const server = http.createServer(handler)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const port = (server.address() as { port: number }).port
  process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
  return { port, close: () => new Promise<void>((resolve) => server.close(() => resolve())) }
}

describe('SEC-02 apiSsrProxy trust-boundary header sanitization', () => {
  afterEach(() => {
    delete process.env.API_PROXY_URL
    delete process.env.ADMIN_GATEWAY_SECRET
    delete process.env.ADMIN_GATEWAY_REQUIRED
  })

  it('classifySsrRequest recognizes X-FLA-Public-Gateway and X-FLA-Admin-Gateway and blocks spoofed tokens behind Caddy', () => {
    const secret = 's'.repeat(40)
    process.env.ADMIN_GATEWAY_SECRET = secret
    process.env.ADMIN_GATEWAY_REQUIRED = '1'

    const validAlt = classifySsrRequest(
      new Headers({ host: 'admin.example.com', 'x-fla-admin-gateway': secret }),
    )
    expect(validAlt).toEqual({ admin: true, gatewayAuthenticated: true, refusal: null })

    const spoofBehindPubGw = classifySsrRequest(
      new Headers({
        host: 'share.example.com',
        'x-fla-public-gateway': 'true',
        'x-admin-gateway-token': secret,
      }),
    )
    expect(spoofBehindPubGw.admin).toBe(false)
    expect(spoofBehindPubGw.gatewayAuthenticated).toBe(false)
  })

  it('proxyUpstreamRequest strips spoofed XFF/Fastly-Client-IP/admin-gateway headers and injects x-fla-frontend-proxy on public requests', async () => {
    let seenHeaders: IncomingMessage['headers'] = {}
    const server = await withUpstreamServer((req, res) => {
      seenHeaders = req.headers
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end('{"ok":true}')
    })

    try {
      process.env.ADMIN_GATEWAY_REQUIRED = '1'
      const req = new Request('https://share.example.com/api/bootstrap', {
        headers: {
          host: 'share.example.com',
          'x-fla-public-gateway': 'true',
          'x-forwarded-for': '127.0.0.1, 198.51.100.42',
          'fastly-client-ip': '127.0.0.1',
          'true-client-ip': '127.0.0.1',
          'x-admin-gateway-token': 'forged-secret',
          'x-fla-admin-gateway': 'forged-secret-2',
        },
      })
      const res = await proxyUpstreamRequest(req)
      expect(res.status).toBe(200)
      expect(seenHeaders['x-fla-frontend-proxy']).toBe('1')
      expect(seenHeaders['x-remote-analyst']).toBe('1')
      expect(seenHeaders['x-admin-gateway-token']).toBeUndefined()
      expect(seenHeaders['x-fla-admin-gateway']).toBeUndefined()
      expect(seenHeaders['fastly-client-ip']).toBeUndefined()
      expect(seenHeaders['true-client-ip']).toBeUndefined()
      // Loopback 127.0.0.1 hop stripped, public hop preserved
      expect(seenHeaders['x-forwarded-for']).toBe('198.51.100.42')
      expect(seenHeaders.host).toBe('share.example.com')
    } finally {
      await server.close()
    }
  })

  it('ssrUpstreamGet treats X-FLA-Public-Gateway as remote analyst and strips internal trust extraHeaders', async () => {
    let seenHeaders: IncomingMessage['headers'] = {}
    const server = await withUpstreamServer((req, res) => {
      seenHeaders = req.headers
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end('{"boot":1}')
    })

    try {
      process.env.ADMIN_GATEWAY_REQUIRED = '1'
      registerSsrHeaders({
        cookies: () => ({ toString: () => '', get: () => undefined }),
        headers: () =>
          new Headers({
            host: 'share.example.com',
            'x-fla-public-gateway': 'true',
            'user-agent': 'AnalystSSR/1.0',
          }),
      })
      const resp = await ssrUpstreamGet({
        path: '/api/bootstrap',
        logPrefix: 'test',
        extraHeaders: {
          'x-fla-admin-gateway': 'injected',
          'x-fla-frontend-proxy': '0',
          'x-service-id': 'svc-1',
        },
      })
      expect(resp?.statusCode).toBe(200)
      expect(seenHeaders['x-remote-analyst']).toBe('1')
      expect(seenHeaders['x-fla-frontend-proxy']).toBe('1')
      expect(seenHeaders['x-fla-admin-gateway']).toBeUndefined()
      expect(seenHeaders['x-service-id']).toBe('svc-1')
    } finally {
      await server.close()
    }
  })

  it('rejects Host: localhost spoofing when external proxy headers are present and strips non-canonical IPv6 loopback XFF hops', async () => {
    // ADMIN_GATEWAY_REQUIRED is unset (local / non-gateway mode)
    expect(classifySsrRequest(new Headers({ host: 'localhost:3000' }))).toEqual({
      admin: true,
      gatewayAuthenticated: false,
      refusal: null,
    })
    for (const spoofHeaders of [
      { host: 'localhost', 'x-forwarded-for': '203.0.113.9' },
      { host: '127.0.0.1:3000', 'fastly-client-ip': '203.0.113.9' },
      { host: 'localhost', 'x-fla-public-gateway': '1' },
      { host: 'localhost', 'x-fla-frontend-proxy': '1' },
    ] as Record<string, string>[]) {
      const res = classifySsrRequest(new Headers(spoofHeaders))
      expect(res.admin).toBe(false)
      expect(res.gatewayAuthenticated).toBe(false)
    }

    let seenHeaders: IncomingMessage['headers'] = {}
    const server = await withUpstreamServer((req, res) => {
      seenHeaders = req.headers
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end('{"ok":true}')
    })

    try {
      // 1. Proxied request with non-canonical IPv6 loopback XFF hops strips all loopback forms
      const proxiedReq = new Request('https://share.example.com/api/bootstrap', {
        headers: {
          host: 'share.example.com',
          'x-fla-public-gateway': '1',
          'x-forwarded-for': '0:0:0:0:0:0:0:0001, ::ffff:7f00:1, ::ffff:127.0.0.2, 198.51.100.42',
        },
      })
      await proxyUpstreamRequest(proxiedReq)
      expect(seenHeaders['x-forwarded-for']).toBe('198.51.100.42')

      // 2. Unproxied direct :3000 request drops client-supplied x-forwarded-for completely
      const directReq = new Request('http://localhost:3000/api/bootstrap', {
        headers: {
          host: 'localhost:3000',
          'x-forwarded-for': '198.51.100.99',
        },
      })
      await proxyUpstreamRequest(directReq)
      expect(seenHeaders['x-forwarded-for']).toBeUndefined()
    } finally {
      await server.close()
    }
  })

  it('T5-FE-01: preserves mTLS admin-gateway auth when Caddy appends X-Forwarded-For, refuses co-occurring public markers, and forwards sanitized XFF in ssrUpstreamGet', async () => {
    const secret = 'g'.repeat(40)
    process.env.ADMIN_GATEWAY_SECRET = secret
    process.env.ADMIN_GATEWAY_REQUIRED = '1'

    // 1. Valid mTLS admin-gateway request carrying Caddy's X-Forwarded-For stays admin
    expect(
      classifySsrRequest(
        new Headers({
          host: 'admin.internal.example.com',
          'x-admin-gateway-token': secret,
          'x-forwarded-for': '10.20.30.40',
        }),
      ),
    ).toEqual({ admin: true, gatewayAuthenticated: true, refusal: null })

    // 2. Co-occurring public/analyst marker with X-Admin-Gateway-Token is refused
    for (const marker of [
      'x-proxied-by-caddy',
      'x-fla-public-gateway',
      'x-fla-frontend-proxy',
      'x-remote-analyst',
    ]) {
      const spoofed = classifySsrRequest(
        new Headers({
          host: 'admin.internal.example.com',
          'x-admin-gateway-token': secret,
          [marker]: '1',
        }),
      )
      expect(spoofed.admin).toBe(false)
      expect(spoofed.gatewayAuthenticated).toBe(false)
      expect(spoofed.refusal).toBe('invalid admin gateway credential')
    }

    let seenHeaders: IncomingMessage['headers'] = {}
    const server = await withUpstreamServer((req, res) => {
      seenHeaders = req.headers
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end('{"ok":true}')
    })

    try {
      // 3. proxyUpstreamRequest for mTLS admin-gateway with XFF forwards token + XFF and NO analyst markers
      const mtlsReq = new Request('https://admin.internal.example.com/api/bootstrap', {
        headers: {
          host: 'admin.internal.example.com',
          'x-admin-gateway-token': secret,
          'x-forwarded-for': '10.20.30.40',
        },
      })
      const mtlsRes = await proxyUpstreamRequest(mtlsReq)
      expect(mtlsRes.status).toBe(200)
      expect(seenHeaders['x-admin-gateway-token']).toBe(secret)
      expect(seenHeaders['x-forwarded-for']).toBe('10.20.30.40')
      expect(seenHeaders['x-remote-analyst']).toBeUndefined()
      expect(seenHeaders['x-fla-frontend-proxy']).toBeUndefined()

      // 4. ssrUpstreamGet behind Caddy forwards sanitized X-Forwarded-For
      registerSsrHeaders({
        cookies: () => ({ toString: () => 'analyst_session_id=abcdefghijklmnop1234', get: () => undefined }),
        headers: () =>
          new Headers({
            host: 'share.example.com',
            'x-proxied-by-caddy': '1',
            'x-forwarded-for': '127.0.0.1, 203.0.113.50',
            'user-agent': 'AnalystSSR/1.0',
          }),
      })
      seenHeaders = {}
      const ssrRes = await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 't5' })
      expect(ssrRes?.statusCode).toBe(200)
      expect(seenHeaders['x-forwarded-for']).toBe('203.0.113.50')

      // 5. Direct loopback ssrUpstreamGet drops spoofed X-Forwarded-For
      delete process.env.ADMIN_GATEWAY_REQUIRED
      registerSsrHeaders({
        cookies: () => ({ toString: () => '', get: () => undefined }),
        headers: () => new Headers({ host: 'localhost:3000' }),
      })
      seenHeaders = {}
      const localRes = await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 't5-local' })
      expect(localRes?.statusCode).toBe(200)
      expect(seenHeaders['x-forwarded-for']).toBeUndefined()
    } finally {
      await server.close()
    }
  })

  it('classifySsrRequest allows Next.js single loopback x-forwarded-for on loopback host while rejecting multi-hop or external x-forwarded-for', () => {
    delete process.env.ADMIN_GATEWAY_REQUIRED
    for (const xff of ['127.0.0.1', '::1', '::ffff:127.0.0.1']) {
      expect(classifySsrRequest(new Headers({ host: '127.0.0.1:13004', 'x-forwarded-for': xff }))).toEqual({
        admin: true,
        gatewayAuthenticated: false,
        refusal: null,
      })
    }
    for (const xff of [
      '203.0.113.5',
      '127.0.0.1, 203.0.113.5',
      '127.0.0.1, ::1',
      '203.0.113.5:54321',
      '[2001:db8::1]',
      'unknown',
      '127.evil.example',
      '127.0.0.1:8080',
      '0001',
    ]) {
      expect(classifySsrRequest(new Headers({ host: '127.0.0.1:13004', 'x-forwarded-for': xff }))).toEqual({
        admin: false,
        gatewayAuthenticated: false,
        refusal: null,
      })
    }
  })

  it('SEC-22: strips internal service_id, fos_bucket, and .edgecompute.app headers from public proxied responses', async () => {
    const server = await withUpstreamServer((_req, res) => {
      res.writeHead(200, {
        'Content-Type': 'application/json',
        'X-Service-Id': 'svc-internal-123',
        'X-FOS-Bucket': 'fos-svc-internal-123-logs',
        'X-Upstream-Host': 'secret-app.edgecompute.app',
        'X-Custom-Edge': 'https://backend.edgecompute.app/v1',
        'X-Safe-Header': 'keep-me',
      })
      res.end('{"ok":true}')
    })

    try {
      const publicReq = new Request('https://share.example.com/api/bootstrap', {
        headers: {
          host: 'share.example.com',
          'x-fla-public-gateway': 'true',
        },
      })
      const publicRes = await proxyUpstreamRequest(publicReq)
      expect(publicRes.status).toBe(200)
      expect(publicRes.headers.get('x-service-id')).toBeNull()
      expect(publicRes.headers.get('x-fos-bucket')).toBeNull()
      expect(publicRes.headers.get('x-upstream-host')).toBeNull()
      expect(publicRes.headers.get('x-custom-edge')).toBeNull()
      expect(publicRes.headers.get('x-safe-header')).toBe('keep-me')
    } finally {
      await server.close()
    }
  })
})
