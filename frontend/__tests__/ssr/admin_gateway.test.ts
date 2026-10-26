import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { setupNextHeadersMock, withTestHttpServer } from '../helpers/page-smoke'

const { setHeaders: inbound, setCookies } = setupNextHeadersMock()

const secret = 'test-gateway-secret-with-at-least-32-characters'
let upstreamBody: object

beforeEach(() => {
  upstreamBody = {}
  process.env.ADMIN_GATEWAY_SECRET = secret
  process.env.ADMIN_GATEWAY_REQUIRED = '1'
  vi.spyOn(console, 'warn').mockImplementation(() => {})
})

afterEach(() => {
  delete process.env.ADMIN_GATEWAY_SECRET
  delete process.env.ADMIN_GATEWAY_REQUIRED
  vi.restoreAllMocks()
})

describe('certificate gateway SSR trust', () => {
  it('forwards authenticated admin traffic on a non-loopback hostname', async () => {
    inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret })
    const { ssrUpstreamGet } = await import('@/lib/api')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        const response = await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })
        expect(response?.statusCode).toBe(200)
        expect(srv.lastRequest?.headers['x-admin-gateway-token']).toBe(secret)
        expect(srv.lastRequest?.headers.host).toBe('admin.example.com')
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBeUndefined()
      },
    )
  })

  it('rejects a forged credential even with a localhost Host', async () => {
    inbound({ host: 'localhost:3000', 'x-admin-gateway-token': 'forged' })
    const { ssrUpstreamGet } = await import('@/lib/api')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        expect(await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })).toBeNull()
        expect(srv.lastRequest).toBeUndefined()
      },
    )
  })

  it('does not prefetch admin data for a cross-site browser request', async () => {
    inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret, 'sec-fetch-site': 'cross-site' })
    const { ssrUpstreamGet } = await import('@/lib/api')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        expect(await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })).toBeNull()
        expect(srv.lastRequest).toBeUndefined()
      },
    )
  })

  it('disables the old localhost admin shortcut in required mode', async () => {
    inbound({ host: 'localhost:3000' })
    const { ssrUpstreamGet } = await import('@/lib/api')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        expect(await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })).toBeNull()
        expect(srv.lastRequest).toBeUndefined()
      },
    )
  })

  it('keeps public ingress traffic on the analyst path', async () => {
    inbound({ host: 'analyst.example.com', 'x-proxied-by-caddy': '1' })
    const { ssrUpstreamGet } = await import('@/lib/api')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        expect((await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' }))?.statusCode).toBe(200)
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBe('1')
        expect(srv.lastRequest?.headers['x-admin-gateway-token']).toBeUndefined()
        expect(srv.lastRequest?.headers.host).toBe('analyst.example.com')
      },
    )
  })

  it('never promotes a public marked request even if it carries a gateway token', async () => {
    inbound({ host: 'analyst.example.com', 'x-proxied-by-caddy': '1', 'x-admin-gateway-token': secret })
    const { ssrUpstreamGet } = await import('@/lib/api')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        expect(await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })).toBeNull()
        expect(srv.lastRequest).toBeUndefined()
      },
    )
  })

  it('does not let caller-supplied extraHeaders forge a gateway credential', async () => {
    inbound({ host: 'analyst.example.com', 'x-proxied-by-caddy': '1' })
    const { ssrUpstreamGet } = await import('@/lib/api')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        await ssrUpstreamGet({
          path: '/api/bootstrap',
          logPrefix: 'test',
          extraHeaders: { 'x-admin-gateway-token': secret },
        })
        expect(srv.lastRequest?.headers['x-admin-gateway-token']).toBeUndefined()
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBe('1')
      },
    )
  })

  it('never returns a shared admin bootstrap to an uncredentialed localhost request', async () => {
    const { fetchBootstrapServerSide } = await import('@/lib/ssr/upstream')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret, 'x-service-id': 'gateway-cache-test' })
        expect(await fetchBootstrapServerSide()).toEqual({})
        srv.clearRequests()
        inbound({ host: 'localhost:3000', 'x-service-id': 'gateway-cache-test' })
        expect(await fetchBootstrapServerSide()).toBeNull()
        expect(srv.lastRequest).toBeUndefined()
      },
    )
  })

  it('does not share public analyst bootstrap responses between requests', async () => {
    const { fetchBootstrapServerSide } = await import('@/lib/ssr/upstream')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        inbound({ host: 'analyst.example.com', 'x-proxied-by-caddy': '1', 'x-service-id': 'gateway-cache-test' })
        expect(await fetchBootstrapServerSide()).toEqual({})
        srv.clearRequests()
        expect(await fetchBootstrapServerSide()).toEqual({})
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBe('1')
      },
    )
  })

  it('bypasses the shared bootstrap cache for an analyst session cookie', async () => {
    const { fetchBootstrapServerSide } = await import('@/lib/ssr/upstream')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret, 'x-service-id': 'analyst-cookie-cache-test' })
        expect(await fetchBootstrapServerSide()).toEqual({})
        srv.clearRequests()
        setCookies(
          'analyst_session_id=test-session',
          (name: string) => (name === 'analyst_session_id' ? { value: 'test-session' } : undefined),
        )
        expect(await fetchBootstrapServerSide()).toEqual({})
        expect(srv.lastRequest?.headers).toEqual(
          expect.objectContaining({ cookie: 'analyst_session_id=test-session' }),
        )
        srv.clearRequests()
        setCookies('')
        expect(await fetchBootstrapServerSide()).toEqual({})
        expect(srv.lastRequest).toBeUndefined()
      },
    )
  })

  it('never caches a bootstrap response marked as analyst-scoped', async () => {
    const { fetchBootstrapServerSide } = await import('@/lib/ssr/upstream')
    await withTestHttpServer(
      (_req, res) => {
        res.end(JSON.stringify(upstreamBody))
      },
      async (srv) => {
        inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret, 'x-service-id': 'analyst-response-cache-test' })
        upstreamBody = { settings: { is_remote_analyst: true } }
        expect(await fetchBootstrapServerSide()).toEqual(upstreamBody)
        srv.clearRequests()
        upstreamBody = { settings: { is_remote_analyst: false } }
        expect(await fetchBootstrapServerSide()).toEqual(upstreamBody)
        expect(srv.lastRequest?.headers).toBeDefined()
      },
    )
  })
})

describe('fetchTosServerSide', () => {
  beforeEach(() => {
    delete process.env.ADMIN_GATEWAY_SECRET
    delete process.env.ADMIN_GATEWAY_REQUIRED
    setCookies('session=abc123')
    inbound({
      'x-proxied-by-caddy': 'true',
      host: 'fastly-log-analytics.global.ssl.fastly.net',
    })
  })

  it('returns null when API_PROXY_URL is unset (pure `next dev` outside docker compose)', async () => {
    const { fetchTosServerSide } = await import('@/lib/ssr/upstream')
    const out = await fetchTosServerSide()
    expect(out).toBeNull()
  })

  it('returns null on a non-2xx, non-401 upstream response', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { fetchTosServerSide } = await import('@/lib/ssr/upstream')
    const out = await fetchTosServerSide()
    expect(out).toBeNull()
    expect(warn).toHaveBeenCalled()
  })

  it("returns 'unauthenticated' on a 401 upstream response (analyst session missing/invalid)", async () => {
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 401
        res.end('{"detail":"unauthenticated"}')
      },
      async () => {
        const { fetchTosServerSide } = await import('@/lib/ssr/upstream')
        const out = await fetchTosServerSide()
        expect(out).toBe('unauthenticated')
      },
    )
  })

  it('returns null on a parse error from a malformed upstream body', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.end('not json {{{')
      },
      async () => {
        const { fetchTosServerSide } = await import('@/lib/ssr/upstream')
        const out = await fetchTosServerSide()
        expect(out).toBeNull()
        expect(warn).toHaveBeenCalled()
      },
    )
  })

  it('hits the upstream with the expected headers when Caddy marker is present', async () => {
    setCookies('session=xyz; theme=dark')
    inbound({
      'x-proxied-by-caddy': 'true',
      host: 'fastly-log-analytics.global.ssl.fastly.net',
    })

    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify({ version: 'v1', text: 'TOS body' }))
      },
      async (srv) => {
        const { fetchTosServerSide } = await import('@/lib/ssr/upstream')
        const out = await fetchTosServerSide()
        expect(out).toEqual({ version: 'v1', text: 'TOS body' })
        const capturedHeaders = srv.lastRequest!.headers
        expect(capturedHeaders['x-remote-analyst']).toBe('1')
        expect(capturedHeaders.host).toBe('fastly-log-analytics.global.ssl.fastly.net')
        expect(capturedHeaders.cookie).toBe('session=xyz; theme=dark')
      },
    )
  })

  it('fail-closed: non-loopback Host + no Caddy marker → null + warns', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    setCookies('')
    inbound({ host: 'fastly-log-analytics.global.ssl.fastly.net' })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { fetchTosServerSide } = await import('@/lib/ssr/upstream')
    const out = await fetchTosServerSide()
    expect(out).toBeNull()
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('no X-Proxied-By-Caddy marker'))
  })

  it('admin SSH-tunnel: loopback Host + no Caddy marker → forwards as admin (no X-Remote-Analyst)', async () => {
    setCookies('')
    inbound({ host: 'localhost:3001' })

    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify({ version: 'v1', text: 'TOS body' }))
      },
      async (srv) => {
        const { fetchTosServerSide } = await import('@/lib/ssr/upstream')
        const out = await fetchTosServerSide()
        expect(out).toEqual({ version: 'v1', text: 'TOS body' })
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBeUndefined()
      },
    )
  })

  it('fail-closed: no Caddy marker + absent Host → null (drift / SSRF without a loopback Host)', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    setCookies('')
    inbound({})
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { fetchTosServerSide } = await import('@/lib/ssr/upstream')
    const out = await fetchTosServerSide()
    expect(out).toBeNull()
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('no X-Proxied-By-Caddy marker'))
  })
})

describe('fetchCronRunsServerSide (logs SSR)', () => {
  beforeEach(() => {
    delete process.env.ADMIN_GATEWAY_SECRET
    delete process.env.ADMIN_GATEWAY_REQUIRED
  })

  it('returns null without a serviceId', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    const { fetchCronRunsServerSide } = await import('@/lib/ssr/upstream')
    expect(await fetchCronRunsServerSide(undefined)).toBeNull()
  })

  it('fail-closed: no Caddy marker + non-loopback Host → null, never forwarded', async () => {
    inbound({ host: 'fastly-log-analytics.global.ssl.fastly.net' })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.end('{"rows":[]}')
      },
      async (srv) => {
        const { fetchCronRunsServerSide } = await import('@/lib/ssr/upstream')
        const out = await fetchCronRunsServerSide('svc-1')
        expect(out).toBeNull()
        expect(srv.requests).toHaveLength(0)
        expect(warn).toHaveBeenCalledWith(expect.stringContaining('non-loopback/absent Host'))
      },
    )
  })

  it('admin SSH-tunnel (loopback Host, no marker): forwards as admin with service id', async () => {
    inbound({ host: 'localhost:3001' })
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('{"rows":[1,2,3]}')
      },
      async (srv) => {
        const { fetchCronRunsServerSide } = await import('@/lib/ssr/upstream')
        const out = await fetchCronRunsServerSide('svc-1')
        expect(out).toEqual({ rows: [1, 2, 3] })
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBeUndefined()
        expect(srv.lastRequest?.headers['x-fastly-service-id']).toBe('svc-1')
      },
    )
  })

  it('analyst (Caddy marker): forwards as remote-analyst', async () => {
    inbound({
      'x-proxied-by-caddy': 'true',
      host: 'fastly-log-analytics.global.ssl.fastly.net',
    })
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('{"rows":[1,2,3]}')
      },
      async (srv) => {
        const { fetchCronRunsServerSide } = await import('@/lib/ssr/upstream')
        const out = await fetchCronRunsServerSide('svc-1')
        expect(out).toEqual({ rows: [1, 2, 3] })
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBe('1')
        expect(srv.lastRequest?.headers['x-fastly-service-id']).toBe('svc-1')
      },
    )
  })
})

describe('fetchUsageLogServerSide (usage-log SSR)', () => {
  const args = { serviceId: 'svc-1', start: '2026-06-01', end: '2026-06-17', pageSize: 50 }

  beforeEach(() => {
    delete process.env.ADMIN_GATEWAY_SECRET
    delete process.env.ADMIN_GATEWAY_REQUIRED
  })

  it('fail-closed: no Caddy marker + non-loopback Host → null, never forwarded', async () => {
    inbound({ host: 'fastly-log-analytics.global.ssl.fastly.net' })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.end('{"rows":[]}')
      },
      async (srv) => {
        const { fetchUsageLogServerSide } = await import('@/lib/ssr/upstream')
        const out = await fetchUsageLogServerSide(args)
        expect(out).toBeNull()
        expect(srv.requests).toHaveLength(0)
        expect(warn).toHaveBeenCalledWith(expect.stringContaining('non-loopback/absent Host'))
      },
    )
  })

  it('admin SSH-tunnel (loopback Host, no marker): forwards as admin with the query string', async () => {
    inbound({ host: 'localhost:3001' })
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('{"rows":[1]}')
      },
      async (srv) => {
        const { fetchUsageLogServerSide } = await import('@/lib/ssr/upstream')
        const out = await fetchUsageLogServerSide(args)
        expect(out).toEqual({ rows: [1] })
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBeUndefined()
        expect(srv.lastRequest?.url).toContain('/api/admin/usage-log?')
        expect(srv.lastRequest?.url).toContain('service_id=svc-1')
      },
    )
  })
})

describe('ssrUpstreamGet — debug-responses cookie propagation', () => {
  beforeEach(() => {
    delete process.env.ADMIN_GATEWAY_SECRET
    delete process.env.ADMIN_GATEWAY_REQUIRED
  })

  it('cookie=1 + loopback Host (admin branch) → upstream request carries x-debug-responses: 1', async () => {
    setCookies('fla.debugResponses=1', (k: string) => (k === 'fla.debugResponses' ? { value: '1' } : undefined))
    inbound({ host: 'localhost:3001' })
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('{}')
      },
      async (srv) => {
        const { ssrUpstreamGet } = await import('@/lib/api')
        const out = await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })
        expect(out).not.toBeNull()
        expect(srv.lastRequest?.headers['x-debug-responses']).toBe('1')
      },
    )
  })

  it('cookie=1 + Caddy marker (analyst branch) → header NOT sent', async () => {
    setCookies('fla.debugResponses=1', (k: string) => (k === 'fla.debugResponses' ? { value: '1' } : undefined))
    inbound({ 'x-proxied-by-caddy': 'true', host: 'fastly-log-analytics.global.ssl.fastly.net' })
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('{}')
      },
      async (srv) => {
        const { ssrUpstreamGet } = await import('@/lib/api')
        const out = await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })
        expect(out).not.toBeNull()
        expect(srv.lastRequest?.headers['x-debug-responses']).toBeUndefined()
      },
    )
  })

  it('cookie absent → header not sent (unchanged from before this feature existed)', async () => {
    setCookies('')
    inbound({ host: 'localhost:3001' })
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('{}')
      },
      async (srv) => {
        const { ssrUpstreamGet } = await import('@/lib/api')
        const out = await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })
        expect(out).not.toBeNull()
        expect(srv.lastRequest?.headers['x-debug-responses']).toBeUndefined()
      },
    )
  })

  it('cookie=0 → header not sent', async () => {
    setCookies('fla.debugResponses=0', (k: string) => (k === 'fla.debugResponses' ? { value: '0' } : undefined))
    inbound({ host: 'localhost:3001' })
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('{}')
      },
      async (srv) => {
        const { ssrUpstreamGet } = await import('@/lib/api')
        const out = await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })
        expect(out).not.toBeNull()
        expect(srv.lastRequest?.headers['x-debug-responses']).toBeUndefined()
      },
    )
  })

  it('disables keep-alive socket reuse (connection: close) and retries once on ECONNRESET', async () => {
    let attempts = 0
    let secondConnHeader: string | string[] | undefined
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    setCookies('')
    inbound({ host: 'localhost:3001' })
    await withTestHttpServer(
      (req, res) => {
        attempts += 1
        if (attempts === 1) {
          req.socket.destroy()
          return
        }
        secondConnHeader = req.headers.connection
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('{"recovered":true}')
      },
      async () => {
        const { ssrUpstreamGet } = await import('@/lib/api')
        const out = await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })
        expect(out).not.toBeNull()
        expect(out?.statusCode).toBe(200)
        expect(out?.body).toBe('{"recovered":true}')
        expect(attempts).toBe(2)
        expect(secondConnHeader).toBe('close')
        expect(warnSpy).not.toHaveBeenCalled()
      },
    )
  })
})
