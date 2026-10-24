import { describe, expect, it, vi } from 'vitest'

import { setupNextHeadersMock, withTestHttpServer } from '../helpers/ssr'

// logs.ts + usage_log.ts were migrated onto the shared ./_transport helper
// (finding-015). These tests pin the security contract for the migrated
// callers: the Caddy-marker trust gate forwards an analyst request (marker
// present) or the admin SSH-tunnel (loopback Host, no marker), but refuses a
// no-marker request whose Host is non-loopback or absent (Caddy drift / SSRF).

const { setHeaders } = setupNextHeadersMock()

describe('fetchCronRunsServerSide (logs SSR)', () => {
  it('returns null without a serviceId', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    const { fetchCronRunsServerSide } = await import('@/lib/ssr/logs')
    expect(await fetchCronRunsServerSide(undefined)).toBeNull()
  })

  it('fail-closed: no Caddy marker + non-loopback Host → null, never forwarded', async () => {
    setHeaders({ host: 'fastly-log-analytics.global.ssl.fastly.net' })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.end('{"rows":[]}')
      },
      async (srv) => {
        const { fetchCronRunsServerSide } = await import('@/lib/ssr/logs')
        const out = await fetchCronRunsServerSide('svc-1')
        expect(out).toBeNull()
        expect(srv.requests).toHaveLength(0)
        expect(warn).toHaveBeenCalledWith(expect.stringContaining('non-loopback/absent Host'))
      },
    )
  })

  it('admin SSH-tunnel (loopback Host, no marker): forwards as admin with service id', async () => {
    setHeaders({ host: 'localhost:3001' })
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('{"rows":[1,2,3]}')
      },
      async (srv) => {
        const { fetchCronRunsServerSide } = await import('@/lib/ssr/logs')
        const out = await fetchCronRunsServerSide('svc-1')
        expect(out).toEqual({ rows: [1, 2, 3] })
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBeUndefined() // admin path
        expect(srv.lastRequest?.headers['x-fastly-service-id']).toBe('svc-1')
      },
    )
  })

  it('analyst (Caddy marker): forwards as remote-analyst', async () => {
    setHeaders({
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
        const { fetchCronRunsServerSide } = await import('@/lib/ssr/logs')
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

  it('fail-closed: no Caddy marker + non-loopback Host → null, never forwarded', async () => {
    setHeaders({ host: 'fastly-log-analytics.global.ssl.fastly.net' })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.end('{"rows":[]}')
      },
      async (srv) => {
        const { fetchUsageLogServerSide } = await import('@/lib/ssr/usage_log')
        const out = await fetchUsageLogServerSide(args)
        expect(out).toBeNull()
        expect(srv.requests).toHaveLength(0)
        expect(warn).toHaveBeenCalledWith(expect.stringContaining('non-loopback/absent Host'))
      },
    )
  })

  it('admin SSH-tunnel (loopback Host, no marker): forwards as admin with the query string', async () => {
    setHeaders({ host: 'localhost:3001' })
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('{"rows":[1]}')
      },
      async (srv) => {
        const { fetchUsageLogServerSide } = await import('@/lib/ssr/usage_log')
        const out = await fetchUsageLogServerSide(args)
        expect(out).toEqual({ rows: [1] })
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBeUndefined()
        expect(srv.lastRequest?.url).toContain('/api/admin/usage-log?')
        expect(srv.lastRequest?.url).toContain('service_id=svc-1')
      },
    )
  })
})
