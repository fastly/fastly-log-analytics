import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

// logs.ts + usage_log.ts were migrated onto the shared ./_transport helper
// (finding-015). These tests pin the security contract for the migrated
// callers: the Caddy-marker trust gate forwards an analyst request (marker
// present) or the admin SSH-tunnel (loopback Host, no marker), but refuses a
// no-marker request whose Host is non-loopback or absent (Caddy drift / SSRF).

const mockCookies = vi.fn()
const mockHeaders = vi.fn()
vi.mock('next/headers', () => ({
  cookies: () => mockCookies(),
  headers: () => mockHeaders(),
}))

beforeEach(() => {
  mockCookies.mockReturnValue({ toString: () => '', get: () => undefined })
  mockHeaders.mockReturnValue({ get: (_k: string) => null })
})

afterEach(() => {
  mockCookies.mockReset()
  mockHeaders.mockReset()
  delete process.env.API_PROXY_URL
  delete process.env.ADMIN_SHARED_SECRET
})

function headerGetter(entries: Record<string, string>) {
  return {
    get: (k: string) => entries[k.toLowerCase()] ?? null,
  }
}

describe('fetchCronRunsServerSide (logs SSR)', () => {
  it('returns null without a serviceId', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    const { fetchCronRunsServerSide } = await import('@/lib/ssr/logs')
    expect(await fetchCronRunsServerSide(undefined)).toBeNull()
  })

  it('fail-closed: no Caddy marker + non-loopback Host → null, never forwarded', async () => {
    const http = await import('node:http')
    let hit = false
    const server = http.createServer((_req, res) => {
      hit = true
      res.statusCode = 200
      res.end('{"rows":[]}')
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
    mockHeaders.mockReturnValue(headerGetter({ host: 'fastly-log-analytics.global.ssl.fastly.net' }))
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    try {
      const { fetchCronRunsServerSide } = await import('@/lib/ssr/logs')
      const out = await fetchCronRunsServerSide('svc-1')
      expect(out).toBeNull()
      expect(hit).toBe(false)
      expect(warn).toHaveBeenCalledWith(expect.stringContaining('non-loopback/absent Host'))
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })

  it('admin SSH-tunnel (loopback Host, no marker): forwards as admin with X-Admin-Token + service id', async () => {
    const http = await import('node:http')
    let captured: Record<string, string | string[] | undefined> = {}
    const server = http.createServer((req, res) => {
      captured = req.headers
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/json')
      res.end('{"rows":[1,2,3]}')
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
    process.env.ADMIN_SHARED_SECRET = 'sekret'
    mockHeaders.mockReturnValue(headerGetter({ host: 'localhost:3001' }))
    try {
      const { fetchCronRunsServerSide } = await import('@/lib/ssr/logs')
      const out = await fetchCronRunsServerSide('svc-1')
      expect(out).toEqual({ rows: [1, 2, 3] })
      expect(captured['x-remote-analyst']).toBeUndefined() // admin path
      expect(captured['x-admin-token']).toBe('sekret')
      expect(captured['x-fastly-service-id']).toBe('svc-1')
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })

  it('analyst (Caddy marker): forwards as remote-analyst, no admin token', async () => {
    const http = await import('node:http')
    let captured: Record<string, string | string[] | undefined> = {}
    const server = http.createServer((req, res) => {
      captured = req.headers
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/json')
      res.end('{"rows":[1,2,3]}')
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
    process.env.ADMIN_SHARED_SECRET = 'sekret'
    mockHeaders.mockReturnValue(
      headerGetter({ 'x-proxied-by-caddy': 'true', host: 'fastly-log-analytics.global.ssl.fastly.net' }),
    )
    try {
      const { fetchCronRunsServerSide } = await import('@/lib/ssr/logs')
      const out = await fetchCronRunsServerSide('svc-1')
      expect(out).toEqual({ rows: [1, 2, 3] })
      expect(captured['x-remote-analyst']).toBe('1')
      expect(captured['x-admin-token']).toBeUndefined() // never on the analyst branch
      expect(captured['x-fastly-service-id']).toBe('svc-1')
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })
})

describe('fetchUsageLogServerSide (usage-log SSR)', () => {
  const args = { serviceId: 'svc-1', start: '2026-06-01', end: '2026-06-17', pageSize: 50 }

  it('fail-closed: no Caddy marker + non-loopback Host → null, never forwarded', async () => {
    const http = await import('node:http')
    let hit = false
    const server = http.createServer((_req, res) => {
      hit = true
      res.statusCode = 200
      res.end('{"rows":[]}')
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
    mockHeaders.mockReturnValue(headerGetter({ host: 'fastly-log-analytics.global.ssl.fastly.net' }))
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    try {
      const { fetchUsageLogServerSide } = await import('@/lib/ssr/usage_log')
      const out = await fetchUsageLogServerSide(args)
      expect(out).toBeNull()
      expect(hit).toBe(false)
      expect(warn).toHaveBeenCalledWith(expect.stringContaining('non-loopback/absent Host'))
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })

  it('admin SSH-tunnel (loopback Host, no marker): forwards as admin with the query string', async () => {
    const http = await import('node:http')
    let capturedUrl = ''
    let captured: Record<string, string | string[] | undefined> = {}
    const server = http.createServer((req, res) => {
      capturedUrl = req.url ?? ''
      captured = req.headers
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/json')
      res.end('{"rows":[1]}')
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
    process.env.ADMIN_SHARED_SECRET = 'sekret'
    mockHeaders.mockReturnValue(headerGetter({ host: 'localhost:3001' }))
    try {
      const { fetchUsageLogServerSide } = await import('@/lib/ssr/usage_log')
      const out = await fetchUsageLogServerSide(args)
      expect(out).toEqual({ rows: [1] })
      expect(captured['x-remote-analyst']).toBeUndefined()
      expect(captured['x-admin-token']).toBe('sekret')
      expect(capturedUrl).toContain('/api/admin/usage-log?')
      expect(capturedUrl).toContain('service_id=svc-1')
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })
})
