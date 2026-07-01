import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

// insights.ts is the FIRST POST caller of the shared ./_transport helper.
// These tests pin (1) the security contract — the Caddy-marker trust gate must
// behave IDENTICALLY for POST as for the GET callers (refuse no-marker +
// non-loopback Host; analyst-clamp on the marker branch; admin token only on
// the loopback branch) — and (2) the key-match contract: the windowHours/
// baselineHours the helper resolves equal what the client's useInsightsDefaults
// computes from the same earliest_log_at, so the SSR seed key byte-matches the
// client first-paint key. A mismatch would double-fetch (worse than no SSR).

const mockCookies = vi.fn()
const mockHeaders = vi.fn()
vi.mock('next/headers', () => ({
  cookies: () => mockCookies(),
  headers: () => mockHeaders(),
}))

beforeEach(() => {
  mockCookies.mockReturnValue({ toString: () => '' })
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

describe('resolveInsightsDefault (key-match contract)', () => {
  it('mirrors useInsightsDefaults: pickInsightsDefault over earliest_log_at', async () => {
    const { resolveInsightsDefault } = await import('@/lib/ssr/insights')
    const { pickInsightsDefault, historyHoursFromExtents } = await import(
      '@/lib/insights-defaults'
    )

    // ~10 days of history → the client's adaptive default is window '1' / baseline '168'.
    const earliest = new Date(Date.now() - 10 * 24 * 3600 * 1000).toISOString()
    const expected = pickInsightsDefault(historyHoursFromExtents(earliest))
    const got = resolveInsightsDefault(earliest)
    expect(got).toEqual({
      windowHours: expected.window,
      baselineHours: expected.baseline,
    })
  })

  it('no extents → STATIC_DEFAULT tokens (matches client no-data path)', async () => {
    const { resolveInsightsDefault } = await import('@/lib/ssr/insights')
    const { STATIC_DEFAULT } = await import('@/lib/insights-defaults')
    expect(resolveInsightsDefault(null)).toEqual({
      windowHours: STATIC_DEFAULT.window,
      baselineHours: STATIC_DEFAULT.baseline,
    })
    expect(resolveInsightsDefault(undefined)).toEqual({
      windowHours: STATIC_DEFAULT.window,
      baselineHours: STATIC_DEFAULT.baseline,
    })
  })
})

describe('fetchInsightsServerSide (insights SSR)', () => {
  it('returns null without a serviceId (no upstream call)', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    const { fetchInsightsServerSide } = await import('@/lib/ssr/insights')
    expect(await fetchInsightsServerSide(undefined, null)).toBeNull()
  })

  it('fail-closed: no Caddy marker + non-loopback Host → null, never forwarded', async () => {
    const http = await import('node:http')
    let hit = false
    const server = http.createServer((_req, res) => {
      hit = true
      res.statusCode = 200
      res.end('{"insights":[]}')
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
    mockHeaders.mockReturnValue(headerGetter({ host: 'fastly-log-analytics.global.ssl.fastly.net' }))
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    try {
      const { fetchInsightsServerSide } = await import('@/lib/ssr/insights')
      const out = await fetchInsightsServerSide('svc-1', null)
      expect(out).toBeNull()
      expect(hit).toBe(false)
      expect(warn).toHaveBeenCalledWith(expect.stringContaining('non-loopback/absent Host'))
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })

  it('admin SSH-tunnel (loopback Host, no marker): POSTs default body as admin with X-Admin-Token + service id', async () => {
    const http = await import('node:http')
    let captured: Record<string, string | string[] | undefined> = {}
    let capturedMethod = ''
    let capturedBody = ''
    const server = http.createServer((req, res) => {
      captured = req.headers
      capturedMethod = req.method ?? ''
      const chunks: Buffer[] = []
      req.on('data', (c: Buffer) => chunks.push(c))
      req.on('end', () => {
        capturedBody = Buffer.concat(chunks).toString('utf8')
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('{"insights":[1,2,3]}')
      })
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
    process.env.ADMIN_SHARED_SECRET = 'sekret'
    mockHeaders.mockReturnValue(headerGetter({ host: 'localhost:3001' }))
    try {
      const { fetchInsightsServerSide } = await import('@/lib/ssr/insights')
      // ~10 days of history → default tokens window '1' / baseline '168'.
      const earliest = new Date(Date.now() - 10 * 24 * 3600 * 1000).toISOString()
      const out = await fetchInsightsServerSide('svc-1', { earliest_log_at: earliest })

      expect(out).not.toBeNull()
      expect(out!.data).toEqual({ insights: [1, 2, 3] })
      expect(out!.windowHours).toBe('1')
      expect(out!.baselineHours).toBe('168')

      // POST with the exact client body shape.
      expect(capturedMethod).toBe('POST')
      expect(captured['content-type']).toContain('application/json')
      expect(JSON.parse(capturedBody)).toEqual({
        window_size_hrs: 1,
        baseline_hours: 168,
        filters: {},
      })

      // Admin trust path: no analyst header, admin token present, service id passed.
      expect(captured['x-remote-analyst']).toBeUndefined()
      expect(captured['x-admin-token']).toBe('sekret')
      expect(captured['x-service-id']).toBe('svc-1')
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })

  it('analyst (Caddy marker): POSTs as remote-analyst, NO admin token', async () => {
    const http = await import('node:http')
    let captured: Record<string, string | string[] | undefined> = {}
    let capturedMethod = ''
    const server = http.createServer((req, res) => {
      captured = req.headers
      capturedMethod = req.method ?? ''
      const chunks: Buffer[] = []
      req.on('data', (c: Buffer) => chunks.push(c))
      req.on('end', () => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('{"insights":[1]}')
      })
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
    process.env.ADMIN_SHARED_SECRET = 'sekret'
    mockHeaders.mockReturnValue(
      headerGetter({ 'x-proxied-by-caddy': 'true', host: 'fastly-log-analytics.global.ssl.fastly.net' }),
    )
    try {
      const { fetchInsightsServerSide } = await import('@/lib/ssr/insights')
      const out = await fetchInsightsServerSide('svc-1', null)
      expect(out).not.toBeNull()
      expect(out!.data).toEqual({ insights: [1] })
      expect(capturedMethod).toBe('POST')
      // Analyst trust path: remote-analyst stamped, admin token NEVER on this branch.
      expect(captured['x-remote-analyst']).toBe('1')
      expect(captured['x-admin-token']).toBeUndefined()
      expect(captured['host']).toBe('fastly-log-analytics.global.ssl.fastly.net')
      expect(captured['x-service-id']).toBe('svc-1')
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })
})
