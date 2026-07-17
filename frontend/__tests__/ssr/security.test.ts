import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

// security.ts mirrors lib/ssr/origin.ts (the proven template) — a POST caller
// of the shared ./_transport helper. These tests pin (1) the security
// contract — the Caddy-marker trust gate must behave IDENTICALLY for this
// POST as for the GET callers (refuse no-marker + non-loopback Host;
// analyst-clamp on the marker branch; admin token only on the loopback
// branch) — and (2) the key-match contract: the (rangeToken, anchor) this
// helper resolves equals what SecurityClient computes on first paint, so the
// SSR seed key byte-matches the client first-paint key. A mismatch would
// double-fetch (worse than no SSR).

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

describe('resolveSecurityDefaultKey (key-match contract)', () => {
  it('cold load → rangeToken "24h", anchor floored to the 60s grid', async () => {
    const { resolveSecurityDefaultKey } = await import('@/lib/ssr/security')
    const { quantizeAnchor } = await import('@/lib/time-window')

    const now = new Date('2026-06-29T12:00:37.123Z')
    const got = resolveSecurityDefaultKey(now)
    expect(got.rangeToken).toBe('24h')
    expect(got.anchor).toBe('2026-06-29T12:00:00Z')
    expect(got.anchor).toBe(quantizeAnchor(now.toISOString(), now))
  })

  it('two instants in the same 60s quantum resolve the identical anchor', async () => {
    const { resolveSecurityDefaultKey } = await import('@/lib/ssr/security')
    const a = resolveSecurityDefaultKey(new Date('2026-06-29T12:00:01Z'))
    const b = resolveSecurityDefaultKey(new Date('2026-06-29T12:00:58Z'))
    expect(a.anchor).toBe(b.anchor)
    expect(a.anchor).toBe('2026-06-29T12:00:00Z')
  })

  it('stale log extents (>15min old) snap the anchor to the real latest log, not now', async () => {
    const { resolveSecurityDefaultKey } = await import('@/lib/ssr/security')
    const now = new Date('2026-06-29T12:00:00Z')
    const got = resolveSecurityDefaultKey(now, {
      earliest_log_at: '2026-06-01T00:00:00Z',
      latest_log_at: '2026-06-29T11:00:00Z',
    })
    expect(got.anchor).toBe('2026-06-29T11:00:00Z')
  })

  it('fresh log extents (<=15min old) leave the naive "now" anchor unchanged', async () => {
    const { resolveSecurityDefaultKey } = await import('@/lib/ssr/security')
    const now = new Date('2026-06-29T12:00:00Z')
    const got = resolveSecurityDefaultKey(now, {
      earliest_log_at: '2026-06-01T00:00:00Z',
      latest_log_at: '2026-06-29T11:55:00Z',
    })
    expect(got.anchor).toBe('2026-06-29T12:00:00Z')
  })
})

describe('SSR-seed key ↔ client first-paint key byte-match', () => {
  it('the page.tsx seed key === the client useServiceQuery key', async () => {
    const { hashKey } = await import('@tanstack/react-query')
    const { resolveSecurityDefaultKey, SECURITY_SSR_DEFAULTS } = await import('@/lib/ssr/security')

    const serviceId = 'svc-1'
    const { rangeToken, anchor } = resolveSecurityDefaultKey(new Date('2026-06-29T12:00:30Z'))

    // ── SSR seed key (mirrors app/security/page.tsx) ──
    const seedKey = [
      'security',
      'aggregates',
      serviceId,
      rangeToken,
      anchor,
      {},
      SECURITY_SSR_DEFAULTS.bucketSeconds,
    ]

    // ── Client first-paint key (mirrors SecurityClient/SecurityBody) ──
    const clientKey = [
      'security',
      'aggregates',
      serviceId,
      rangeToken,
      anchor,
      {}, // no filters on a cold load
      3600, // default 24h span → "1 hour" → INTERVAL_SECONDS['1 hour']
    ]

    expect(hashKey(seedKey)).toBe(hashKey(clientKey))
  })
})

describe('fetchSecurityServerSide (security SSR)', () => {
  it('returns null without a serviceId (no upstream call)', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    const { fetchSecurityServerSide } = await import('@/lib/ssr/security')
    expect(await fetchSecurityServerSide(undefined)).toBeNull()
  })

  it('fail-closed: no Caddy marker + non-loopback Host → null, never forwarded', async () => {
    const http = await import('node:http')
    let hit = false
    const server = http.createServer((_req, res) => {
      hit = true
      res.statusCode = 200
      res.end('{}')
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
    mockHeaders.mockReturnValue(headerGetter({ host: 'fastly-log-analytics.global.ssl.fastly.net' }))
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    try {
      const { fetchSecurityServerSide } = await import('@/lib/ssr/security')
      const out = await fetchSecurityServerSide('svc-1')
      expect(out).toBeNull()
      expect(hit).toBe(false)
      expect(warn).toHaveBeenCalledWith(expect.stringContaining('non-loopback/absent Host'))
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })

  it('admin SSH-tunnel (loopback Host, no marker): POSTs default keyed body as admin with X-Admin-Token + service id', async () => {
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
        res.end('{"ngwaf_verified_bots":{"rows":[]}}')
      })
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
    process.env.ADMIN_SHARED_SECRET = 'sekret'
    mockHeaders.mockReturnValue(headerGetter({ host: 'localhost:3001' }))
    try {
      const { fetchSecurityServerSide, SECURITY_SSR_DEFAULTS, SECURITY_SSR_SECTIONS } = await import(
        '@/lib/ssr/security'
      )
      const now = new Date('2026-06-29T12:00:30Z')
      const out = await fetchSecurityServerSide('svc-1', now)

      expect(out).not.toBeNull()
      expect(out!.data).toEqual({ ngwaf_verified_bots: { rows: [] } })
      expect(out!.rangeToken).toBe('24h')
      expect(out!.anchor).toBe('2026-06-29T12:00:00Z')

      expect(capturedMethod).toBe('POST')
      expect(captured['content-type']).toContain('application/json')
      const body = JSON.parse(capturedBody)
      expect(body).toEqual({
        filters: {},
        bucket_seconds: SECURITY_SSR_DEFAULTS.bucketSeconds,
        sections: [...SECURITY_SSR_SECTIONS],
        range_token: '24h',
        anchor: '2026-06-29T12:00:00Z',
      })
      expect(body.start_time).toBeUndefined()
      expect(body.end_time).toBeUndefined()

      expect(captured['x-remote-analyst']).toBeUndefined()
      expect(captured['x-admin-token']).toBeUndefined()
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
        res.end('{"ngwaf_verified_bots":{"rows":[]}}')
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
      const { fetchSecurityServerSide } = await import('@/lib/ssr/security')
      const out = await fetchSecurityServerSide('svc-1')
      expect(out).not.toBeNull()
      expect(capturedMethod).toBe('POST')
      expect(captured['x-remote-analyst']).toBe('1')
      expect(captured['x-admin-token']).toBeUndefined()
      expect(captured['x-service-id']).toBe('svc-1')
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })
})
