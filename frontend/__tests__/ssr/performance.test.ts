import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

// performance.ts fetches TWO section groups (core + distributions) per cold
// load, sharing one (rangeToken, anchor) pair. These tests pin (1) the
// security contract — the Caddy-marker trust gate must behave IDENTICALLY as
// the other POST SSR callers (refuse no-marker + non-loopback Host;
// analyst-clamp on the marker branch; admin token only on the loopback
// branch) — and (2) the key-match contract: the (rangeToken, anchor) this
// helper resolves equals what PerformanceClient computes on first paint for
// BOTH keys, so the SSR seed byte-matches. A mismatch would double-fetch
// (worse than no SSR).

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

describe('resolvePerformanceDefaultKey (key-match contract)', () => {
  it('cold load → rangeToken "24h", anchor floored to the 60s grid', async () => {
    const { resolvePerformanceDefaultKey } = await import('@/lib/ssr/performance')
    const { quantizeAnchor } = await import('@/lib/time-window')

    const now = new Date('2026-06-29T12:00:37.123Z')
    const got = resolvePerformanceDefaultKey(now)
    expect(got.rangeToken).toBe('24h')
    expect(got.anchor).toBe('2026-06-29T12:00:00Z')
    expect(got.anchor).toBe(quantizeAnchor(now.toISOString(), now))
  })

  it('stale log extents (>15min old) snap the anchor to the real latest log, not now', async () => {
    const { resolvePerformanceDefaultKey } = await import('@/lib/ssr/performance')
    const now = new Date('2026-06-29T12:00:00Z')
    const got = resolvePerformanceDefaultKey(now, {
      earliest_log_at: '2026-06-01T00:00:00Z',
      latest_log_at: '2026-06-29T11:00:00Z',
    })
    expect(got.anchor).toBe('2026-06-29T11:00:00Z')
  })

  it('fresh log extents (<=15min old) leave the naive "now" anchor unchanged', async () => {
    const { resolvePerformanceDefaultKey } = await import('@/lib/ssr/performance')
    const now = new Date('2026-06-29T12:00:00Z')
    const got = resolvePerformanceDefaultKey(now, {
      earliest_log_at: '2026-06-01T00:00:00Z',
      latest_log_at: '2026-06-29T11:55:00Z',
    })
    expect(got.anchor).toBe('2026-06-29T12:00:00Z')
  })
})

describe('SSR-seed keys ↔ client first-paint keys byte-match', () => {
  it('both the core and distributions page.tsx seed keys === the client keys', async () => {
    const { hashKey } = await import('@tanstack/react-query')
    const { resolvePerformanceDefaultKey, PERFORMANCE_SSR_DEFAULTS } = await import('@/lib/ssr/performance')

    const serviceId = 'svc-1'
    const { rangeToken, anchor } = resolvePerformanceDefaultKey(new Date('2026-06-29T12:00:30Z'))

    const coreSeedKey = ['performance', 'aggregates', 'core', serviceId, rangeToken, anchor, {}, PERFORMANCE_SSR_DEFAULTS.sortBy]
    const coreClientKey = ['performance', 'aggregates', 'core', serviceId, rangeToken, anchor, {}, 'p99']
    expect(hashKey(coreSeedKey)).toBe(hashKey(coreClientKey))

    const distSeedKey = ['performance', 'aggregates', 'distributions', serviceId, rangeToken, anchor, {}, PERFORMANCE_SSR_DEFAULTS.sortBy]
    const distClientKey = ['performance', 'aggregates', 'distributions', serviceId, rangeToken, anchor, {}, 'p99']
    expect(hashKey(distSeedKey)).toBe(hashKey(distClientKey))
  })
})

describe('fetchPerformanceServerSide (performance SSR)', () => {
  it('returns null without a serviceId (no upstream call)', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    const { fetchPerformanceServerSide } = await import('@/lib/ssr/performance')
    expect(await fetchPerformanceServerSide(undefined)).toBeNull()
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
      const { fetchPerformanceServerSide } = await import('@/lib/ssr/performance')
      const out = await fetchPerformanceServerSide('svc-1')
      expect(out).toBeNull()
      expect(hit).toBe(false)
      expect(warn).toHaveBeenCalled()
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })

  it('admin SSH-tunnel: POSTs both core + distributions keyed bodies as admin with X-Admin-Token + service id', async () => {
    const http = await import('node:http')
    const capturedBodies: string[] = []
    const capturedHeaders: Array<Record<string, string | string[] | undefined>> = []
    const server = http.createServer((req, res) => {
      capturedHeaders.push(req.headers)
      const chunks: Buffer[] = []
      req.on('data', (c: Buffer) => chunks.push(c))
      req.on('end', () => {
        capturedBodies.push(Buffer.concat(chunks).toString('utf8'))
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('{"ok":true}')
      })
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
    process.env.ADMIN_SHARED_SECRET = 'sekret'
    mockHeaders.mockReturnValue(headerGetter({ host: 'localhost:3001' }))
    try {
      const {
        fetchPerformanceServerSide,
        PERFORMANCE_SSR_DEFAULTS,
        PERFORMANCE_CORE_SSR_SECTIONS,
        PERFORMANCE_DISTRIBUTIONS_SSR_SECTIONS,
      } = await import('@/lib/ssr/performance')
      const now = new Date('2026-06-29T12:00:30Z')
      const out = await fetchPerformanceServerSide('svc-1', now)

      expect(out).not.toBeNull()
      expect(out!.coreData).toEqual({ ok: true })
      expect(out!.distributionsData).toEqual({ ok: true })
      expect(out!.rangeToken).toBe('24h')
      expect(out!.anchor).toBe('2026-06-29T12:00:00Z')

      expect(capturedBodies).toHaveLength(2)
      const bodies = capturedBodies.map((b) => JSON.parse(b))
      const coreBody = bodies.find((b) => b.sections[0] === PERFORMANCE_CORE_SSR_SECTIONS[0])
      const distBody = bodies.find((b) => b.sections[0] === PERFORMANCE_DISTRIBUTIONS_SSR_SECTIONS[0])
      expect(coreBody).toEqual({
        filters: {},
        sort_by: PERFORMANCE_SSR_DEFAULTS.sortBy,
        sections: [...PERFORMANCE_CORE_SSR_SECTIONS],
        range_token: '24h',
        anchor: '2026-06-29T12:00:00Z',
      })
      expect(distBody).toEqual({
        filters: {},
        sort_by: PERFORMANCE_SSR_DEFAULTS.sortBy,
        sections: [...PERFORMANCE_DISTRIBUTIONS_SSR_SECTIONS],
        range_token: '24h',
        anchor: '2026-06-29T12:00:00Z',
      })

      for (const h of capturedHeaders) {
        expect(h['x-remote-analyst']).toBeUndefined()
        expect(h['x-admin-token']).toBe('sekret')
        expect(h['x-service-id']).toBe('svc-1')
      }
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })

  it('all-or-nothing: if either sub-fetch fails, returns null (no partial seed)', async () => {
    const http = await import('node:http')
    let calls = 0
    const server = http.createServer((req, res) => {
      calls += 1
      const chunks: Buffer[] = []
      req.on('data', (c: Buffer) => chunks.push(c))
      req.on('end', () => {
        // Fail the second request only.
        if (calls === 2) {
          res.statusCode = 500
          res.end('boom')
          return
        }
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('{"ok":true}')
      })
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
    process.env.ADMIN_SHARED_SECRET = 'sekret'
    mockHeaders.mockReturnValue(headerGetter({ host: 'localhost:3001' }))
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    try {
      const { fetchPerformanceServerSide } = await import('@/lib/ssr/performance')
      const out = await fetchPerformanceServerSide('svc-1')
      expect(out).toBeNull()
    } finally {
      warn.mockRestore()
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })
})
