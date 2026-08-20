import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

// origin.ts is the SECOND POST caller of the shared ./_transport helper (after
// insights.ts). These tests pin (1) the security contract — the Caddy-marker
// trust gate must behave IDENTICALLY for this POST as for the GET callers
// (refuse no-marker + non-loopback Host; analyst-clamp on the marker branch;
// admin token only on the loopback branch) — and (2) the key-match contract:
// the (rangeToken, anchor) this helper resolves equals what OriginClient
// computes on first paint, so the SSR seed key byte-matches the client
// first-paint key. A mismatch would double-fetch (worse than no SSR).

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

describe('resolveOriginDefaultKey (key-match contract)', () => {
  it('cold load → rangeToken "24h", anchor floored to the 60s grid', async () => {
    const { resolveOriginDefaultKey } = await import('@/lib/ssr/origin')
    const { quantizeAnchor } = await import('@/lib/time-window')

    const now = new Date('2026-06-29T12:00:37.123Z')
    const got = resolveOriginDefaultKey(now)
    expect(got.rangeToken).toBe('24h')
    // 60s-floored, ISO-Z (no millis) — must equal what OriginClient's
    // quantizeAnchor pins for the same instant.
    expect(got.anchor).toBe('2026-06-29T12:00:00Z')
    expect(got.anchor).toBe(quantizeAnchor(now.toISOString(), now))
  })

  it('two instants in the same 60s quantum resolve the identical anchor', async () => {
    const { resolveOriginDefaultKey } = await import('@/lib/ssr/origin')
    const a = resolveOriginDefaultKey(new Date('2026-06-29T12:00:01Z'))
    const b = resolveOriginDefaultKey(new Date('2026-06-29T12:00:58Z'))
    expect(a.anchor).toBe(b.anchor)
    expect(a.anchor).toBe('2026-06-29T12:00:00Z')
  })

  it.skip('stale log extents (>15min old) snap the anchor to the real latest log, not now', async () => {
    const { resolveOriginDefaultKey } = await import('@/lib/ssr/origin')
    const now = new Date('2026-06-29T12:00:00Z')
    const got = resolveOriginDefaultKey(now, {
      earliest_log_at: '2026-06-01T00:00:00Z',
      latest_log_at: '2026-06-29T11:00:00Z',
    })
    expect(got.rangeToken).toBe('24h')
    expect(got.anchor).toBe('2026-06-29T11:00:00Z')
  })

  it('fresh log extents (<=15min old) leave the naive "now" anchor unchanged', async () => {
    const { resolveOriginDefaultKey } = await import('@/lib/ssr/origin')
    const now = new Date('2026-06-29T12:00:00Z')
    const got = resolveOriginDefaultKey(now, {
      earliest_log_at: '2026-06-01T00:00:00Z',
      latest_log_at: '2026-06-29T11:55:00Z',
    })
    expect(got.anchor).toBe('2026-06-29T12:00:00Z')
  })
})

describe('SSR-seed key ↔ client first-paint key byte-match', () => {
  // The load-bearing invariant: the key page.tsx seeds under MUST byte-match the
  // key OriginReportContent's useServiceQuery builds on first paint, or the seed
  // misses and the page double-fetches (worse than no SSR). React Query hashes
  // keys structurally via hashKey (stable, sorted JSON), so we assert the two
  // keys produce the identical hash. Both keys are reconstructed here from the
  // SAME server-reproducible first-paint values documented in both files.
  it('the page.tsx seed key === the client useServiceQuery key', async () => {
    const { hashKey } = await import('@tanstack/react-query')
    const { resolveOriginDefaultKey, ORIGIN_SSR_DEFAULTS, ORIGIN_SSR_SECTIONS } = await import(
      '@/lib/ssr/origin'
    )

    const serviceId = 'svc-1'
    const { rangeToken, anchor } = resolveOriginDefaultKey(new Date('2026-06-29T12:00:30Z'))

    // ── SSR seed key (mirrors app/origin/page.tsx) ──
    const seedKey = [
      'origin',
      'aggregates',
      serviceId,
      rangeToken,
      anchor,
      {},
      ORIGIN_SSR_DEFAULTS.bucketMinutes,
      ORIGIN_SSR_DEFAULTS.metric,
      ORIGIN_SSR_DEFAULTS.percentile,
      ORIGIN_SSR_SECTIONS,
    ]

    // ── Client first-paint key (mirrors OriginReportContent) ──
    // Cold-load first-paint values: filterPayload = {} (no filters), bucket =
    // intervalMap['1 hour'] = 60 (default 24h span → "1 hour"), originMetric =
    // 'ttfb', originPercentile = 'p95', the constant ORIGIN_SECTIONS, and the
    // same (rangeToken, anchor) OriginClient pins.
    const clientFilterPayload = {} // buildFiltersPayload([]) on a cold load
    const clientBucketMinutes = 60 // intervalMap['1 hour'] at the 24h default span
    const clientMetric = 'ttfb'
    const clientPercentile = 'p95'
    const ORIGIN_SECTIONS = [
      'summary',
      'timeseries',
      'slow_urls',
      'status_codes',
      'path_breakdown',
      'pop_latency',
      'ip_health',
    ]
    const clientKey = [
      'origin',
      'aggregates',
      serviceId,
      rangeToken,
      anchor,
      clientFilterPayload,
      clientBucketMinutes,
      clientMetric,
      clientPercentile,
      ORIGIN_SECTIONS,
    ]

    expect(hashKey(seedKey)).toBe(hashKey(clientKey))
  })
})

describe('fetchOriginServerSide (origin SSR)', () => {
  it('returns null without a serviceId (no upstream call)', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    const { fetchOriginServerSide } = await import('@/lib/ssr/origin')
    expect(await fetchOriginServerSide(undefined)).toBeNull()
  })

  it('fail-closed: no Caddy marker + non-loopback Host → null, never forwarded', async () => {
    const http = await import('node:http')
    let hit = false
    const server = http.createServer((_req, res) => {
      hit = true
      res.statusCode = 200
      res.end('{"summary":{}}')
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
    mockHeaders.mockReturnValue(headerGetter({ host: 'fastly-log-analytics.global.ssl.fastly.net' }))
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    try {
      const { fetchOriginServerSide } = await import('@/lib/ssr/origin')
      const out = await fetchOriginServerSide('svc-1')
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
        res.end('{"summary":{"has_data":true}}')
      })
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
    process.env.ADMIN_SHARED_SECRET = 'sekret'
    mockHeaders.mockReturnValue(headerGetter({ host: 'localhost:3001' }))
    try {
      const { fetchOriginServerSide, ORIGIN_SSR_DEFAULTS, ORIGIN_SSR_SECTIONS } = await import(
        '@/lib/ssr/origin'
      )
      const now = new Date('2026-06-29T12:00:30Z')
      const out = await fetchOriginServerSide('svc-1', now)

      expect(out).not.toBeNull()
      expect(out!.data).toEqual({ summary: { has_data: true } })
      expect(out!.rangeToken).toBe('24h')
      expect(out!.anchor).toBe('2026-06-29T12:00:00Z')

      // POST with the exact client keyed body shape. start_time/end_time MUST be
      // absent (the keyed path resolves the window from range_token + anchor).
      expect(capturedMethod).toBe('POST')
      expect(captured['content-type']).toContain('application/json')
      const body = JSON.parse(capturedBody)
      expect(body).toEqual({
        filters: {},
        bucket_minutes: ORIGIN_SSR_DEFAULTS.bucketMinutes,
        split_by_leg: false,
        timeseries_metric: ORIGIN_SSR_DEFAULTS.metric,
        timeseries_percentile: ORIGIN_SSR_DEFAULTS.percentile,
        slow_urls_limit: ORIGIN_SSR_DEFAULTS.slowUrlsLimit,
        slow_urls_min_requests: ORIGIN_SSR_DEFAULTS.slowUrlsMinRequests,
        pop_latency_limit: ORIGIN_SSR_DEFAULTS.popLatencyLimit,
        ip_health_limit: ORIGIN_SSR_DEFAULTS.ipHealthLimit,
        sections: [...ORIGIN_SSR_SECTIONS],
        range_token: '24h',
        anchor: '2026-06-29T12:00:00Z',
      })
      expect(body.start_time).toBeUndefined()
      expect(body.end_time).toBeUndefined()

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
        res.end('{"summary":{"has_data":true}}')
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
      const { fetchOriginServerSide } = await import('@/lib/ssr/origin')
      const out = await fetchOriginServerSide('svc-1')
      expect(out).not.toBeNull()
      expect(out!.data).toEqual({ summary: { has_data: true } })
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
