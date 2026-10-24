import { describe, expect, it, vi } from 'vitest'

import { setupNextHeadersMock, withTestHttpServer } from '../helpers/ssr'

// origin.ts is the SECOND POST caller of the shared ./_transport helper (after
// insights.ts). These tests pin (1) the security contract — the Caddy-marker
// trust gate must behave IDENTICALLY for this POST as for the GET callers
// (refuse no-marker + non-loopback Host; analyst-clamp on the marker branch) — and (2) the key-match contract:
// the (rangeToken, anchor) this helper resolves equals what OriginClient
// computes on first paint, so the SSR seed key byte-matches the client
// first-paint key. A mismatch would double-fetch (worse than no SSR).

const { setHeaders } = setupNextHeadersMock()

describe('resolveDefaultSsrRangeKey (key-match contract)', () => {
  it('cold load → rangeToken "24h", anchor floored to the 60s grid', async () => {
    const { resolveDefaultSsrRangeKey } = await import('@/lib/ssr/upstream')
    const { quantizeAnchor } = await import('@/lib/time-window')

    const now = new Date('2026-06-29T12:00:37.123Z')
    const got = resolveDefaultSsrRangeKey(now)
    expect(got.rangeToken).toBe('24h')
    // 60s-floored, ISO-Z (no millis) — must equal what OriginClient's
    // quantizeAnchor pins for the same instant.
    expect(got.anchor).toBe('2026-06-29T12:00:00Z')
    expect(got.anchor).toBe(quantizeAnchor(now.toISOString(), now))
  })

  it('two instants in the same 60s quantum resolve the identical anchor', async () => {
    const { resolveDefaultSsrRangeKey } = await import('@/lib/ssr/upstream')
    const a = resolveDefaultSsrRangeKey(new Date('2026-06-29T12:00:01Z'))
    const b = resolveDefaultSsrRangeKey(new Date('2026-06-29T12:00:58Z'))
    expect(a.anchor).toBe(b.anchor)
    expect(a.anchor).toBe('2026-06-29T12:00:00Z')
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
    const { resolveDefaultSsrRangeKey } = await import('@/lib/ssr/upstream')
    const { ORIGIN_SSR_DEFAULTS, ORIGIN_SSR_SECTIONS } = await import(
      '@/lib/ssr/upstream'
    )

    const serviceId = 'svc-1'
    const { rangeToken, anchor } = resolveDefaultSsrRangeKey(new Date('2026-06-29T12:00:30Z'))

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
    const { fetchOriginServerSide } = await import('@/lib/ssr/upstream')
    expect(await fetchOriginServerSide(undefined)).toBeNull()
  })

  it('fail-closed: no Caddy marker + non-loopback Host → null, never forwarded', async () => {
    setHeaders({ host: 'fastly-log-analytics.global.ssl.fastly.net' })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.end('{"summary":{}}')
      },
      async (srv) => {
        const { fetchOriginServerSide } = await import('@/lib/ssr/upstream')
        const out = await fetchOriginServerSide('svc-1')
        expect(out).toBeNull()
        expect(srv.requests).toHaveLength(0)
        expect(warn).toHaveBeenCalledWith(expect.stringContaining('non-loopback/absent Host'))
      },
    )
  })

  it('admin SSH-tunnel (loopback Host, no marker): POSTs default keyed body as admin with service id', async () => {
    setHeaders({ host: 'localhost:3001' })
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('{"summary":{"has_data":true}}')
      },
      async (srv) => {
        const { fetchOriginServerSide, ORIGIN_SSR_DEFAULTS, ORIGIN_SSR_SECTIONS } = await import(
          '@/lib/ssr/upstream'
        )
        const now = new Date('2026-06-29T12:00:30Z')
        const out = await fetchOriginServerSide('svc-1', now)

        expect(out).not.toBeNull()
        expect(out!.data).toEqual({ summary: { has_data: true } })
        expect(out!.rangeToken).toBe('24h')
        expect(out!.anchor).toBe('2026-06-29T12:00:00Z')

        const req = srv.lastRequest!
        // POST with the exact client keyed body shape. start_time/end_time MUST be
        // absent (the keyed path resolves the window from range_token + anchor).
        expect(req.method).toBe('POST')
        expect(req.headers['content-type']).toContain('application/json')
        const body = JSON.parse(req.body)
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

        // Admin trust path: no analyst header, service id passed.
        expect(req.headers['x-remote-analyst']).toBeUndefined()
        expect(req.headers['x-service-id']).toBe('svc-1')
      },
    )
  })

  it('analyst (Caddy marker): POSTs as remote-analyst', async () => {
    setHeaders({
      'x-proxied-by-caddy': 'true',
      host: 'fastly-log-analytics.global.ssl.fastly.net',
    })
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('{"summary":{"has_data":true}}')
      },
      async (srv) => {
        const { fetchOriginServerSide } = await import('@/lib/ssr/upstream')
        const out = await fetchOriginServerSide('svc-1')
        expect(out).not.toBeNull()
        expect(out!.data).toEqual({ summary: { has_data: true } })
        const req = srv.lastRequest!
        expect(req.method).toBe('POST')
        // Analyst trust path: remote-analyst stamped.
        expect(req.headers['x-remote-analyst']).toBe('1')
        expect(req.headers['host']).toBe('fastly-log-analytics.global.ssl.fastly.net')
        expect(req.headers['x-service-id']).toBe('svc-1')
      },
    )
  })
})
