import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { hashKey, hydrate, QueryClient } from '@tanstack/react-query'

import { setupNextHeadersMock, withTestHttpServer } from '../helpers/page-smoke'
import { resolveRangeWire, quantizeAnchor, pickInsightsDefault, historyHoursFromExtents } from '@/lib/utils'
import {
  seedDehydratedState,
  fetchStreamingServerSide,
  fetchBootstrapServerSide,
  fetchPerformanceServerSide,
  fetchSecurityServerSide,
  fetchInsightsServerSide,
  fetchOriginServerSide,
  fetchTosServerSide,
  fetchCronRunsServerSide,
  fetchUsageLogServerSide,
  resolveDefaultSsrRangeKey,
  resolveInsightsDefault,
  PERFORMANCE_SSR_DEFAULTS,
  SECURITY_SSR_DEFAULTS,
  SECURITY_SSR_SECTIONS,
  ORIGIN_SSR_DEFAULTS,
  ORIGIN_SSR_SECTIONS,
} from '@/lib/ssr/upstream'
import { ssrUpstreamGet } from '@/lib/api'

const { setHeaders, setCookies } = setupNextHeadersMock({ cookies: 'session=abc123' })
const inbound = setHeaders
const PUBLIC_HOST = 'fastly-log-analytics.global.ssl.fastly.net'
const NOW_ISO = new Date('2026-06-29T12:00:30Z')

const withJsonServer = <T>(
  body: unknown,
  fn: Parameters<typeof withTestHttpServer<T>>[1],
  status = 200,
) =>
  withTestHttpServer(
    (_req, res) => {
      res.statusCode = status
      res.setHeader('Content-Type', 'application/json')
      res.end(typeof body === 'string' ? body : JSON.stringify(body))
    },
    fn,
  )

describe('fetchBootstrapServerSide', () => {
  it('returns null when API_PROXY_URL is unset (pure `next dev` outside docker compose)', async () => {
    expect(await fetchBootstrapServerSide()).toBeNull()
  })

  it('returns null on a non-2xx upstream response', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    expect(await fetchBootstrapServerSide()).toBeNull()
    expect(warn).toHaveBeenCalled()
  })

  it('returns null on a parse error from a malformed upstream body', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await withJsonServer('not json {{{', async () => {
      expect(await fetchBootstrapServerSide()).toBeNull()
      expect(warn).toHaveBeenCalled()
    })
  })

  it('hits the upstream with the expected headers (smoke — verifies node:http path runs)', async () => {
    setCookies('session=xyz; theme=dark')
    setHeaders({ 'x-proxied-by-caddy': 'true', host: PUBLIC_HOST })
    await withJsonServer({ active_service_id: 'svc-1' }, async (srv) => {
      expect(await fetchBootstrapServerSide()).toEqual({ active_service_id: 'svc-1' })
      const capturedHeaders = srv.lastRequest!.headers
      expect(capturedHeaders['x-remote-analyst']).toBe('1')
      expect(capturedHeaders.host).toBe(PUBLIC_HOST)
      expect(capturedHeaders.cookie).toBe('session=xyz; theme=dark')
    })
  })

  it.each([
    ['non-loopback Host + no Caddy marker', { host: PUBLIC_HOST }],
    ['no Caddy marker + absent Host', {}],
  ])('fail-closed: %s → null + warns', async (_label, hdrs) => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    setCookies('')
    setHeaders(hdrs)
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    expect(await fetchBootstrapServerSide()).toBeNull()
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('no X-Proxied-By-Caddy marker'))
  })

  it('admin SSH-tunnel: loopback Host + no Caddy marker → forwards as admin (no X-Remote-Analyst)', async () => {
    setCookies('')
    setHeaders({ host: 'localhost:3001' })
    await withJsonServer({ active_service_id: 'svc-1' }, async (srv) => {
      await fetchBootstrapServerSide()
      expect(srv.lastRequest?.headers['x-remote-analyst']).toBeUndefined()
    })
  })
})

describe('Streaming SSR fixture contract', () => {
  it.each([
    { analyst: false, payload: { available: true, overview: { active_sessions: 1234 } } },
    { analyst: true, payload: { available: true, overview: { active_sessions: 1234 } } },
    { analyst: false, payload: { available: false, has_data: false } },
  ])('seeds $payload.available data with analyst=$analyst using the browser cookie and key', async ({ analyst, payload }) => {
    setHeaders({
      host: analyst ? 'logs.example.com' : 'localhost:13004',
      ...(analyst ? { 'x-proxied-by-caddy': '1' } : {}),
    })
    setCookies('fla_streaming_fixture=test-fixture')
    const queryClient = new QueryClient()
    try {
      await withJsonServer(payload, async (srv) => {
        const now = new Date('2026-10-23T12:00:37Z')
        const seed = await fetchStreamingServerSide('svc-streaming-test', now)
        expect(seed).not.toBeNull()
        if (!seed) throw new Error('Streaming SSR did not produce a seed')
        const req = srv.lastRequest!
        expect(req.method).toBe('POST')
        expect(req.url).toBe('/api/cmcd/aggregates')
        expect(JSON.parse(req.body)).toEqual({
          filters: {},
          bucket_seconds: 300,
          top_n: 30,
          range_token: '24h',
          anchor: '2026-10-23T12:00:00Z',
        })
        expect(req.headers.cookie).toBe('fla_streaming_fixture=test-fixture')
        expect(req.headers['x-service-id']).toBe('svc-streaming-test')
        expect(req.headers['x-remote-analyst']).toBe(analyst ? '1' : undefined)

        const anchor = quantizeAnchor(now.toISOString(), now)
        const { rangeKey } = resolveRangeWire({
          relativeRange: null,
          isAutoRange: true,
          startTime: null,
          endTime: null,
          anchor,
        })
        const seedKey = ['cmcd', 'aggregates', 'svc-streaming-test', seed.rangeToken, seed.anchor, {}]
        const browserKey = ['cmcd', 'aggregates', 'svc-streaming-test', rangeKey, anchor, {}]
        const dehydratedState = seedDehydratedState(seedKey, seed.data)
        expect(dehydratedState).not.toBeNull()
        if (!dehydratedState) throw new Error('Streaming SSR did not produce dehydrated state')
        hydrate(queryClient, dehydratedState)
        expect(queryClient.getQueryData(browserKey)).toEqual(payload)
        expect(queryClient.getQueryState(browserKey)?.dataUpdatedAt).toBeGreaterThan(0)
      })
    } finally {
      queryClient.clear()
    }
  })
})

describe('resolveDefaultSsrRangeKey & SSR-seed key ↔ client first-paint key byte-match', () => {
  it('cold load → rangeToken "24h", anchor floored to the 60s grid', () => {
    const now = new Date('2026-06-29T12:00:37.123Z')
    const got = resolveDefaultSsrRangeKey(now)
    expect(got.rangeToken).toBe('24h')
    expect(got.anchor).toBe('2026-06-29T12:00:00Z')
    expect(got.anchor).toBe(quantizeAnchor(now.toISOString(), now))
  })

  it('two instants in the same 60s quantum resolve the identical anchor', () => {
    const a = resolveDefaultSsrRangeKey(new Date('2026-06-29T12:00:01Z'))
    const b = resolveDefaultSsrRangeKey(new Date('2026-06-29T12:00:58Z'))
    expect(a.anchor).toBe(b.anchor)
    expect(a.anchor).toBe('2026-06-29T12:00:00Z')
  })

  it.each([
    {
      name: 'Performance',
      buildKeys: (serviceId: string, rangeToken: string, anchor: string) => [
        ['performance', 'aggregates', serviceId, rangeToken, anchor, {}, PERFORMANCE_SSR_DEFAULTS.sortBy],
        ['performance', 'aggregates', serviceId, rangeToken, anchor, {}, 'p99'],
      ],
    },
    {
      name: 'Security',
      buildKeys: (serviceId: string, rangeToken: string, anchor: string) => [
        ['security', 'aggregates', serviceId, rangeToken, anchor, {}, SECURITY_SSR_DEFAULTS.bucketSeconds],
        ['security', 'aggregates', serviceId, rangeToken, anchor, {}, 3600],
      ],
    },
    {
      name: 'Origin',
      buildKeys: (serviceId: string, rangeToken: string, anchor: string) => [
        [
          'origin', 'aggregates', serviceId, rangeToken, anchor, {},
          ORIGIN_SSR_DEFAULTS.bucketMinutes, ORIGIN_SSR_DEFAULTS.metric, ORIGIN_SSR_DEFAULTS.percentile, ORIGIN_SSR_SECTIONS,
        ],
        [
          'origin', 'aggregates', serviceId, rangeToken, anchor, {},
          60, 'ttfb', 'p95', ['summary', 'timeseries', 'slow_urls', 'status_codes', 'path_breakdown', 'pop_latency', 'ip_health'],
        ],
      ],
    },
  ])('$name SSR-seed key === client useServiceQuery key', ({ buildKeys }) => {
    const { rangeToken, anchor } = resolveDefaultSsrRangeKey(NOW_ISO)
    const [seedKey, clientKey] = buildKeys('svc-1', rangeToken, anchor)
    expect(hashKey(seedKey)).toBe(hashKey(clientKey))
  })
})

describe('fetchPerformanceServerSide (performance SSR)', () => {
  it('returns null without a serviceId (no upstream call)', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    expect(await fetchPerformanceServerSide(undefined)).toBeNull()
  })

  it('fail-closed: no Caddy marker + non-loopback Host → null, never forwarded', async () => {
    setHeaders({ host: PUBLIC_HOST })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await withJsonServer({}, async (srv) => {
      expect(await fetchPerformanceServerSide('svc-1')).toBeNull()
      expect(srv.requests).toHaveLength(0)
      expect(warn).toHaveBeenCalled()
    })
  })

  it('admin SSH-tunnel: POSTs unified aggregates as admin with service id', async () => {
    setHeaders({ host: 'localhost:3001' })
    await withJsonServer({ ok: true }, async (srv) => {
      const out = await fetchPerformanceServerSide('svc-1', NOW_ISO)
      expect(out).toEqual({ data: { ok: true }, rangeToken: '24h', anchor: '2026-06-29T12:00:00Z' })
      expect(srv.requests).toHaveLength(1)
      expect(JSON.parse(srv.requests[0].body)).toEqual({
        filters: {},
        sort_by: PERFORMANCE_SSR_DEFAULTS.sortBy,
        range_token: '24h',
        anchor: '2026-06-29T12:00:00Z',
      })
      for (const req of srv.requests) {
        expect(req.headers['x-remote-analyst']).toBeUndefined()
        expect(req.headers['x-service-id']).toBe('svc-1')
      }
    })
  })

  it('fails safely: if fetch fails, returns null', async () => {
    setHeaders({ host: 'localhost:3001' })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    try {
      await withJsonServer('boom', async () => {
        expect(await fetchPerformanceServerSide('svc-1')).toBeNull()
      }, 500)
    } finally {
      warn.mockRestore()
    }
  })
})

describe('fetchSecurityServerSide (security SSR)', () => {
  it('returns null without a serviceId (no upstream call)', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    expect(await fetchSecurityServerSide(undefined)).toBeNull()
  })

  it('fail-closed: no Caddy marker + non-loopback Host → null, never forwarded', async () => {
    setHeaders({ host: PUBLIC_HOST })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await withJsonServer({}, async (srv) => {
      expect(await fetchSecurityServerSide('svc-1')).toBeNull()
      expect(srv.requests).toHaveLength(0)
      expect(warn).toHaveBeenCalledWith(expect.stringContaining('non-loopback/absent Host'))
    })
  })

  it('admin SSH-tunnel (loopback Host, no marker): POSTs default keyed body as admin with service id', async () => {
    setHeaders({ host: 'localhost:3001' })
    await withJsonServer({ ngwaf_verified_bots: { rows: [] } }, async (srv) => {
      const out = await fetchSecurityServerSide('svc-1', NOW_ISO)
      expect(out).toEqual({
        data: { ngwaf_verified_bots: { rows: [] } },
        rangeToken: '24h',
        anchor: '2026-06-29T12:00:00Z',
      })
      const req = srv.lastRequest!
      expect(req.method).toBe('POST')
      expect(req.headers['content-type']).toContain('application/json')
      const body = JSON.parse(req.body)
      expect(body).toEqual({
        filters: {},
        bucket_seconds: SECURITY_SSR_DEFAULTS.bucketSeconds,
        sections: [...SECURITY_SSR_SECTIONS],
        range_token: '24h',
        anchor: '2026-06-29T12:00:00Z',
      })
      expect(body.start_time).toBeUndefined()
      expect(body.end_time).toBeUndefined()
      expect(req.headers['x-remote-analyst']).toBeUndefined()
      expect(req.headers['x-service-id']).toBe('svc-1')
    })
  })

  it('analyst (Caddy marker): POSTs as remote-analyst', async () => {
    setHeaders({ 'x-proxied-by-caddy': 'true', host: PUBLIC_HOST })
    await withJsonServer({ ngwaf_verified_bots: { rows: [] } }, async (srv) => {
      const out = await fetchSecurityServerSide('svc-1')
      expect(out).not.toBeNull()
      const req = srv.lastRequest!
      expect(req.method).toBe('POST')
      expect(req.headers['x-remote-analyst']).toBe('1')
      expect(req.headers['x-service-id']).toBe('svc-1')
    })
  })
})

describe('resolveInsightsDefault (key-match contract)', () => {
  it('mirrors useInsightsDefaults: pickInsightsDefault over earliest_log_at', () => {
    const earliest = new Date(Date.now() - 10 * 24 * 3600 * 1000).toISOString()
    const expected = pickInsightsDefault(historyHoursFromExtents(earliest))
    expect(resolveInsightsDefault(earliest)).toEqual({
      windowHours: expected.window,
      baselineHours: expected.baseline,
    })
  })

  it('no extents → STATIC_DEFAULT tokens (matches client no-data path)', () => {
    expect(resolveInsightsDefault(null)).toEqual({ windowHours: '24', baselineHours: '168' })
    expect(resolveInsightsDefault(undefined)).toEqual({ windowHours: '24', baselineHours: '168' })
  })
})

describe('fetchInsightsServerSide (insights SSR)', () => {
  it('returns null without a serviceId (no upstream call)', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    expect(await fetchInsightsServerSide(undefined, null)).toBeNull()
  })

  describe('Insights page hydration', () => {
    it.each([undefined, 'svc-2'])('scopes first-render extents to the bootstrap service (override: %s)', async (service) => {
      vi.resetModules()
      setHeaders({ host: 'localhost:3001' })
      const extents = {
        earliest_log_at: new Date(Date.now() - 36 * 3600_000).toISOString(),
        latest_log_at: new Date().toISOString(),
      }
      await withTestHttpServer(
        (req, res) => {
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify(req.url === '/api/bootstrap'
            ? { active_service_id: 'svc-1', log_extents: extents }
            : { insights: [] }))
        },
        async () => {
          const { default: InsightsPage } = await import('@/app/insights/page')
          const page = await InsightsPage({ searchParams: Promise.resolve({ service }) })
          const queries = page.props.state.queries
          expect(queries).toEqual(expect.arrayContaining([
            expect.objectContaining({ queryKey: ['insights', service ?? 'svc-1', '4', '24'] }),
          ]))
          const extentsQuery = queries.find(
            (query: { queryKey: readonly unknown[] }) => query.queryKey[0] === 'log-extents',
          )
          if (service) {
            expect(extentsQuery).toBeUndefined()
          } else {
            expect(extentsQuery).toMatchObject({
              queryKey: ['log-extents', 'svc-1'],
              state: { data: extents },
            })
          }
        },
      )
    })
  })

  it('fail-closed: no Caddy marker + non-loopback Host → null, never forwarded', async () => {
    setHeaders({ host: PUBLIC_HOST })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await withJsonServer({ insights: [] }, async (srv) => {
      expect(await fetchInsightsServerSide('svc-1', null)).toBeNull()
      expect(srv.requests).toHaveLength(0)
      expect(warn).toHaveBeenCalledWith(expect.stringContaining('non-loopback/absent Host'))
    })
  })

  it('admin SSH-tunnel (loopback Host, no marker): POSTs default body as admin with service id', async () => {
    setHeaders({ host: 'localhost:3001' })
    await withJsonServer({ insights: [1, 2, 3] }, async (srv) => {
      const earliest = new Date(Date.now() - 10 * 24 * 3600 * 1000).toISOString()
      const out = await fetchInsightsServerSide('svc-1', { earliest_log_at: earliest })
      expect(out).toEqual({ data: { insights: [1, 2, 3] }, windowHours: '24', baselineHours: '168' })
      const req = srv.lastRequest!
      expect(req.method).toBe('POST')
      expect(req.headers['content-type']).toContain('application/json')
      expect(JSON.parse(req.body)).toEqual({ window_size_hrs: 24, baseline_hours: 168, filters: {} })
      expect(req.headers['x-remote-analyst']).toBeUndefined()
      expect(req.headers['x-service-id']).toBe('svc-1')
    })
  })

  it('analyst (Caddy marker): POSTs as remote-analyst', async () => {
    setHeaders({ 'x-proxied-by-caddy': 'true', host: PUBLIC_HOST })
    await withJsonServer({ insights: [1] }, async (srv) => {
      const out = await fetchInsightsServerSide('svc-1', null)
      expect(out?.data).toEqual({ insights: [1] })
      const req = srv.lastRequest!
      expect(req.method).toBe('POST')
      expect(req.headers['x-remote-analyst']).toBe('1')
      expect(req.headers['host']).toBe(PUBLIC_HOST)
      expect(req.headers['x-service-id']).toBe('svc-1')
    })
  })
})

describe('fetchOriginServerSide (origin SSR)', () => {
  it('returns null without a serviceId (no upstream call)', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    expect(await fetchOriginServerSide(undefined)).toBeNull()
  })

  it('fail-closed: no Caddy marker + non-loopback Host → null, never forwarded', async () => {
    setHeaders({ host: PUBLIC_HOST })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await withJsonServer({ summary: {} }, async (srv) => {
      expect(await fetchOriginServerSide('svc-1')).toBeNull()
      expect(srv.requests).toHaveLength(0)
      expect(warn).toHaveBeenCalledWith(expect.stringContaining('non-loopback/absent Host'))
    })
  })

  it('admin SSH-tunnel (loopback Host, no marker): POSTs default keyed body as admin with service id', async () => {
    setHeaders({ host: 'localhost:3001' })
    await withJsonServer({ summary: { has_data: true } }, async (srv) => {
      const out = await fetchOriginServerSide('svc-1', NOW_ISO)
      expect(out).toEqual({
        data: { summary: { has_data: true } },
        rangeToken: '24h',
        anchor: '2026-06-29T12:00:00Z',
      })
      const req = srv.lastRequest!
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
      expect(req.headers['x-remote-analyst']).toBeUndefined()
      expect(req.headers['x-service-id']).toBe('svc-1')
    })
  })

  it('analyst (Caddy marker): POSTs as remote-analyst', async () => {
    setHeaders({ 'x-proxied-by-caddy': 'true', host: PUBLIC_HOST })
    await withJsonServer({ summary: { has_data: true } }, async (srv) => {
      const out = await fetchOriginServerSide('svc-1')
      expect(out?.data).toEqual({ summary: { has_data: true } })
      const req = srv.lastRequest!
      expect(req.method).toBe('POST')
      expect(req.headers['x-remote-analyst']).toBe('1')
      expect(req.headers['host']).toBe(PUBLIC_HOST)
      expect(req.headers['x-service-id']).toBe('svc-1')
    })
  })
})

const secret = 'test-gateway-secret-with-at-least-32-characters'
let upstreamBody: object

describe('certificate gateway SSR trust', () => {
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

  it('forwards authenticated admin traffic on a non-loopback hostname', async () => {
    inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret })
    await withTestHttpServer(
      (_req, res) => { res.end(JSON.stringify(upstreamBody)) },
      async (srv) => {
        const response = await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })
        expect(response?.statusCode).toBe(200)
        expect(srv.lastRequest?.headers['x-admin-gateway-token']).toBe(secret)
        expect(srv.lastRequest?.headers.host).toBe('admin.example.com')
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBeUndefined()
      },
    )
  })

  it.each([
    ['rejects a forged credential even with a localhost Host', { host: 'localhost:3000', 'x-admin-gateway-token': 'forged' }],
    ['does not prefetch admin data for a cross-site browser request', { host: 'admin.example.com', 'x-admin-gateway-token': secret, 'sec-fetch-site': 'cross-site' }],
    ['disables the old localhost admin shortcut in required mode', { host: 'localhost:3000' }],
    ['never promotes a public marked request even if it carries a gateway token', { host: 'analyst.example.com', 'x-proxied-by-caddy': '1', 'x-admin-gateway-token': secret }],
  ])('%s', async (_label, hdrs) => {
    inbound(hdrs as Record<string, string>)
    await withTestHttpServer(
      (_req, res) => { res.end(JSON.stringify(upstreamBody)) },
      async (srv) => {
        expect(await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })).toBeNull()
        expect(srv.lastRequest).toBeUndefined()
      },
    )
  })

  it('keeps public ingress traffic on the analyst path', async () => {
    inbound({ host: 'analyst.example.com', 'x-proxied-by-caddy': '1' })
    await withTestHttpServer(
      (_req, res) => { res.end(JSON.stringify(upstreamBody)) },
      async (srv) => {
        expect((await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' }))?.statusCode).toBe(200)
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBe('1')
        expect(srv.lastRequest?.headers['x-admin-gateway-token']).toBeUndefined()
        expect(srv.lastRequest?.headers.host).toBe('analyst.example.com')
      },
    )
  })

  it('does not let caller-supplied extraHeaders forge a gateway credential', async () => {
    inbound({ host: 'analyst.example.com', 'x-proxied-by-caddy': '1' })
    await withTestHttpServer(
      (_req, res) => { res.end(JSON.stringify(upstreamBody)) },
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
    await withTestHttpServer(
      (_req, res) => { res.end(JSON.stringify(upstreamBody)) },
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
    await withTestHttpServer(
      (_req, res) => { res.end(JSON.stringify(upstreamBody)) },
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
    await withTestHttpServer(
      (_req, res) => { res.end(JSON.stringify(upstreamBody)) },
      async (srv) => {
        inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret, 'x-service-id': 'analyst-cookie-cache-test' })
        expect(await fetchBootstrapServerSide()).toEqual({})
        srv.clearRequests()
        setCookies(
          'analyst_session_id=test-session',
          (name: string) => (name === 'analyst_session_id' ? { value: 'test-session' } : undefined),
        )
        expect(await fetchBootstrapServerSide()).toEqual({})
        expect(srv.lastRequest?.headers).toEqual(expect.objectContaining({ cookie: 'analyst_session_id=test-session' }))
        srv.clearRequests()
        setCookies('')
        expect(await fetchBootstrapServerSide()).toEqual({})
        expect(srv.lastRequest).toBeUndefined()
      },
    )
  })

  it('never caches a bootstrap response marked as analyst-scoped', async () => {
    await withTestHttpServer(
      (_req, res) => { res.end(JSON.stringify(upstreamBody)) },
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
    inbound({ 'x-proxied-by-caddy': 'true', host: PUBLIC_HOST })
  })

  afterEach(() => vi.restoreAllMocks())

  it('returns null when API_PROXY_URL is unset (pure `next dev` outside docker compose)', async () => {
    expect(await fetchTosServerSide()).toBeNull()
  })

  it('returns null on a non-2xx, non-401 upstream response', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    expect(await fetchTosServerSide()).toBeNull()
    expect(warn).toHaveBeenCalled()
  })

  it("returns 'unauthenticated' on a 401 upstream response (analyst session missing/invalid)", async () => {
    await withJsonServer({ detail: 'unauthenticated' }, async () => {
      expect(await fetchTosServerSide()).toBe('unauthenticated')
    }, 401)
  })

  it('returns null on a parse error from a malformed upstream body', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await withJsonServer('not json {{{', async () => {
      expect(await fetchTosServerSide()).toBeNull()
      expect(warn).toHaveBeenCalled()
    })
  })

  it('hits the upstream with the expected headers when Caddy marker is present', async () => {
    setCookies('session=xyz; theme=dark')
    inbound({ 'x-proxied-by-caddy': 'true', host: PUBLIC_HOST })
    await withJsonServer({ version: 'v1', text: 'TOS body' }, async (srv) => {
      expect(await fetchTosServerSide()).toEqual({ version: 'v1', text: 'TOS body' })
      const capturedHeaders = srv.lastRequest!.headers
      expect(capturedHeaders['x-remote-analyst']).toBe('1')
      expect(capturedHeaders.host).toBe(PUBLIC_HOST)
      expect(capturedHeaders.cookie).toBe('session=xyz; theme=dark')
    })
  })

  it.each([
    ['non-loopback Host + no Caddy marker', { host: PUBLIC_HOST }],
    ['no Caddy marker + absent Host', {}],
  ])('fail-closed: %s → null + warns', async (_label, hdrs) => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    setCookies('')
    inbound(hdrs as Record<string, string>)
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    expect(await fetchTosServerSide()).toBeNull()
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('no X-Proxied-By-Caddy marker'))
  })

  it('admin SSH-tunnel: loopback Host + no Caddy marker → forwards as admin (no X-Remote-Analyst)', async () => {
    setCookies('')
    inbound({ host: 'localhost:3001' })
    await withJsonServer({ version: 'v1', text: 'TOS body' }, async (srv) => {
      expect(await fetchTosServerSide()).toEqual({ version: 'v1', text: 'TOS body' })
      expect(srv.lastRequest?.headers['x-remote-analyst']).toBeUndefined()
    })
  })
})

describe('fetchCronRunsServerSide & fetchUsageLogServerSide', () => {
  const usageArgs = { serviceId: 'svc-1', start: '2026-06-01', end: '2026-06-17', pageSize: 50 }

  beforeEach(() => {
    delete process.env.ADMIN_GATEWAY_SECRET
    delete process.env.ADMIN_GATEWAY_REQUIRED
  })

  afterEach(() => vi.restoreAllMocks())

  it('fetchCronRunsServerSide returns null without a serviceId', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    expect(await fetchCronRunsServerSide(undefined)).toBeNull()
  })

  it.each([
    ['fetchCronRunsServerSide', () => fetchCronRunsServerSide('svc-1')],
    ['fetchUsageLogServerSide', () => fetchUsageLogServerSide(usageArgs)],
  ])('%s fail-closed: no Caddy marker + non-loopback Host → null, never forwarded', async (_name, call) => {
    inbound({ host: PUBLIC_HOST })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await withJsonServer({ rows: [] }, async (srv) => {
      expect(await call()).toBeNull()
      expect(srv.requests).toHaveLength(0)
      expect(warn).toHaveBeenCalledWith(expect.stringContaining('non-loopback/absent Host'))
    })
  })

  it('fetchCronRunsServerSide admin SSH-tunnel: forwards as admin with service id', async () => {
    inbound({ host: 'localhost:3001' })
    await withJsonServer({ rows: [1, 2, 3] }, async (srv) => {
      expect(await fetchCronRunsServerSide('svc-1')).toEqual({ rows: [1, 2, 3] })
      expect(srv.lastRequest?.headers['x-remote-analyst']).toBeUndefined()
      expect(srv.lastRequest?.headers['x-fastly-service-id']).toBe('svc-1')
    })
  })

  it('fetchCronRunsServerSide analyst (Caddy marker): forwards as remote-analyst', async () => {
    inbound({ 'x-proxied-by-caddy': 'true', host: PUBLIC_HOST })
    await withJsonServer({ rows: [1, 2, 3] }, async (srv) => {
      expect(await fetchCronRunsServerSide('svc-1')).toEqual({ rows: [1, 2, 3] })
      expect(srv.lastRequest?.headers['x-remote-analyst']).toBe('1')
      expect(srv.lastRequest?.headers['x-fastly-service-id']).toBe('svc-1')
    })
  })

  it('fetchUsageLogServerSide admin SSH-tunnel: forwards as admin with the query string', async () => {
    inbound({ host: 'localhost:3001' })
    await withJsonServer({ rows: [1] }, async (srv) => {
      expect(await fetchUsageLogServerSide(usageArgs)).toEqual({ rows: [1] })
      expect(srv.lastRequest?.headers['x-remote-analyst']).toBeUndefined()
      expect(srv.lastRequest?.url).toContain('/api/admin/usage-log?')
      expect(srv.lastRequest?.url).toContain('service_id=svc-1')
    })
  })
})

describe('ssrUpstreamGet — debug-responses cookie propagation', () => {
  beforeEach(() => {
    delete process.env.ADMIN_GATEWAY_SECRET
    delete process.env.ADMIN_GATEWAY_REQUIRED
  })

  afterEach(() => vi.restoreAllMocks())

  it.each([
    {
      label: 'cookie=1 + loopback Host (admin branch) → upstream request carries x-debug-responses: 1',
      cookieVal: '1',
      hdrs: { host: 'localhost:3001' },
      expected: '1',
    },
    {
      label: 'cookie=1 + Caddy marker (analyst branch) → header NOT sent',
      cookieVal: '1',
      hdrs: { 'x-proxied-by-caddy': 'true', host: PUBLIC_HOST },
      expected: undefined,
    },
    {
      label: 'cookie absent → header not sent',
      cookieVal: undefined,
      hdrs: { host: 'localhost:3001' },
      expected: undefined,
    },
    {
      label: 'cookie=0 → header not sent',
      cookieVal: '0',
      hdrs: { host: 'localhost:3001' },
      expected: undefined,
    },
  ])('$label', async ({ cookieVal, hdrs, expected }) => {
    if (cookieVal === undefined) {
      setCookies('')
    } else {
      setCookies(`fla.debugResponses=${cookieVal}`, (k: string) =>
        k === 'fla.debugResponses' ? { value: cookieVal } : undefined,
      )
    }
    inbound(hdrs as Record<string, string>)
    await withJsonServer({}, async (srv) => {
      const out = await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })
      expect(out).not.toBeNull()
      expect(srv.lastRequest?.headers['x-debug-responses']).toBe(expected)
    })
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
