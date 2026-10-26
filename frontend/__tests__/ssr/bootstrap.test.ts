import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { hydrate, QueryClient } from '@tanstack/react-query'

import { setupNextHeadersMock, withTestHttpServer } from '../helpers/page-smoke'
import { resolveRangeWire, quantizeAnchor } from '@/lib/utils'
import { seedDehydratedState } from '@/lib/ssr/upstream'
import { fetchStreamingServerSide } from '@/lib/ssr/upstream'

const { setHeaders, setCookies } = setupNextHeadersMock({ cookies: 'session=abc123' })
const inbound = setHeaders

// The helper uses node:http.request (NOT fetch — fetch overrides
// Host, which the backend's _remote_host_allowed gate rejects). The
// header-shape assertions live in the adversarial prod verification
// rather than in unit tests because mocking node:http portably across
// vitest's module-transform layers is fragile and tends to mask the
// real behavior we care about (which node:http actually emits on the
// wire). Unit tests here focus on the failure paths the helper
// catches so a backend outage / misconfig never breaks SSR rendering.

describe('fetchBootstrapServerSide', () => {
  it('returns null when API_PROXY_URL is unset (pure `next dev` outside docker compose)', async () => {
    const { fetchBootstrapServerSide } = await import('@/lib/ssr/upstream')
    const out = await fetchBootstrapServerSide()
    expect(out).toBeNull()
  })

  it('returns null on a non-2xx upstream response', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1' // refused — guaranteed network failure path
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { fetchBootstrapServerSide } = await import('@/lib/ssr/upstream')
    const out = await fetchBootstrapServerSide()
    // Either a network error or a 5xx — both must collapse to null,
    // never throw, never leak a partial response.
    expect(out).toBeNull()
    expect(warn).toHaveBeenCalled()
  })

  it('returns null on a parse error from a malformed upstream body', async () => {
    // Stand up a one-shot HTTP server that returns invalid JSON, so
    // the JSON.parse inside the helper throws and the catch returns null.
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.end('not json {{{')
      },
      async () => {
        const { fetchBootstrapServerSide } = await import('@/lib/ssr/upstream')
        const out = await fetchBootstrapServerSide()
        expect(out).toBeNull()
        expect(warn).toHaveBeenCalled()
      },
    )
  })

  it('hits the upstream with the expected headers (smoke — verifies node:http path runs)', async () => {
    setCookies('session=xyz; theme=dark')
    setHeaders({
      'x-proxied-by-caddy': 'true',
      host: 'fastly-log-analytics.global.ssl.fastly.net',
    })

    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify({ active_service_id: 'svc-1' }))
      },
      async (srv) => {
        const { fetchBootstrapServerSide } = await import('@/lib/ssr/upstream')
        const out = await fetchBootstrapServerSide()
        expect(out).toEqual({ active_service_id: 'svc-1' })
        // Security-critical assertions: when inbound has the Caddy
        // marker, upstream MUST set X-Remote-Analyst AND forward the
        // public Host so the backend classifies as remote-analyst
        // instead of admin-from-loopback.
        const capturedHeaders = srv.lastRequest!.headers
        expect(capturedHeaders['x-remote-analyst']).toBe('1')
        expect(capturedHeaders.host).toBe('fastly-log-analytics.global.ssl.fastly.net')
        expect(capturedHeaders.cookie).toBe('session=xyz; theme=dark')
      },
    )
  })

  it('fail-closed: non-loopback Host + no Caddy marker → null + warns', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1' // must not be reached
    setCookies('')
    setHeaders({ host: 'fastly-log-analytics.global.ssl.fastly.net' })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { fetchBootstrapServerSide } = await import('@/lib/ssr/upstream')
    const out = await fetchBootstrapServerSide()
    expect(out).toBeNull()
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('no X-Proxied-By-Caddy marker'))
  })

  it('admin SSH-tunnel: loopback Host + no Caddy marker → forwards as admin (no X-Remote-Analyst)', async () => {
    setCookies('')
    setHeaders({ host: 'localhost:3001' })

    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify({ active_service_id: 'svc-1' }))
      },
      async (srv) => {
        const { fetchBootstrapServerSide } = await import('@/lib/ssr/upstream')
        await fetchBootstrapServerSide()
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBeUndefined()
      },
    )
  })

  it('fail-closed: no Caddy marker + absent Host → null (drift / SSRF without a loopback Host)', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1' // must not be reached
    setCookies('')
    setHeaders({})
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { fetchBootstrapServerSide } = await import('@/lib/ssr/upstream')
    const out = await fetchBootstrapServerSide()
    expect(out).toBeNull()
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('no X-Proxied-By-Caddy marker'))
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
      await withTestHttpServer(
        (_req, res) => {
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify(payload))
        },
        async (srv) => {
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
        },
      )
    } finally {
      queryClient.clear()
    }
  })
})

describe('resolveDefaultSsrRangeKey (key-match contract)', () => {
  it('cold load → rangeToken "24h", anchor floored to the 60s grid', async () => {
    const { resolveDefaultSsrRangeKey } = await import('@/lib/ssr/upstream')
    const { quantizeAnchor: qa } = await import('@/lib/utils')

    const now = new Date('2026-06-29T12:00:37.123Z')
    const got = resolveDefaultSsrRangeKey(now)
    expect(got.rangeToken).toBe('24h')
    expect(got.anchor).toBe('2026-06-29T12:00:00Z')
    expect(got.anchor).toBe(qa(now.toISOString(), now))
  })

  it('two instants in the same 60s quantum resolve the identical anchor', async () => {
    const { resolveDefaultSsrRangeKey } = await import('@/lib/ssr/upstream')
    const a = resolveDefaultSsrRangeKey(new Date('2026-06-29T12:00:01Z'))
    const b = resolveDefaultSsrRangeKey(new Date('2026-06-29T12:00:58Z'))
    expect(a.anchor).toBe(b.anchor)
    expect(a.anchor).toBe('2026-06-29T12:00:00Z')
  })
})

describe('Performance SSR-seed key ↔ client first-paint key byte-match', () => {
  it('the page.tsx seed key === the client key', async () => {
    const { hashKey } = await import('@tanstack/react-query')
    const { resolveDefaultSsrRangeKey, PERFORMANCE_SSR_DEFAULTS } = await import('@/lib/ssr/upstream')

    const serviceId = 'svc-1'
    const { rangeToken, anchor } = resolveDefaultSsrRangeKey(new Date('2026-06-29T12:00:30Z'))

    const seedKey = ['performance', 'aggregates', serviceId, rangeToken, anchor, {}, PERFORMANCE_SSR_DEFAULTS.sortBy]
    const clientKey = ['performance', 'aggregates', serviceId, rangeToken, anchor, {}, 'p99']
    expect(hashKey(seedKey)).toBe(hashKey(clientKey))
  })
})

describe('fetchPerformanceServerSide (performance SSR)', () => {
  it('returns null without a serviceId (no upstream call)', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    const { fetchPerformanceServerSide } = await import('@/lib/ssr/upstream')
    expect(await fetchPerformanceServerSide(undefined)).toBeNull()
  })

  it('fail-closed: no Caddy marker + non-loopback Host → null, never forwarded', async () => {
    setHeaders({ host: 'fastly-log-analytics.global.ssl.fastly.net' })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.end('{}')
      },
      async (srv) => {
        const { fetchPerformanceServerSide } = await import('@/lib/ssr/upstream')
        const out = await fetchPerformanceServerSide('svc-1')
        expect(out).toBeNull()
        expect(srv.requests).toHaveLength(0)
        expect(warn).toHaveBeenCalled()
      },
    )
  })

  it('admin SSH-tunnel: POSTs unified aggregates as admin with service id', async () => {
    setHeaders({ host: 'localhost:3001' })
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('{"ok":true}')
      },
      async (srv) => {
        const {
          fetchPerformanceServerSide,
          PERFORMANCE_SSR_DEFAULTS,
        } = await import('@/lib/ssr/upstream')
        const now = new Date('2026-06-29T12:00:30Z')
        const out = await fetchPerformanceServerSide('svc-1', now)

        expect(out).not.toBeNull()
        expect(out!.data).toEqual({ ok: true })
        expect(out!.rangeToken).toBe('24h')
        expect(out!.anchor).toBe('2026-06-29T12:00:00Z')

        expect(srv.requests).toHaveLength(1)
        const body = JSON.parse(srv.requests[0].body)
        expect(body).toEqual({
          filters: {},
          sort_by: PERFORMANCE_SSR_DEFAULTS.sortBy,
          range_token: '24h',
          anchor: '2026-06-29T12:00:00Z',
        })

        for (const req of srv.requests) {
          expect(req.headers['x-remote-analyst']).toBeUndefined()
          expect(req.headers['x-service-id']).toBe('svc-1')
        }
      },
    )
  })

  it('fails safely: if fetch fails, returns null', async () => {
    setHeaders({ host: 'localhost:3001' })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    try {
      await withTestHttpServer(
        (_req, res) => {
          res.statusCode = 500
          res.end('boom')
        },
        async () => {
          const { fetchPerformanceServerSide } = await import('@/lib/ssr/upstream')
          const out = await fetchPerformanceServerSide('svc-1')
          expect(out).toBeNull()
        },
      )
    } finally {
      warn.mockRestore()
    }
  })
})

describe('Security SSR-seed key ↔ client first-paint key byte-match', () => {
  it('the page.tsx seed key === the client useServiceQuery key', async () => {
    const { hashKey } = await import('@tanstack/react-query')
    const { resolveDefaultSsrRangeKey, SECURITY_SSR_DEFAULTS } = await import('@/lib/ssr/upstream')

    const serviceId = 'svc-1'
    const { rangeToken, anchor } = resolveDefaultSsrRangeKey(new Date('2026-06-29T12:00:30Z'))

    const seedKey = [
      'security',
      'aggregates',
      serviceId,
      rangeToken,
      anchor,
      {},
      SECURITY_SSR_DEFAULTS.bucketSeconds,
    ]

    const clientKey = [
      'security',
      'aggregates',
      serviceId,
      rangeToken,
      anchor,
      {},
      3600,
    ]

    expect(hashKey(seedKey)).toBe(hashKey(clientKey))
  })
})

describe('fetchSecurityServerSide (security SSR)', () => {
  it('returns null without a serviceId (no upstream call)', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    const { fetchSecurityServerSide } = await import('@/lib/ssr/upstream')
    expect(await fetchSecurityServerSide(undefined)).toBeNull()
  })

  it('fail-closed: no Caddy marker + non-loopback Host → null, never forwarded', async () => {
    setHeaders({ host: 'fastly-log-analytics.global.ssl.fastly.net' })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.end('{}')
      },
      async (srv) => {
        const { fetchSecurityServerSide } = await import('@/lib/ssr/upstream')
        const out = await fetchSecurityServerSide('svc-1')
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
        res.end('{"ngwaf_verified_bots":{"rows":[]}}')
      },
      async (srv) => {
        const { fetchSecurityServerSide, SECURITY_SSR_DEFAULTS, SECURITY_SSR_SECTIONS } = await import(
          '@/lib/ssr/upstream'
        )
        const now = new Date('2026-06-29T12:00:30Z')
        const out = await fetchSecurityServerSide('svc-1', now)

        expect(out).not.toBeNull()
        expect(out!.data).toEqual({ ngwaf_verified_bots: { rows: [] } })
        expect(out!.rangeToken).toBe('24h')
        expect(out!.anchor).toBe('2026-06-29T12:00:00Z')

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
        res.end('{"ngwaf_verified_bots":{"rows":[]}}')
      },
      async (srv) => {
        const { fetchSecurityServerSide } = await import('@/lib/ssr/upstream')
        const out = await fetchSecurityServerSide('svc-1')
        expect(out).not.toBeNull()
        const req = srv.lastRequest!
        expect(req.method).toBe('POST')
        expect(req.headers['x-remote-analyst']).toBe('1')
        expect(req.headers['x-service-id']).toBe('svc-1')
      },
    )
  })
})

describe('resolveInsightsDefault (key-match contract)', () => {
  it('mirrors useInsightsDefaults: pickInsightsDefault over earliest_log_at', async () => {
    const { resolveInsightsDefault } = await import('@/lib/ssr/upstream')
    const { pickInsightsDefault, historyHoursFromExtents } = await import(
      "@/lib/utils"
    )

    const earliest = new Date(Date.now() - 10 * 24 * 3600 * 1000).toISOString()
    const expected = pickInsightsDefault(historyHoursFromExtents(earliest))
    const got = resolveInsightsDefault(earliest)
    expect(got).toEqual({
      windowHours: expected.window,
      baselineHours: expected.baseline,
    })
  })

  it('no extents → STATIC_DEFAULT tokens (matches client no-data path)', async () => {
    const { resolveInsightsDefault } = await import('@/lib/ssr/upstream')
    expect(resolveInsightsDefault(null)).toEqual({
      windowHours: '24',
      baselineHours: '168',
    })
    expect(resolveInsightsDefault(undefined)).toEqual({
      windowHours: '24',
      baselineHours: '168',
    })
  })
})

describe('fetchInsightsServerSide (insights SSR)', () => {
  it('returns null without a serviceId (no upstream call)', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    const { fetchInsightsServerSide } = await import('@/lib/ssr/upstream')
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
    setHeaders({ host: 'fastly-log-analytics.global.ssl.fastly.net' })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.end('{"insights":[]}')
      },
      async (srv) => {
        const { fetchInsightsServerSide } = await import('@/lib/ssr/upstream')
        const out = await fetchInsightsServerSide('svc-1', null)
        expect(out).toBeNull()
        expect(srv.requests).toHaveLength(0)
        expect(warn).toHaveBeenCalledWith(expect.stringContaining('non-loopback/absent Host'))
      },
    )
  })

  it('admin SSH-tunnel (loopback Host, no marker): POSTs default body as admin with service id', async () => {
    setHeaders({ host: 'localhost:3001' })
    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('{"insights":[1,2,3]}')
      },
      async (srv) => {
        const { fetchInsightsServerSide } = await import('@/lib/ssr/upstream')
        const earliest = new Date(Date.now() - 10 * 24 * 3600 * 1000).toISOString()
        const out = await fetchInsightsServerSide('svc-1', { earliest_log_at: earliest })

        expect(out).not.toBeNull()
        expect(out!.data).toEqual({ insights: [1, 2, 3] })
        expect(out!.windowHours).toBe('24')
        expect(out!.baselineHours).toBe('168')

        const req = srv.lastRequest!
        expect(req.method).toBe('POST')
        expect(req.headers['content-type']).toContain('application/json')
        expect(JSON.parse(req.body)).toEqual({
          window_size_hrs: 24,
          baseline_hours: 168,
          filters: {},
        })

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
        res.end('{"insights":[1]}')
      },
      async (srv) => {
        const { fetchInsightsServerSide } = await import('@/lib/ssr/upstream')
        const out = await fetchInsightsServerSide('svc-1', null)
        expect(out).not.toBeNull()
        expect(out!.data).toEqual({ insights: [1] })
        const req = srv.lastRequest!
        expect(req.method).toBe('POST')
        expect(req.headers['x-remote-analyst']).toBe('1')
        expect(req.headers['host']).toBe('fastly-log-analytics.global.ssl.fastly.net')
        expect(req.headers['x-service-id']).toBe('svc-1')
      },
    )
  })
})

describe('Origin SSR-seed key ↔ client first-paint key byte-match', () => {
  it('the page.tsx seed key === the client useServiceQuery key', async () => {
    const { hashKey } = await import('@tanstack/react-query')
    const { resolveDefaultSsrRangeKey, ORIGIN_SSR_DEFAULTS, ORIGIN_SSR_SECTIONS } = await import(
      '@/lib/ssr/upstream'
    )

    const serviceId = 'svc-1'
    const { rangeToken, anchor } = resolveDefaultSsrRangeKey(new Date('2026-06-29T12:00:30Z'))

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

    const clientFilterPayload = {}
    const clientBucketMinutes = 60
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
        expect(req.headers['x-remote-analyst']).toBe('1')
        expect(req.headers['host']).toBe('fastly-log-analytics.global.ssl.fastly.net')
        expect(req.headers['x-service-id']).toBe('svc-1')
      },
    )
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

  afterEach(() => {
    vi.restoreAllMocks()
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

  afterEach(() => {
    vi.restoreAllMocks()
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

  afterEach(() => {
    vi.restoreAllMocks()
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

  afterEach(() => {
    vi.restoreAllMocks()
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
