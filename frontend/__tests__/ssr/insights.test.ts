import { describe, expect, it, vi } from 'vitest'

import { setupNextHeadersMock, withTestHttpServer } from '../helpers/ssr'

// insights.ts is the FIRST POST caller of the shared ./_transport helper.
// These tests pin (1) the security contract — the Caddy-marker trust gate must
// behave IDENTICALLY for POST as for the GET callers (refuse no-marker +
// non-loopback Host; analyst-clamp on the marker branch) — and (2) the key-match contract: the windowHours/
// baselineHours the helper resolves equal what the client's useInsightsDefaults
// computes from the same earliest_log_at, so the SSR seed key byte-matches the
// client first-paint key. A mismatch would double-fetch (worse than no SSR).

const { setHeaders } = setupNextHeadersMock()

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
    const { fetchInsightsServerSide } = await import('@/lib/ssr/insights')
    expect(await fetchInsightsServerSide(undefined, null)).toBeNull()
  })

  describe('Insights page hydration', () => {
    it.each([undefined, 'svc-2'])('scopes first-render extents to the bootstrap service (override: %s)', async (service) => {
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
        const { fetchInsightsServerSide } = await import('@/lib/ssr/insights')
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
        const { fetchInsightsServerSide } = await import('@/lib/ssr/insights')
        // ~10 days of history → default tokens window '24' / baseline '168'.
        const earliest = new Date(Date.now() - 10 * 24 * 3600 * 1000).toISOString()
        const out = await fetchInsightsServerSide('svc-1', { earliest_log_at: earliest })

        expect(out).not.toBeNull()
        expect(out!.data).toEqual({ insights: [1, 2, 3] })
        expect(out!.windowHours).toBe('24')
        expect(out!.baselineHours).toBe('168')

        const req = srv.lastRequest!
        // POST with the exact client body shape.
        expect(req.method).toBe('POST')
        expect(req.headers['content-type']).toContain('application/json')
        expect(JSON.parse(req.body)).toEqual({
          window_size_hrs: 24,
          baseline_hours: 168,
          filters: {},
        })

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
        res.end('{"insights":[1]}')
      },
      async (srv) => {
        const { fetchInsightsServerSide } = await import('@/lib/ssr/insights')
        const out = await fetchInsightsServerSide('svc-1', null)
        expect(out).not.toBeNull()
        expect(out!.data).toEqual({ insights: [1] })
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
