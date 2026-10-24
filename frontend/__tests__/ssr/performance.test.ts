import { describe, expect, it, vi } from 'vitest'

import { setupNextHeadersMock, withTestHttpServer } from '../helpers/ssr'

// performance.ts fetches the unified /api/performance/aggregates selection per cold
// load, sharing one (rangeToken, anchor) pair. These tests pin (1) the
// security contract — the Caddy-marker trust gate must behave IDENTICALLY as
// the other POST SSR callers (refuse no-marker + non-loopback Host;
// analyst-clamp on the marker branch) — and (2) the key-match contract: the (rangeToken, anchor) this
// helper resolves equals what PerformanceClient computes on first paint for
// the key, so the SSR seed byte-matches. A mismatch would double-fetch
// (worse than no SSR).

const { setHeaders } = setupNextHeadersMock()

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
})

describe('SSR-seed key ↔ client first-paint key byte-match', () => {
  it('the page.tsx seed key === the client key', async () => {
    const { hashKey } = await import('@tanstack/react-query')
    const { resolvePerformanceDefaultKey, PERFORMANCE_SSR_DEFAULTS } = await import('@/lib/ssr/performance')

    const serviceId = 'svc-1'
    const { rangeToken, anchor } = resolvePerformanceDefaultKey(new Date('2026-06-29T12:00:30Z'))

    const seedKey = ['performance', 'aggregates', serviceId, rangeToken, anchor, {}, PERFORMANCE_SSR_DEFAULTS.sortBy]
    const clientKey = ['performance', 'aggregates', serviceId, rangeToken, anchor, {}, 'p99']
    expect(hashKey(seedKey)).toBe(hashKey(clientKey))
  })
})

describe('fetchPerformanceServerSide (performance SSR)', () => {
  it('returns null without a serviceId (no upstream call)', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    const { fetchPerformanceServerSide } = await import('@/lib/ssr/performance')
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
        const { fetchPerformanceServerSide } = await import('@/lib/ssr/performance')
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
        } = await import('@/lib/ssr/performance')
        const now = new Date('2026-06-29T12:00:30Z')
        const out = await fetchPerformanceServerSide('svc-1', now)

        expect(out).not.toBeNull()
        expect(out!.performanceData).toEqual({ ok: true })
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
          const { fetchPerformanceServerSide } = await import('@/lib/ssr/performance')
          const out = await fetchPerformanceServerSide('svc-1')
          expect(out).toBeNull()
        },
      )
    } finally {
      warn.mockRestore()
    }
  })
})
