import { describe, expect, it, vi } from 'vitest'

import { setupNextHeadersMock, withTestHttpServer } from '../helpers/ssr'

// security.ts mirrors lib/ssr/origin.ts (the proven template) — a POST caller
// of the shared ./_transport helper. These tests pin (1) the security
// contract — the Caddy-marker trust gate must behave IDENTICALLY for this
// POST as for the GET callers (refuse no-marker + non-loopback Host;
// analyst-clamp on the marker branch) — and (2) the key-match contract: the (rangeToken, anchor) this
// helper resolves equals what SecurityClient computes on first paint, so the
// SSR seed key byte-matches the client first-paint key. A mismatch would
// double-fetch (worse than no SSR).

const { setHeaders } = setupNextHeadersMock()

describe('resolveDefaultSsrRangeKey (key-match contract)', () => {
  it('cold load → rangeToken "24h", anchor floored to the 60s grid', async () => {
    const { resolveDefaultSsrRangeKey } = await import('@/lib/ssr/upstream')
    const { quantizeAnchor } = await import('@/lib/time-window')

    const now = new Date('2026-06-29T12:00:37.123Z')
    const got = resolveDefaultSsrRangeKey(now)
    expect(got.rangeToken).toBe('24h')
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
  it('the page.tsx seed key === the client useServiceQuery key', async () => {
    const { hashKey } = await import('@tanstack/react-query')
    const { resolveDefaultSsrRangeKey } = await import('@/lib/ssr/upstream')
    const { SECURITY_SSR_DEFAULTS } = await import('@/lib/ssr/upstream')

    const serviceId = 'svc-1'
    const { rangeToken, anchor } = resolveDefaultSsrRangeKey(new Date('2026-06-29T12:00:30Z'))

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
