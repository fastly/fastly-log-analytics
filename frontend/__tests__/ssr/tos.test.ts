import { describe, expect, it, vi } from 'vitest'

import { setupNextHeadersMock, withTestHttpServer } from '../helpers/ssr'

const { setHeaders, setCookies } = setupNextHeadersMock({
  cookies: 'session=abc123',
  headers: {
    'x-proxied-by-caddy': 'true',
    host: 'fastly-log-analytics.global.ssl.fastly.net',
  },
})

// Mirrors the testing rationale in bootstrap.test.ts — the helper uses
// node:http.request (NOT fetch), end-to-end tests against a real loopback
// socket are more robust than module-transform mocking of node:http, and
// the focus here is on failure-path collapse so a backend outage / misconfig
// never breaks SSR rendering.

describe('fetchTosServerSide', () => {
  it('returns null when API_PROXY_URL is unset (pure `next dev` outside docker compose)', async () => {
    const { fetchTosServerSide } = await import('@/lib/ssr/tos')
    const out = await fetchTosServerSide()
    expect(out).toBeNull()
  })

  it('returns null on a non-2xx, non-401 upstream response', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1' // refused — guaranteed network failure path
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { fetchTosServerSide } = await import('@/lib/ssr/tos')
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
        const { fetchTosServerSide } = await import('@/lib/ssr/tos')
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
        const { fetchTosServerSide } = await import('@/lib/ssr/tos')
        const out = await fetchTosServerSide()
        expect(out).toBeNull()
        expect(warn).toHaveBeenCalled()
      },
    )
  })

  it('hits the upstream with the expected headers when Caddy marker is present', async () => {
    setCookies('session=xyz; theme=dark')
    setHeaders({
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
        const { fetchTosServerSide } = await import('@/lib/ssr/tos')
        const out = await fetchTosServerSide()
        expect(out).toEqual({ version: 'v1', text: 'TOS body' })
        // Security-critical assertions: when inbound has the Caddy marker,
        // upstream MUST set X-Remote-Analyst AND forward the public Host
        // so the backend classifies as remote-analyst instead of
        // admin-from-loopback.
        const capturedHeaders = srv.lastRequest!.headers
        expect(capturedHeaders['x-remote-analyst']).toBe('1')
        expect(capturedHeaders.host).toBe('fastly-log-analytics.global.ssl.fastly.net')
        expect(capturedHeaders.cookie).toBe('session=xyz; theme=dark')
      },
    )
  })

  it('fail-closed: non-loopback Host + no Caddy marker → null + warns', async () => {
    // A public visitor reaches SSR but Caddy stopped setting
    // X-Proxied-By-Caddy (Caddyfile drift, header strip). Without the gate
    // the SSR would forward over loopback with no X-Remote-Analyst and the
    // backend would classify it admin-from-loopback, leaking operator data.
    process.env.API_PROXY_URL = 'http://127.0.0.1:1' // must not be reached
    setCookies('')
    setHeaders({ host: 'fastly-log-analytics.global.ssl.fastly.net' })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { fetchTosServerSide } = await import('@/lib/ssr/tos')
    const out = await fetchTosServerSide()
    expect(out).toBeNull()
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('no X-Proxied-By-Caddy marker'))
  })

  it('admin SSH-tunnel: loopback Host + no Caddy marker → forwards as admin (no X-Remote-Analyst)', async () => {
    // The legitimate admin path: SSH tunnel → Next.js with a loopback Host
    // and no Caddy marker. The gate permits loopback Hosts so admin pages
    // keep their SSR pre-render; the backend classifies the loopback peer as
    // admin. No X-Remote-Analyst is forwarded (that would mis-scope to the
    // analyst session). Only non-loopback / absent Hosts without the marker
    // are refused (drift / SSRF).
    setCookies('')
    setHeaders({ host: 'localhost:3001' })

    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify({ version: 'v1', text: 'TOS body' }))
      },
      async (srv) => {
        const { fetchTosServerSide } = await import('@/lib/ssr/tos')
        const out = await fetchTosServerSide()
        expect(out).toEqual({ version: 'v1', text: 'TOS body' })
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBeUndefined()
      },
    )
  })

  it('fail-closed: no Caddy marker + absent Host → null (drift / SSRF without a loopback Host)', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1' // must not be reached
    setCookies('')
    setHeaders({})
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { fetchTosServerSide } = await import('@/lib/ssr/tos')
    const out = await fetchTosServerSide()
    expect(out).toBeNull()
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('no X-Proxied-By-Caddy marker'))
  })
})
