import { describe, expect, it, vi } from 'vitest'

import { setupNextHeadersMock, withTestHttpServer } from '../helpers/ssr'

const { setHeaders, setCookies } = setupNextHeadersMock({ cookies: 'session=abc123' })

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
    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
    const out = await fetchBootstrapServerSide()
    expect(out).toBeNull()
  })

  it('returns null on a non-2xx upstream response', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1' // refused — guaranteed network failure path
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
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
        const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
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
        const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
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
    // A public visitor reaches SSR but Caddy stopped setting
    // X-Proxied-By-Caddy (Caddyfile drift, header strip, replaced reverse
    // proxy). Without the gate the SSR would forward over loopback with no
    // X-Remote-Analyst, the backend would classify it admin-from-loopback,
    // and the admin bootstrap (cron schedules, audit logs, ops metrics)
    // would land in public-facing HTML. Collapse to null → client fetch.
    process.env.API_PROXY_URL = 'http://127.0.0.1:1' // must not be reached
    setCookies('')
    setHeaders({ host: 'fastly-log-analytics.global.ssl.fastly.net' })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
    const out = await fetchBootstrapServerSide()
    expect(out).toBeNull()
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('no X-Proxied-By-Caddy marker'))
  })

  it('admin SSH-tunnel: loopback Host + no Caddy marker → forwards as admin (no X-Remote-Analyst)', async () => {
    // The legitimate admin path keeps bootstrap SSR: a loopback Host with no
    // Caddy marker is permitted and forwarded as admin (loopback peer). No
    // X-Remote-Analyst (that would mis-scope to an analyst session). Only
    // non-loopback / absent Hosts without the marker are refused.
    setCookies('')
    setHeaders({ host: 'localhost:3001' })

    await withTestHttpServer(
      (_req, res) => {
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify({ active_service_id: 'svc-1' }))
      },
      async (srv) => {
        const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
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
    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
    const out = await fetchBootstrapServerSide()
    expect(out).toBeNull()
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('no X-Proxied-By-Caddy marker'))
  })
})
