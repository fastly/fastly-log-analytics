import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const mockCookies = vi.fn()
const mockHeaders = vi.fn()
vi.mock('next/headers', () => ({
  cookies: () => mockCookies(),
  headers: () => mockHeaders(),
}))

// Caddy-marked inbound (the analyst path): the only inbound shape the
// fail-closed transport gate forwards. Tests that exercise the upstream
// response handling (2xx/401/parse) must present this so the request
// actually reaches the backend.
function caddyMarked(host = 'fastly-log-analytics.global.ssl.fastly.net') {
  return {
    get: (k: string) => {
      const norm = k.toLowerCase()
      if (norm === 'x-proxied-by-caddy') return 'true'
      if (norm === 'host') return host
      return null
    },
  }
}

beforeEach(() => {
  mockCookies.mockReturnValue({ toString: () => 'session=abc123' })
  mockHeaders.mockReturnValue(caddyMarked())
})

afterEach(() => {
  mockCookies.mockReset()
  mockHeaders.mockReset()
  delete process.env.API_PROXY_URL
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
    const http = await import('node:http')
    const server = http.createServer((_req, res) => {
      res.statusCode = 401
      res.end('{"detail":"unauthenticated"}')
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
    try {
      const { fetchTosServerSide } = await import('@/lib/ssr/tos')
      const out = await fetchTosServerSide()
      expect(out).toBe('unauthenticated')
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })

  it('returns null on a parse error from a malformed upstream body', async () => {
    const http = await import('node:http')
    const server = http.createServer((_req, res) => {
      res.statusCode = 200
      res.end('not json {{{')
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    try {
      const { fetchTosServerSide } = await import('@/lib/ssr/tos')
      const out = await fetchTosServerSide()
      expect(out).toBeNull()
      expect(warn).toHaveBeenCalled()
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })

  it('hits the upstream with the expected headers when Caddy marker is present', async () => {
    const http = await import('node:http')
    let capturedHeaders: Record<string, string | string[] | undefined> = {}
    const server = http.createServer((req, res) => {
      capturedHeaders = req.headers
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ version: 'v1', text: 'TOS body' }))
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
    mockCookies.mockReturnValue({ toString: () => 'session=xyz; theme=dark' })
    mockHeaders.mockReturnValue({
      get: (k: string) => {
        const norm = k.toLowerCase()
        if (norm === 'x-proxied-by-caddy') return 'true'
        if (norm === 'host') return 'fastly-log-analytics.global.ssl.fastly.net'
        return null
      },
    })

    try {
      const { fetchTosServerSide } = await import('@/lib/ssr/tos')
      const out = await fetchTosServerSide()
      expect(out).toEqual({ version: 'v1', text: 'TOS body' })
      // Security-critical assertions: when inbound has the Caddy marker,
      // upstream MUST set X-Remote-Analyst AND forward the public Host
      // so the backend classifies as remote-analyst instead of
      // admin-from-loopback.
      expect(capturedHeaders['x-remote-analyst']).toBe('1')
      expect(capturedHeaders.host).toBe('fastly-log-analytics.global.ssl.fastly.net')
      expect(capturedHeaders.cookie).toBe('session=xyz; theme=dark')
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })

  it('fail-closed: non-loopback Host + no Caddy marker → null + warns', async () => {
    // A public visitor reaches SSR but Caddy stopped setting
    // X-Proxied-By-Caddy (Caddyfile drift, header strip). Without the gate
    // the SSR would forward over loopback with no X-Remote-Analyst and the
    // backend would classify it admin-from-loopback, leaking operator data.
    process.env.API_PROXY_URL = 'http://127.0.0.1:1' // must not be reached
    mockCookies.mockReturnValue({ toString: () => '' })
    mockHeaders.mockReturnValue({
      get: (k: string) => {
        const norm = k.toLowerCase()
        if (norm === 'host') return 'fastly-log-analytics.global.ssl.fastly.net'
        return null
      },
    })
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
    const http = await import('node:http')
    let captured: Record<string, string | string[] | undefined> = {}
    const server = http.createServer((req, res) => {
      captured = req.headers
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ version: 'v1', text: 'TOS body' }))
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
    mockCookies.mockReturnValue({ toString: () => '' })
    mockHeaders.mockReturnValue({
      get: (k: string) => (k.toLowerCase() === 'host' ? 'localhost:3001' : null),
    })

    try {
      const { fetchTosServerSide } = await import('@/lib/ssr/tos')
      const out = await fetchTosServerSide()
      expect(out).toEqual({ version: 'v1', text: 'TOS body' })
      expect(captured['x-remote-analyst']).toBeUndefined()
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })

  it('fail-closed: no Caddy marker + absent Host → null (drift / SSRF without a loopback Host)', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1' // must not be reached
    mockCookies.mockReturnValue({ toString: () => '' })
    mockHeaders.mockReturnValue({ get: (_k: string) => null })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { fetchTosServerSide } = await import('@/lib/ssr/tos')
    const out = await fetchTosServerSide()
    expect(out).toBeNull()
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('no X-Proxied-By-Caddy marker'))
  })
})
