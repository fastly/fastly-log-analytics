import { EventEmitter } from 'node:events'

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const mockCookies = vi.fn()
const mockHeaders = vi.fn()
vi.mock('next/headers', () => ({
  cookies: () => mockCookies(),
  headers: () => mockHeaders(),
}))

beforeEach(() => {
  mockCookies.mockReturnValue({ toString: () => 'session=abc123' })
  mockHeaders.mockReturnValue({ get: (_k: string) => null })
})

afterEach(() => {
  mockCookies.mockReset()
  mockHeaders.mockReset()
  delete process.env.API_PROXY_URL
})

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
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'  // refused — guaranteed network failure path
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
      const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
      const out = await fetchBootstrapServerSide()
      expect(out).toBeNull()
      expect(warn).toHaveBeenCalled()
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })

  it('hits the upstream with the expected headers (smoke — verifies node:http path runs)', async () => {
    // Stand up a one-shot HTTP server. Capture the inbound headers
    // and return a stub response so the helper's JSON.parse succeeds.
    // Asserts the header-forwarding contract without needing to mock
    // node:http modules (mocking nested core module imports across
    // vitest's transform layers is fragile — go end-to-end against
    // a real loopback socket instead).
    const http = await import('node:http')
    let capturedHeaders: Record<string, string | string[] | undefined> = {}
    const server = http.createServer((req, res) => {
      capturedHeaders = req.headers
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ active_service_id: 'svc-1' }))
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
      const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
      const out = await fetchBootstrapServerSide()
      expect(out).toEqual({ active_service_id: 'svc-1' })
      // Security-critical assertions: when inbound has the Caddy
      // marker, upstream MUST set X-Remote-Analyst AND forward the
      // public Host so the backend classifies as remote-analyst
      // instead of admin-from-loopback.
      expect(capturedHeaders['x-remote-analyst']).toBe('1')
      expect(capturedHeaders.host).toBe('fastly-log-analytics.global.ssl.fastly.net')
      expect(capturedHeaders.cookie).toBe('session=xyz; theme=dark')
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })

  it('admin SSH-tunnel path: no Caddy header inbound → no X-Remote-Analyst, no Host override', async () => {
    const http = await import('node:http')
    let capturedHeaders: Record<string, string | string[] | undefined> = {}
    const server = http.createServer((req, res) => {
      capturedHeaders = req.headers
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ active_service_id: 'svc-1' }))
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
    mockCookies.mockReturnValue({ toString: () => '' })
    mockHeaders.mockReturnValue({ get: (_k: string) => null })

    try {
      const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
      await fetchBootstrapServerSide()
      expect(capturedHeaders['x-remote-analyst']).toBeUndefined()
      // Host header defaults to whatever node:http sets from the URL
      // (127.0.0.1:<port>), NOT the public endpoint. That's what
      // keeps the backend's _local_host_allowed branch happy for the
      // admin path.
      expect(capturedHeaders.host).toMatch(/^127\.0\.0\.1:\d+$/)
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })
})
