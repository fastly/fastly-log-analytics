import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

// No direct test file existed for _transport.ts before this — its trust-gate
// behavior was only exercised indirectly through each fetcher's own tests
// (__tests__/ssr/dashboard.test.ts etc). This file adds direct coverage for
// the debug-responses cookie → x-debug-responses header propagation added
// for the DiagnosticsPanel/Query-debugging-panel SSR fix — see
// lib/debug-cookie.ts and DiagnosticsPanel.tsx.

const mockCookies = vi.fn()
const mockHeaders = vi.fn()
vi.mock('next/headers', () => ({
  cookies: () => mockCookies(),
  headers: () => mockHeaders(),
}))

function cookieJar(entries: Record<string, string>) {
  return {
    toString: () =>
      Object.entries(entries)
        .map(([k, v]) => `${k}=${v}`)
        .join('; '),
    get: (k: string) => (k in entries ? { name: k, value: entries[k] } : undefined),
  }
}

function headerGetter(entries: Record<string, string>) {
  return {
    get: (k: string) => entries[k.toLowerCase()] ?? null,
  }
}

beforeEach(() => {
  mockCookies.mockReturnValue(cookieJar({}))
  mockHeaders.mockReturnValue(headerGetter({}))
})

afterEach(() => {
  mockCookies.mockReset()
  mockHeaders.mockReset()
  delete process.env.API_PROXY_URL
})

async function withServer(handler: (req: import('node:http').IncomingMessage, res: import('node:http').ServerResponse) => void) {
  const http = await import('node:http')
  const server = http.createServer(handler)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const port = (server.address() as { port: number }).port
  process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
  return {
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  }
}

describe('ssrUpstreamGet — debug-responses cookie propagation', () => {
  it('cookie=1 + loopback Host (admin branch) → upstream request carries x-debug-responses: 1', async () => {
    let captured: Record<string, string | string[] | undefined> = {}
    const server = await withServer((req, res) => {
      captured = req.headers
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/json')
      res.end('{}')
    })
    mockCookies.mockReturnValue(cookieJar({ 'fla.debugResponses': '1' }))
    mockHeaders.mockReturnValue(headerGetter({ host: 'localhost:3001' }))
    try {
      const { ssrUpstreamGet } = await import('@/lib/ssr/_transport')
      const out = await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })
      expect(out).not.toBeNull()
      expect(captured['x-debug-responses']).toBe('1')
    } finally {
      await server.close()
    }
  })

  it('cookie=1 + Caddy marker (analyst branch) → header NOT sent', async () => {
    let captured: Record<string, string | string[] | undefined> = {}
    const server = await withServer((req, res) => {
      captured = req.headers
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/json')
      res.end('{}')
    })
    mockCookies.mockReturnValue(cookieJar({ 'fla.debugResponses': '1' }))
    mockHeaders.mockReturnValue(
      headerGetter({ 'x-proxied-by-caddy': 'true', host: 'fastly-log-analytics.global.ssl.fastly.net' }),
    )
    try {
      const { ssrUpstreamGet } = await import('@/lib/ssr/_transport')
      const out = await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })
      expect(out).not.toBeNull()
      expect(captured['x-debug-responses']).toBeUndefined()
    } finally {
      await server.close()
    }
  })

  it('cookie absent → header not sent (unchanged from before this feature existed)', async () => {
    let captured: Record<string, string | string[] | undefined> = {}
    const server = await withServer((req, res) => {
      captured = req.headers
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/json')
      res.end('{}')
    })
    mockCookies.mockReturnValue(cookieJar({}))
    mockHeaders.mockReturnValue(headerGetter({ host: 'localhost:3001' }))
    try {
      const { ssrUpstreamGet } = await import('@/lib/ssr/_transport')
      const out = await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })
      expect(out).not.toBeNull()
      expect(captured['x-debug-responses']).toBeUndefined()
    } finally {
      await server.close()
    }
  })

  it('cookie=0 → header not sent', async () => {
    let captured: Record<string, string | string[] | undefined> = {}
    const server = await withServer((req, res) => {
      captured = req.headers
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/json')
      res.end('{}')
    })
    mockCookies.mockReturnValue(cookieJar({ 'fla.debugResponses': '0' }))
    mockHeaders.mockReturnValue(headerGetter({ host: 'localhost:3001' }))
    try {
      const { ssrUpstreamGet } = await import('@/lib/ssr/_transport')
      const out = await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })
      expect(out).not.toBeNull()
      expect(captured['x-debug-responses']).toBeUndefined()
    } finally {
      await server.close()
    }
  })
})
