import { createServer, type IncomingHttpHeaders } from 'node:http'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { hydrate, QueryClient } from '@tanstack/react-query'

import { resolveRangeWire } from '@/lib/range-wire'
import { seedDehydratedState } from '@/lib/ssr/seed'
import { fetchStreamingServerSide } from '@/lib/ssr/streaming'
import { quantizeAnchor } from '@/lib/time-window'

const { mockHeaders, mockCookies } = vi.hoisted(() => ({
  mockHeaders: vi.fn(),
  mockCookies: vi.fn(),
}))

vi.mock('next/headers', () => ({
  headers: () => mockHeaders(),
  cookies: () => mockCookies(),
}))

afterEach(() => {
  vi.unstubAllEnvs()
  vi.clearAllMocks()
})

describe('Streaming SSR fixture contract', () => {
  it.each([
    { analyst: false, payload: { available: true, overview: { active_sessions: 1234 } } },
    { analyst: true, payload: { available: true, overview: { active_sessions: 1234 } } },
    { analyst: false, payload: { available: false, has_data: false } },
  ])('seeds $payload.available data with analyst=$analyst using the browser cookie and key', async ({ analyst, payload }) => {
    const headers: Record<string, string> = {
      host: analyst ? 'logs.example.com' : 'localhost:13004',
      ...(analyst ? { 'x-proxied-by-caddy': '1' } : {}),
    }
    mockHeaders.mockReturnValue({ get: (key: string) => headers[key.toLowerCase()] ?? null })
    mockCookies.mockReturnValue({
      toString: () => 'fla_streaming_fixture=test-fixture',
      get: () => undefined,
    })

    let capturedHeaders: IncomingHttpHeaders = {}
    let capturedBody = ''
    let capturedMethod: string | undefined
    let capturedPath: string | undefined
    const server = createServer((request, response) => {
      capturedHeaders = request.headers
      capturedMethod = request.method
      capturedPath = request.url
      request.on('data', (chunk: Buffer) => { capturedBody += chunk.toString() })
      request.on('end', () => {
        response.setHeader('Content-Type', 'application/json')
        response.end(JSON.stringify(payload))
      })
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    vi.stubEnv('API_PROXY_URL', `http://127.0.0.1:${port}`)
    vi.stubEnv('ADMIN_SHARED_SECRET', 'test-admin-secret')
    const queryClient = new QueryClient()
    try {
      const now = new Date('2026-10-23T12:00:37Z')
      const seed = await fetchStreamingServerSide('svc-streaming-test', now)
      expect(seed).not.toBeNull()
      if (!seed) throw new Error('Streaming SSR did not produce a seed')
      expect(capturedMethod).toBe('POST')
      expect(capturedPath).toBe('/api/cmcd/aggregates')
      expect(JSON.parse(capturedBody)).toEqual({
        filters: {},
        bucket_seconds: 300,
        top_n: 30,
        range_token: '24h',
        anchor: '2026-10-23T12:00:00Z',
      })
      expect(capturedHeaders.cookie).toBe('fla_streaming_fixture=test-fixture')
      expect(capturedHeaders['x-service-id']).toBe('svc-streaming-test')
      expect(capturedHeaders['x-remote-analyst']).toBe(analyst ? '1' : undefined)
      expect(capturedHeaders['x-admin-token']).toBe(analyst ? undefined : 'test-admin-secret')

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
      hydrate(queryClient, seedDehydratedState(seedKey, seed.data))
      expect(queryClient.getQueryData(browserKey)).toEqual(payload)
      expect(queryClient.getQueryState(browserKey)?.dataUpdatedAt).toBeGreaterThan(0)
    } finally {
      queryClient.clear()
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })
})
