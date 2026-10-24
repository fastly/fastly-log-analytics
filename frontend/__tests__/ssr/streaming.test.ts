import { describe, expect, it } from 'vitest'
import { hydrate, QueryClient } from '@tanstack/react-query'

import { setupNextHeadersMock, withTestHttpServer } from '../helpers/ssr'
import { resolveRangeWire } from '@/lib/range-wire'
import { seedDehydratedState } from '@/lib/ssr/seed'
import { fetchStreamingServerSide } from '@/lib/ssr/streaming'
import { quantizeAnchor } from '@/lib/time-window'

const { setHeaders, setCookies } = setupNextHeadersMock()

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
