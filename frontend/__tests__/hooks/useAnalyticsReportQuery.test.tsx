/**
 * useAnalyticsReportQuery is the shared query shape for the SSR-seedable
 * analytics pages (origin / performance / security). It resolves the
 * range-wire contract once, assembles the canonical query key, and runs it
 * through useServiceQuery.
 *
 * Tests pin the two invariants pages depend on:
 *  (1) the query-key SHAPE — `[...namespace, serviceId, rangeKey, anchor,
 *      filterPayload, ...keyParts]` — and that on a cold-load default it
 *      hashKey-matches the SSR seed key (so SSR hydration still hits), and
 *  (2) the body-mapper CONTRACT — the queryFn receives the resolved
 *      `rangeBody` (token vs absolute mode) + `rangeToken` + `signal`.
 *
 * @vitest-environment jsdom
 */
import { renderHook, waitFor } from '@testing-library/react'
import { hashKey, QueryClient } from '@tanstack/react-query'
import { describe, it, expect, vi } from 'vitest'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { useAnalyticsReportQuery } from '@/hooks/useAnalyticsReportQuery'

// useServiceQuery gates `enabled` on useIsDataReady — force it ready so the
// queryFn actually runs in these tests.
vi.mock('@/hooks/useIsDataReady', () => ({
  useIsDataReady: () => true,
}))

function makeClient() {
  return createTestQueryClient({ queries: { gcTime: 5_000, staleTime: 0 } })
}

function soleKey(qc: QueryClient): unknown[] {
  const entries = qc.getQueryCache().getAll()
  expect(entries).toHaveLength(1)
  return entries[0].queryKey as unknown[]
}

describe('useAnalyticsReportQuery', () => {
  it('builds [...namespace, serviceId, rangeKey, anchor, filterPayload, ...keyParts] and byte-matches the SSR seed key in token mode', async () => {
    const captured: Array<Record<string, unknown>> = []
    const qc = makeClient()
    const { result } = renderHook(
      () =>
        useAnalyticsReportQuery<{ ok: true }>({
          namespace: ['origin', 'aggregates'],
          serviceId: 'svc-1',
          filterPayload: {},
          range: {
            relativeRange: null,
            isAutoRange: true,
            startTime: '2026-06-29T11:00:00Z',
            endTime: '2026-06-29T12:00:00Z',
            anchor: '2026-06-29T12:00:00Z',
          },
          keyParts: [60, 'ttfb', 'p95'],
          queryFn: async ({ rangeBody, rangeToken }) => {
            captured.push({ rangeBody, rangeToken })
            return { ok: true }
          },
        }),
      { wrapper: makeQueryWrapper(qc) },
    )

    await waitFor(() => expect(result.current.data).toEqual({ ok: true }))

    // Cold-load / auto default resolves the server-reproducible '24h' token.
    expect(captured[0].rangeToken).toBe('24h')
    expect(captured[0].rangeBody).toMatchObject({
      range_token: '24h',
      anchor: '2026-06-29T12:00:00Z',
    })

    // The actual key written to the cache.
    const actualKey = soleKey(qc)
    expect(actualKey).toEqual([
      'origin',
      'aggregates',
      'svc-1',
      '24h', // rangeKey === rangeToken in token mode
      '2026-06-29T12:00:00Z',
      {},
      60,
      'ttfb',
      'p95',
    ])

    // hashKey-equal to the SSR seed key (mirrors lib/ssr/origin.ts +
    // app/origin/page.tsx), so cold-load hydration still hits.
    const ssrSeedKey = [
      'origin',
      'aggregates',
      'svc-1',
      '24h',
      '2026-06-29T12:00:00Z',
      {},
      60,
      'ttfb',
      'p95',
    ]
    expect(hashKey(actualKey)).toBe(hashKey(ssrSeedKey))
  })

  it('passes absolute-mode rangeBody (start_time/end_time, no range_token) and keys on abs:… when a custom range is set', async () => {
    const captured: Array<Record<string, unknown>> = []
    const qc = makeClient()
    const { result } = renderHook(
      () =>
        useAnalyticsReportQuery<{ ok: true }>({
          namespace: ['performance', 'aggregates'],
          serviceId: 'svc-1',
          filterPayload: {},
          range: {
            relativeRange: null,
            isAutoRange: false,
            startTime: '2026-06-01T00:00:00Z',
            endTime: '2026-06-02T00:00:00Z',
            anchor: '2026-06-02T00:00:00Z',
          },
          keyParts: ['p99'],
          queryFn: async ({ rangeBody, rangeToken }) => {
            captured.push({ rangeBody, rangeToken })
            return { ok: true }
          },
        }),
      { wrapper: makeQueryWrapper(qc) },
    )

    await waitFor(() => expect(result.current.data).toEqual({ ok: true }))
    expect(captured[0].rangeToken).toBeNull()
    expect(captured[0].rangeBody).toEqual({
      start_time: '2026-06-01T00:00:00Z',
      end_time: '2026-06-02T00:00:00Z',
    })

    const actualKey = soleKey(qc)
    expect(actualKey).toEqual([
      'performance',
      'aggregates',
      'svc-1',
      'abs:2026-06-01T00:00:00Z|2026-06-02T00:00:00Z',
      '2026-06-02T00:00:00Z',
      {},
      'p99',
    ])
  })

  it('threads the AbortSignal from React Query into the fetcher', async () => {
    let seenSignal: AbortSignal | undefined
    const qc = makeClient()
    const { result } = renderHook(
      () =>
        useAnalyticsReportQuery<{ ok: true }>({
          namespace: ['security', 'aggregates'],
          serviceId: 'svc-1',
          filterPayload: {},
          range: {
            relativeRange: null,
            isAutoRange: true,
            startTime: '2026-06-29T11:00:00Z',
            endTime: '2026-06-29T12:00:00Z',
            anchor: '2026-06-29T12:00:00Z',
          },
          keyParts: [3600],
          queryFn: async ({ signal }) => {
            seenSignal = signal
            return { ok: true }
          },
        }),
      { wrapper: makeQueryWrapper(qc) },
    )

    await waitFor(() => expect(result.current.data).toEqual({ ok: true }))
    expect(seenSignal).toBeInstanceOf(AbortSignal)
  })
})
