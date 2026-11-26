import { beforeEach, describe, expect, it, vi } from 'vitest'

const { mockSsrUpstreamGet } = vi.hoisted(() => ({
  mockSsrUpstreamGet: vi.fn(),
}))

vi.mock('next/headers', () => ({
  cookies: vi.fn(async () => ({ toString: (): string => '', get: (): undefined => undefined })),
  headers: vi.fn(async () => new Headers()),
}))

vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>()
  return {
    ...actual,
    ssrUpstreamGet: mockSsrUpstreamGet,
  }
})

import { cookies, headers } from 'next/headers'
import {
  fetchAlertsServerSide,
  fetchAssetsServerSide,
  fetchBootstrapServerSide,
  fetchCronRunsServerSide,
  fetchCronScheduleServerSide,
  fetchMetricHistoryBatchServerSide,
  fetchPerformanceServerSide,
  fetchStreamingServerSide,
  fetchUsageLogServerSide,
  INSIGHTS_SSR_TIMEOUT_MS,
  ORIGIN_SSR_TIMEOUT_MS,
  resolvePageServiceId,
  SECURITY_SSR_TIMEOUT_MS,
} from './upstream'

describe('T5-FE-02: SSR resolvePageServiceId service parameter validation', () => {
  beforeEach(() => {
    mockSsrUpstreamGet.mockReset()
    mockSsrUpstreamGet.mockResolvedValue({
      statusCode: 200,
      body: JSON.stringify({
        active_service_id: 'svc_fallback_01',
        services: [{ id: 'svc_first_01' }],
      }),
    })
  })

  it('rejects path-traversal and malformed ?service= parameters and falls back to bootstrap active_service_id', async () => {
    for (const bad of ['..', '../admin/config', 'svc?foo=bar#baz', 'svc/evil', 'a'.repeat(65)]) {
      const resolved = await resolvePageServiceId({ service: bad })
      expect(resolved, bad).toBe('svc_fallback_01')
    }
  })

  it('accepts valid alphanumeric/underscore/hyphen service IDs without extra bootstrap fetch', async () => {
    const resolved = await resolvePageServiceId({ service: 'svc_prod-01' })
    expect(resolved).toBe('svc_prod-01')
    expect(mockSsrUpstreamGet).not.toHaveBeenCalled()
  })
})

describe('R18-FE-01: SSR upstream timeout budgets and bootstrap admin TTL', () => {
  beforeEach(() => {
    mockSsrUpstreamGet.mockReset()
    mockSsrUpstreamGet.mockResolvedValue({
      statusCode: 200,
      body: JSON.stringify({ active_service_id: 'svc-1' }),
    })
  })

  it('caps SSR page and bootstrap timeouts to avoid >1000ms cold FCP', async () => {
    expect(SECURITY_SSR_TIMEOUT_MS).toBe(280)
    expect(INSIGHTS_SSR_TIMEOUT_MS).toBe(280)
    expect(ORIGIN_SSR_TIMEOUT_MS).toBe(280)

    await fetchAssetsServerSide('svc-1')
    expect(mockSsrUpstreamGet).toHaveBeenLastCalledWith(expect.objectContaining({ timeoutMs: 280 }))

    await fetchPerformanceServerSide('svc-1')
    expect(mockSsrUpstreamGet).toHaveBeenLastCalledWith(expect.objectContaining({ timeoutMs: 280 }))

    await fetchStreamingServerSide('svc-1')
    expect(mockSsrUpstreamGet).toHaveBeenLastCalledWith(expect.objectContaining({ timeoutMs: 280 }))

    await fetchMetricHistoryBatchServerSide()
    expect(mockSsrUpstreamGet).toHaveBeenLastCalledWith(expect.objectContaining({ timeoutMs: 280 }))

    await fetchCronRunsServerSide('svc-1')
    expect(mockSsrUpstreamGet).toHaveBeenLastCalledWith(expect.objectContaining({ timeoutMs: 300 }))

    await fetchCronScheduleServerSide('svc-1')
    expect(mockSsrUpstreamGet).toHaveBeenLastCalledWith(expect.objectContaining({ timeoutMs: 300 }))

    await fetchAlertsServerSide('svc-1')
    expect(mockSsrUpstreamGet).toHaveBeenLastCalledWith(expect.objectContaining({ timeoutMs: 300 }))

    await fetchUsageLogServerSide({
      serviceId: 'svc-1',
      start: '2026-11-25T00:00:00Z',
      end: '2026-11-26T00:00:00Z',
      pageSize: 50,
    })
    expect(mockSsrUpstreamGet).toHaveBeenLastCalledWith(expect.objectContaining({ timeoutMs: 300 }))

    vi.mocked(headers).mockResolvedValueOnce(new Headers({ 'x-service-id': 'svc-timeout-check' }))
    await fetchBootstrapServerSide()
    expect(mockSsrUpstreamGet).toHaveBeenLastCalledWith(
      expect.objectContaining({ path: '/api/bootstrap?service_id=svc-timeout-check', timeoutMs: 280 }),
    )
  })

  it('normalizes non-analyst admin cookies in _adminCachedMap while bypassing for analyst cookies and remote analyst payloads', async () => {
    vi.mocked(headers).mockImplementation(
      async () => new Headers({ host: 'localhost:3001', 'x-service-id': 'svc-cookie-norm-test' }),
    )
    try {
      vi.mocked(cookies).mockResolvedValueOnce({
        toString: () => 'fla.sidebarCollapsed=1; playwright_run=ctx1',
        get: () => undefined,
      } as never)
      await fetchBootstrapServerSide()
      expect(mockSsrUpstreamGet).toHaveBeenCalledTimes(1)

      vi.mocked(cookies).mockResolvedValueOnce({
        toString: () => 'fla.sidebarCollapsed=0; playwright_run=ctx2',
        get: () => undefined,
      } as never)
      await fetchBootstrapServerSide()
      expect(mockSsrUpstreamGet).toHaveBeenCalledTimes(1)

      vi.mocked(cookies).mockResolvedValueOnce({
        toString: () => 'analyst_session_id=session-1',
        get: (name: string) => (name === 'analyst_session_id' ? { value: 'session-1' } : undefined),
      } as never)
      await fetchBootstrapServerSide()
      expect(mockSsrUpstreamGet).toHaveBeenCalledTimes(2)

      vi.mocked(headers).mockImplementation(
        async () => new Headers({ host: 'localhost:3001', 'x-service-id': 'svc-remote-flag-test' }),
      )
      mockSsrUpstreamGet.mockResolvedValueOnce({
        statusCode: 200,
        body: JSON.stringify({ active_service_id: 'svc-1', settings: { is_remote_analyst: true } }),
      })
      await fetchBootstrapServerSide()
      expect(mockSsrUpstreamGet).toHaveBeenCalledTimes(3)

      await fetchBootstrapServerSide()
      expect(mockSsrUpstreamGet).toHaveBeenCalledTimes(4)
    } finally {
      vi.mocked(headers).mockImplementation(async () => new Headers())
      vi.mocked(cookies).mockImplementation(async () => ({ toString: (): string => '', get: (): undefined => undefined }) as never)
    }
  })

  it('caches admin bootstrap SSR responses for 10,000ms before refreshing', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-11-26T00:00:00Z'))
    try {
      vi.mocked(headers).mockImplementation(
        async () => new Headers({ host: 'localhost:3001', 'x-service-id': 'svc-admin-ttl-test' }),
      )

      await fetchBootstrapServerSide()
      expect(mockSsrUpstreamGet).toHaveBeenCalledTimes(1)

      vi.advanceTimersByTime(5000)
      await fetchBootstrapServerSide()
      expect(mockSsrUpstreamGet).toHaveBeenCalledTimes(1)

      vi.advanceTimersByTime(5500)
      await fetchBootstrapServerSide()
      expect(mockSsrUpstreamGet).toHaveBeenCalledTimes(2)
    } finally {
      vi.useRealTimers()
      vi.mocked(headers).mockImplementation(async () => new Headers())
    }
  })

  it('serves stale cache immediately between 10s and 600s with deduped background SWR refresh, and blocks past 600s', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-11-26T01:00:00Z'))
    try {
      vi.mocked(headers).mockImplementation(
        async () => new Headers({ host: 'localhost:3001', 'x-service-id': 'svc-admin-swr-test' }),
      )

      mockSsrUpstreamGet.mockResolvedValueOnce({
        statusCode: 200,
        body: JSON.stringify({ active_service_id: 'svc-v1' }),
      })
      expect(await fetchBootstrapServerSide()).toEqual({ active_service_id: 'svc-v1' })
      expect(mockSsrUpstreamGet).toHaveBeenCalledTimes(1)

      // Within 10s: fresh cache hit, no upstream call.
      vi.advanceTimersByTime(5_000)
      expect(await fetchBootstrapServerSide()).toEqual({ active_service_id: 'svc-v1' })
      expect(mockSsrUpstreamGet).toHaveBeenCalledTimes(1)

      // Between 10s and 600s: returns stale v1 immediately while background refresh is in flight.
      let resolveRefresh!: (resp: { statusCode: number; body: string }) => void
      mockSsrUpstreamGet.mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveRefresh = resolve
          }),
      )
      vi.advanceTimersByTime(10_000) // t = 15s
      expect(await fetchBootstrapServerSide()).toEqual({ active_service_id: 'svc-v1' })
      await Promise.resolve()
      expect(mockSsrUpstreamGet).toHaveBeenCalledTimes(2)

      // Concurrent/subsequent stale call while refresh is inflight still returns v1 and dedupes via _adminInflightMap.
      expect(await fetchBootstrapServerSide()).toEqual({ active_service_id: 'svc-v1' })
      expect(mockSsrUpstreamGet).toHaveBeenCalledTimes(2)

      // Finish background refresh -> cache updates to v2.
      resolveRefresh({ statusCode: 200, body: JSON.stringify({ active_service_id: 'svc-v2' }) })
      await Promise.resolve()
      await Promise.resolve()

      expect(await fetchBootstrapServerSide()).toEqual({ active_service_id: 'svc-v2' })
      expect(mockSsrUpstreamGet).toHaveBeenCalledTimes(2)

      // Failed background refresh (null) preserves existing non-null cached value.
      vi.advanceTimersByTime(15_000) // t = 30s (15s since v2 cached)
      mockSsrUpstreamGet.mockResolvedValueOnce(null)
      expect(await fetchBootstrapServerSide()).toEqual({ active_service_id: 'svc-v2' })
      await Promise.resolve()
      await Promise.resolve()
      expect(mockSsrUpstreamGet).toHaveBeenCalledTimes(3)

      // Still within 600s stale TTL (past old 60s window): continues serving stale v2 immediately.
      vi.advanceTimersByTime(100_000) // t = 130s (115s since v2 cached)
      mockSsrUpstreamGet.mockResolvedValueOnce(null)
      expect(await fetchBootstrapServerSide()).toEqual({ active_service_id: 'svc-v2' })
      await Promise.resolve()
      await Promise.resolve()
      expect(mockSsrUpstreamGet).toHaveBeenCalledTimes(4)

      // Past 600s stale TTL: synchronous fetch blocks until upstream resolves.
      vi.advanceTimersByTime(490_000) // t = 620s (605s since v2 cached)
      mockSsrUpstreamGet.mockResolvedValueOnce({
        statusCode: 200,
        body: JSON.stringify({ active_service_id: 'svc-v3' }),
      })
      expect(await fetchBootstrapServerSide()).toEqual({ active_service_id: 'svc-v3' })
      expect(mockSsrUpstreamGet).toHaveBeenCalledTimes(5)
    } finally {
      vi.useRealTimers()
      vi.mocked(headers).mockImplementation(async () => new Headers())
    }
  })
})
