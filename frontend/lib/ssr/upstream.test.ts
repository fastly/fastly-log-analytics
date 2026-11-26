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
      expect.objectContaining({ path: '/api/bootstrap?service_id=svc-timeout-check', timeoutMs: 450 }),
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
})
