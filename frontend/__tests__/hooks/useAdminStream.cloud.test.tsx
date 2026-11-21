import { act, cleanup, renderHook } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { useHeaderBadgeStream } from '@/hooks/useAdminStream'
import { queryKeys } from '@/lib/api'

const mocks = vi.hoisted(() => ({
  fetch: vi.fn<typeof fetch>(),
  observeCatalogCommit: vi.fn(),
  notifyDashboardCatalogChange: vi.fn(),
  service: { activeServiceId: 'svc-1' as string | null },
}))

vi.mock('@/lib/cloud-engine/mode', () => ({
  IS_CLOUD_MODE: true,
  isCloudMode: () => true,
}))
vi.mock('@/lib/cloud-engine/dashboard/catalog-manifest', () => ({
  observeCatalogCommit: mocks.observeCatalogCommit,
}))
vi.mock('@/lib/cloud-engine/dashboard/client', () => ({
  notifyDashboardCatalogChange: mocks.notifyDashboardCatalogChange,
}))
vi.mock('@/stores/filterStore', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/stores/filterStore')>()),
  useServiceStore: (selector: (state: typeof mocks.service) => unknown) => selector(mocks.service),
}))

function frame(
  event: string,
  cursor: string,
  retry = 5000,
  data: Record<string, unknown> = {
    cursor,
    generation: 42,
    manifest: 'processed/_catalog/current.json',
  },
) {
  return `retry: ${retry}\nid: ${cursor}\nevent: ${event}\ndata: ${JSON.stringify(data)}\n\n`
}

function response(body: string, status = 200) {
  return new Response(body, { status })
}

function deferredResponse() {
  let resolve!: (value: Response) => void
  const promise = new Promise<Response>((done) => { resolve = done })
  return { promise, resolve }
}

function setHidden(hidden: boolean) {
  Object.defineProperty(document, 'hidden', { configurable: true, value: hidden })
  document.dispatchEvent(new Event('visibilitychange'))
}

function requestUrls() {
  return mocks.fetch.mock.calls.map(([url]) => String(url))
}

function mountStream() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: Infinity } },
  })
  queryClient.setQueryData(queryKeys.syncStatus('svc-1'), { local_rows: 1 })
  queryClient.setQueryData(queryKeys.syncStatus('svc-2'), { local_rows: 2 })
  const hook = renderHook(({ enabled }) => useHeaderBadgeStream(enabled), {
    initialProps: { enabled: true },
    wrapper: ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    ),
  })
  return { ...hook, queryClient }
}

describe('Cloud header ingest events', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.stubGlobal('fetch', mocks.fetch)
    mocks.fetch.mockReset().mockResolvedValue(response('retry: 5000\n\n'))
    mocks.observeCatalogCommit.mockReset()
    mocks.notifyDashboardCatalogChange.mockReset()
    mocks.service.activeServiceId = 'svc-1'
    setHidden(false)
  })

  afterEach(() => {
    cleanup()
    setHidden(false)
    vi.unstubAllGlobals()
    vi.useRealTimers()
  })

  it('does not request while hidden and catches up with the retained SSE cursor', async () => {
    setHidden(true)
    const stream = mountStream()
    await act(async () => { await vi.advanceTimersByTimeAsync(60_000) })
    expect(mocks.fetch).not.toHaveBeenCalled()

    mocks.fetch.mockResolvedValueOnce(response(frame('commit', 'svc-1:42')))
    await act(async () => { setHidden(false); await vi.advanceTimersByTimeAsync(0) })
    expect(mocks.fetch).toHaveBeenCalledTimes(1)
    expect(mocks.fetch).toHaveBeenCalledWith(
      expect.stringMatching(/\/api\/ingest\/events$/),
      expect.objectContaining({
        headers: expect.objectContaining({ Accept: 'text/event-stream', 'x-service-id': 'svc-1' }),
        signal: expect.any(AbortSignal),
      }),
    )
    expect(stream.queryClient.getQueryState(queryKeys.syncStatus('svc-1'))?.isInvalidated).toBe(true)
    expect(stream.queryClient.getQueryState(queryKeys.syncStatus('svc-2'))?.isInvalidated).toBe(false)

    await act(async () => { setHidden(true); await vi.advanceTimersByTimeAsync(60_000) })
    expect(mocks.fetch).toHaveBeenCalledTimes(1)
    await act(async () => { setHidden(false); await vi.advanceTimersByTimeAsync(0) })
    expect(mocks.fetch).toHaveBeenCalledTimes(2)
    expect(mocks.fetch.mock.calls[1][0]).toContain('/api/ingest/events?cursor=svc-1%3A42')
    await act(async () => { await vi.advanceTimersByTimeAsync(5_000) })
    expect(mocks.fetch).toHaveBeenCalledTimes(3)
    expect(requestUrls()).not.toEqual(expect.arrayContaining([
      expect.stringContaining('/api/logs/cron_runs/live'),
      expect.stringContaining('/api/files'),
    ]))
  })

  it('coalesces visibility catch-up without overlapping an in-flight request', async () => {
    const pending = deferredResponse()
    mocks.fetch.mockReturnValueOnce(pending.promise)
    mountStream()
    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    expect(mocks.fetch).toHaveBeenCalledTimes(1)
    await act(async () => {
      setHidden(true)
      await vi.advanceTimersByTimeAsync(60_000)
      setHidden(false)
      document.dispatchEvent(new Event('visibilitychange'))
    })
    expect(mocks.fetch).toHaveBeenCalledTimes(1)
    await act(async () => {
      pending.resolve(response(frame('ready', 'svc-1:41')))
      await vi.advanceTimersByTimeAsync(0)
    })
    expect(mocks.fetch).toHaveBeenCalledTimes(2)
    expect(mocks.fetch.mock.calls[1][0]).toContain('/api/ingest/events?cursor=svc-1%3A41')
  })

  it('synchronizes ready, commit, and resync events with page and worker catalogs', async () => {
    mocks.fetch
      .mockResolvedValueOnce(response(frame('ready', 'svc-1:41')))
      .mockResolvedValueOnce(response(frame('commit', 'svc-1:42')))
      .mockResolvedValueOnce(response(frame('resync', 'svc-1:50')))
    const { queryClient } = mountStream()
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries')

    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    expect(mocks.observeCatalogCommit).toHaveBeenCalledWith(
      expect.any(Function),
      { type: 'ready', generation: 42, manifest: 'processed/_catalog/current.json' },
    )
    expect(mocks.notifyDashboardCatalogChange).toHaveBeenCalledTimes(1)
    expect(invalidate).not.toHaveBeenCalled()
    await act(async () => { await vi.advanceTimersByTimeAsync(5_000) })
    expect(mocks.observeCatalogCommit).toHaveBeenCalledWith(
      expect.any(Function),
      { type: 'commit', generation: 42, manifest: 'processed/_catalog/current.json' },
    )
    expect(invalidate).toHaveBeenCalledTimes(1)
    expect(mocks.notifyDashboardCatalogChange).toHaveBeenCalledTimes(2)
    await act(async () => { await vi.advanceTimersByTimeAsync(5_000) })
    expect(mocks.observeCatalogCommit).toHaveBeenCalledWith(
      expect.any(Function),
      { type: 'resync', generation: 42, manifest: 'processed/_catalog/current.json' },
    )
    expect(invalidate).toHaveBeenCalledTimes(2)
    expect(mocks.notifyDashboardCatalogChange).toHaveBeenCalledTimes(3)
    expect(requestUrls().every((url) => url.includes('/api/ingest/events'))).toBe(true)
  })

  it('uses bounded server reconnect delays', async () => {
    mocks.fetch
      .mockResolvedValueOnce(response(frame('ready', 'svc-1:1', 20)))
      .mockResolvedValueOnce(response(frame('ready', 'svc-1:2', 90_000)))
    mountStream()
    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    await act(async () => { await vi.advanceTimersByTimeAsync(999) })
    expect(mocks.fetch).toHaveBeenCalledTimes(1)
    await act(async () => { await vi.advanceTimersByTimeAsync(1) })
    expect(mocks.fetch).toHaveBeenCalledTimes(2)
    await act(async () => { await vi.advanceTimersByTimeAsync(29_999) })
    expect(mocks.fetch).toHaveBeenCalledTimes(2)
    await act(async () => { await vi.advanceTimersByTimeAsync(1) })
    expect(mocks.fetch).toHaveBeenCalledTimes(3)
  })

  it('stops polling after an authentication failure', async () => {
    mocks.fetch.mockResolvedValueOnce(response('', 401))
    mountStream()

    await act(async () => { await vi.advanceTimersByTimeAsync(60_000) })

    expect(mocks.fetch).toHaveBeenCalledTimes(1)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('recovers after a transient failure and removes timers, wakeups, and late results on cleanup', async () => {
    mocks.fetch.mockRejectedValueOnce(new Error('temporary event failure'))
    const pending = deferredResponse()
    mocks.fetch.mockReturnValueOnce(pending.promise)
    const { unmount } = mountStream()
    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    expect(mocks.fetch).toHaveBeenCalledTimes(1)
    await act(async () => { setHidden(true); await vi.advanceTimersByTimeAsync(60_000) })
    expect(mocks.fetch).toHaveBeenCalledTimes(1)
    await act(async () => { setHidden(false); await vi.advanceTimersByTimeAsync(0) })
    expect(mocks.fetch).toHaveBeenCalledTimes(2)
    const signal = mocks.fetch.mock.calls[1][1]?.signal
    unmount()
    expect(signal?.aborted).toBe(true)
    await act(async () => {
      pending.resolve(response(frame('commit', 'svc-1:42')))
      setHidden(true)
      setHidden(false)
      await vi.advanceTimersByTimeAsync(60_000)
    })
    expect(mocks.fetch).toHaveBeenCalledTimes(2)
    expect(mocks.observeCatalogCommit).not.toHaveBeenCalled()
    expect(vi.getTimerCount()).toBe(0)
  })

  it('aborts an old service request and starts the replacement with a fresh cursor', async () => {
    const oldRead = deferredResponse()
    const newRead = deferredResponse()
    mocks.fetch.mockReturnValueOnce(oldRead.promise).mockReturnValueOnce(newRead.promise)
    const { rerender, queryClient } = mountStream()
    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    const oldSignal = mocks.fetch.mock.calls[0][1]?.signal
    mocks.service.activeServiceId = 'svc-2'
    rerender({ enabled: true })
    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    expect(oldSignal?.aborted).toBe(true)
    expect(mocks.fetch.mock.calls[1][0]).toMatch(/\/api\/ingest\/events$/)
    await act(async () => {
      oldRead.resolve(response(frame('commit', 'svc-1:99')))
      await vi.advanceTimersByTimeAsync(0)
    })
    expect(mocks.observeCatalogCommit).not.toHaveBeenCalled()
    expect(queryClient.getQueryState(queryKeys.syncStatus('svc-1'))?.isInvalidated).toBe(false)
    expect(queryClient.getQueryState(queryKeys.syncStatus('svc-2'))?.isInvalidated).toBe(false)
    await act(async () => {
      newRead.resolve(response('retry: 5000\n\n'))
      await vi.advanceTimersByTimeAsync(5_000)
    })
    expect(mocks.fetch.mock.calls[2][0]).toMatch(/\/api\/ingest\/events$/)
  })
})
