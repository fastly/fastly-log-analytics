import { act, cleanup, renderHook } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { useHeaderBadgeStream } from '@/hooks/useAdminStream'
import { queryKeys } from '@/lib/api'
import type { readIngestEvents } from '@/lib/cloud-engine/telemetry/engine'

const mocks = vi.hoisted(() => ({
  readIngestEvents: vi.fn<typeof readIngestEvents>(),
  observeCatalogCommit: vi.fn(),
  service: { activeServiceId: 'svc-1' as string | null },
}))

vi.mock('@/lib/cloud-engine/mode', () => ({
  IS_CLOUD_MODE: true,
  isCloudMode: () => true,
}))
vi.mock('@/lib/cloud-engine/telemetry/engine', () => ({
  readIngestEvents: mocks.readIngestEvents,
}))
vi.mock('@/lib/cloud-engine/dashboard/catalog-manifest', () => ({
  observeCatalogCommit: mocks.observeCatalogCommit,
}))
vi.mock('@/stores/filterStore', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/stores/filterStore')>()),
  useServiceStore: (selector: (state: typeof mocks.service) => unknown) => selector(mocks.service),
}))

type IngestResult = Awaited<ReturnType<typeof readIngestEvents>>

function emptyResult(): IngestResult {
  return { events: [], nextCursorMs: 0, seenIds: new Set() }
}

function commitResult(cursor: number, eventId = 'event-1'): IngestResult {
  return {
    events: [{
      event_id: eventId, timestamp: cursor, key: 'request-file',
      outcome: 'converted', rows_written: 1, bytes_read: 1, bytes_written: 1,
    }],
    nextCursorMs: cursor,
    seenIds: new Set([eventId]),
  }
}

function deferredResult() {
  let resolve!: (result: IngestResult) => void
  const promise = new Promise<IngestResult>((done) => { resolve = done })
  return { promise, resolve }
}

function setHidden(hidden: boolean) {
  Object.defineProperty(document, 'hidden', { configurable: true, value: hidden })
  document.dispatchEvent(new Event('visibilitychange'))
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

describe('Cloud header ingest-event polling visibility', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-11-06T00:00:00Z'))
    mocks.readIngestEvents.mockReset().mockImplementation(async (_source, options) => ({
      events: [],
      nextCursorMs: options.lastCursorMs ?? 0,
      seenIds: options.seenIds ?? new Set(),
    }))
    mocks.observeCatalogCommit.mockReset()
    mocks.service.activeServiceId = 'svc-1'
    setHidden(false)
  })

  afterEach(() => {
    cleanup()
    setHidden(false)
    vi.useRealTimers()
  })

  it('does not poll hidden tabs and immediately catches up with the retained cursor on return', async () => {
    setHidden(true)
    const stream = mountStream()
    await act(async () => { await vi.advanceTimersByTimeAsync(60_000) })
    expect(mocks.readIngestEvents).not.toHaveBeenCalled()

    const cursor = Date.now()
    mocks.readIngestEvents.mockResolvedValueOnce(commitResult(cursor))
    await act(async () => { setHidden(false); await vi.advanceTimersByTimeAsync(0) })
    expect(mocks.readIngestEvents).toHaveBeenCalledTimes(1)
    expect(stream.queryClient.getQueryState(queryKeys.syncStatus('svc-1'))?.isInvalidated).toBe(true)
    expect(stream.queryClient.getQueryState(queryKeys.syncStatus('svc-2'))?.isInvalidated).toBe(false)

    await act(async () => { setHidden(true); await vi.advanceTimersByTimeAsync(60_000) })
    expect(mocks.readIngestEvents).toHaveBeenCalledTimes(1)
    await act(async () => { setHidden(false); await vi.advanceTimersByTimeAsync(0) })
    expect(mocks.readIngestEvents).toHaveBeenCalledTimes(2)
    expect(mocks.readIngestEvents.mock.calls[1][1]).toMatchObject({
      from: new Date(cursor).toISOString(),
      lastCursorMs: cursor,
      seenIds: new Set(['event-1']),
    })
    await act(async () => { await vi.advanceTimersByTimeAsync(15_000) })
    expect(mocks.readIngestEvents).toHaveBeenCalledTimes(3)
  })

  it('coalesces visibility catch-up without overlapping an in-flight read', async () => {
    const pending = deferredResult()
    mocks.readIngestEvents.mockReturnValueOnce(pending.promise)
    mountStream()
    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    expect(mocks.readIngestEvents).toHaveBeenCalledTimes(1)
    await act(async () => {
      setHidden(true)
      await vi.advanceTimersByTimeAsync(60_000)
      setHidden(false)
      document.dispatchEvent(new Event('visibilitychange'))
    })
    expect(mocks.readIngestEvents).toHaveBeenCalledTimes(1)
    const cursor = Date.now()
    await act(async () => {
      pending.resolve(commitResult(cursor))
      await vi.advanceTimersByTimeAsync(0)
    })
    expect(mocks.readIngestEvents).toHaveBeenCalledTimes(2)
    expect(mocks.readIngestEvents.mock.calls[1][1].lastCursorMs).toBe(cursor)
    await act(async () => { await vi.advanceTimersByTimeAsync(15_000) })
    expect(mocks.readIngestEvents).toHaveBeenCalledTimes(3)
  })

  it('recovers after a transient failure and removes timers, visibility wakeups and late results on cleanup', async () => {
    mocks.readIngestEvents.mockRejectedValueOnce(new Error('temporary listing failure'))
    const pending = deferredResult()
    mocks.readIngestEvents.mockReturnValueOnce(pending.promise)
    const { unmount } = mountStream()
    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    expect(mocks.readIngestEvents).toHaveBeenCalledTimes(1)
    await act(async () => { setHidden(true); await vi.advanceTimersByTimeAsync(60_000) })
    expect(mocks.readIngestEvents).toHaveBeenCalledTimes(1)
    await act(async () => { setHidden(false); await vi.advanceTimersByTimeAsync(0) })
    expect(mocks.readIngestEvents).toHaveBeenCalledTimes(2)
    const signal = mocks.readIngestEvents.mock.calls[1][1].signal
    unmount()
    expect(signal?.aborted).toBe(true)
    await act(async () => {
      pending.resolve(commitResult(Date.now()))
      setHidden(true)
      setHidden(false)
      await vi.advanceTimersByTimeAsync(60_000)
    })
    expect(mocks.readIngestEvents).toHaveBeenCalledTimes(2)
    expect(mocks.observeCatalogCommit).not.toHaveBeenCalled()
    expect(vi.getTimerCount()).toBe(0)
  })

  it('ignores a previous service read after replacement without contaminating its cursor or invalidating the new service', async () => {
    const oldRead = deferredResult()
    const newRead = deferredResult()
    mocks.readIngestEvents.mockReturnValueOnce(oldRead.promise).mockReturnValueOnce(newRead.promise)
    const { rerender, queryClient } = mountStream()
    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    const oldSignal = mocks.readIngestEvents.mock.calls[0][1].signal
    mocks.service.activeServiceId = 'svc-2'
    rerender({ enabled: true })
    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    expect(oldSignal?.aborted).toBe(true)
    await act(async () => {
      mocks.readIngestEvents.mock.calls[0][1].seenIds?.add('stale-event')
      oldRead.resolve(commitResult(Date.now(), 'stale-event'))
      await vi.advanceTimersByTimeAsync(0)
    })
    expect(mocks.observeCatalogCommit).not.toHaveBeenCalled()
    expect(queryClient.getQueryState(queryKeys.syncStatus('svc-1'))?.isInvalidated).toBe(false)
    expect(queryClient.getQueryState(queryKeys.syncStatus('svc-2'))?.isInvalidated).toBe(false)
    await act(async () => {
      newRead.resolve(emptyResult())
      await vi.advanceTimersByTimeAsync(15_000)
    })
    expect(mocks.readIngestEvents.mock.calls[2][1].lastCursorMs).toBe(0)
    expect(mocks.readIngestEvents.mock.calls[2][1].seenIds).toEqual(new Set())
  })
})
