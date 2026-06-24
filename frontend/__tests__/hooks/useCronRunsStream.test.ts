/**
 * useCronRunsStream — push channel that invalidates the cron-logs +
 * last-sync React Query keys on every cron lifecycle event.
 *
 * @vitest-environment jsdom
 */
import { renderHook, waitFor, act } from '@testing-library/react'
import { QueryClient } from '@tanstack/react-query'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

vi.mock('@/lib/api', () => ({
  getApiBase: () => 'http://test',
}))

let mockState: {
  activeServiceId: string | null
  services: Array<{ id: string; name: string; accessLevel: 'read_write' | 'read_only' }>
} = {
  activeServiceId: 'svc-1',
  services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_write' }],
}

vi.mock('@/stores/serviceStore', () => {
  const useServiceStore: any = vi.fn((selector?: (s: any) => any) =>
    selector ? selector(mockState) : mockState,
  )
  useServiceStore.getState = () => mockState
  return { useServiceStore }
})

function makeStreamResponse(messages: string[]): Response {
  const enc = new TextEncoder()
  let i = 0
  const stream = new ReadableStream({
    pull(controller) {
      if (i >= messages.length) {
        controller.close()
        return
      }
      controller.enqueue(enc.encode(messages[i]))
      i += 1
    },
  })
  return new Response(stream, { status: 200 })
}

function makeQueryClient() {
  return createTestQueryClient({ queries: { gcTime: 60_000, staleTime: 0 } })
}

function wrapperWith(qc: QueryClient) {
  return makeQueryWrapper(qc)
}

beforeEach(() => {
  vi.stubGlobal('fetch', vi.fn())
  mockState = {
    activeServiceId: 'svc-1',
    services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_write' }],
  }
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('useCronRunsStream', () => {
  it('invalidates cron-logs queries on every event (filter-agnostic)', async () => {
    // CRLF separator — matches sse-starlette's wire format. Same
    // regression shape the CRLF test in useSSE.test.ts pins.
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([
        `data: ${JSON.stringify({ event: 'cron_run_changed', run_id: 1, task: 'commit', status: 'success' })}\r\n\r\n`,
      ]),
    )

    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const { useCronRunsStream } = await import('@/hooks/useCronRunsStream')
    renderHook(() => useCronRunsStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => {
      const keys = spy.mock.calls.map(c => JSON.stringify(c[0]?.queryKey))
      expect(keys).toContain(JSON.stringify(['admin', 'cron-logs', 'svc-1']))
      expect(keys).toContain(JSON.stringify(['admin', 'cron-logs-recent', 'svc-1']))
    })

    // A non-sync task event must NOT invalidate the last-sync key.
    const lastSyncCalls = spy.mock.calls
      .map(c => JSON.stringify(c[0]?.queryKey))
      .filter(k => k === JSON.stringify(['last-sync', 'svc-1']))
    expect(lastSyncCalls).toHaveLength(0)
  })

  it('invalidates [last-sync, svc] when a sync run COMPLETES (status !== "running")', async () => {
    // Updated 2026-06-16: the hook only refreshes the Last Sync badge
    // on a terminal status (success / error / partial_success) so the
    // "Last Sync: running" UX label stays anchored to the prior
    // completion's started_at while the new run is in flight. A
    // running-status event must NOT reset the timer.
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([
        `data: ${JSON.stringify({ event: 'cron_run_changed', run_id: 2, task: 'sync', status: 'success' })}\r\n\r\n`,
      ]),
    )

    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const { useCronRunsStream } = await import('@/hooks/useCronRunsStream')
    renderHook(() => useCronRunsStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => {
      const keys = spy.mock.calls.map(c => JSON.stringify(c[0]?.queryKey))
      expect(keys).toContain(JSON.stringify(['last-sync', 'svc-1']))
    })
  })

  it('invalidates [last-sync] for a running-status sync event (so the badge can flip to "running")', async () => {
    // The badge's status-ternary renders text-vs-TimeAgo from
    // lastSync.status; without a refetch on the running event the
    // cache never sees status='running' and the badge stays on
    // TimeAgo. The "timer-restart" concern is moot — when
    // status='running' the badge shows the literal word "running"
    // instead of a counter. The matching a11y live-region announcement
    // in SyncStatusBadge.tsx also depends on this transition firing.
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([
        `data: ${JSON.stringify({ event: 'cron_run_changed', run_id: 2, task: 'sync', status: 'running' })}\r\n\r\n`,
      ]),
    )

    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const { useCronRunsStream } = await import('@/hooks/useCronRunsStream')
    renderHook(() => useCronRunsStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => {
      const keys = spy.mock.calls.map(c => JSON.stringify(c[0]?.queryKey))
      expect(keys).toContain(JSON.stringify(['last-sync', 'svc-1']))
    })
    // The heavy 500-row table key should NOT have been invalidated for
    // a running event — _state.ts handles that path separately.
    const tableCalls = spy.mock.calls
      .map(c => JSON.stringify(c[0]?.queryKey))
      .filter(k => k === JSON.stringify(['admin', 'cron-logs', 'svc-1']))
    expect(tableCalls).toHaveLength(0)
  })

  it('does NOT open a connection when disabled is false', async () => {
    const qc = makeQueryClient()
    const { useCronRunsStream } = await import('@/hooks/useCronRunsStream')
    renderHook(() => useCronRunsStream(false), { wrapper: wrapperWith(qc) })
    await new Promise(r => setTimeout(r, 30))
    expect(fetch).not.toHaveBeenCalled()
  })

  it('does NOT open a connection when activeServiceId is null', async () => {
    mockState = { activeServiceId: null, services: [] }
    const qc = makeQueryClient()
    const { useCronRunsStream } = await import('@/hooks/useCronRunsStream')
    renderHook(() => useCronRunsStream(true), { wrapper: wrapperWith(qc) })
    await new Promise(r => setTimeout(r, 30))
    expect(fetch).not.toHaveBeenCalled()
  })

  it('aborts the connection on unmount', async () => {
    let abortSignal: AbortSignal | undefined
    vi.mocked(fetch).mockImplementation(async (_url: any, init?: any) => {
      abortSignal = (init as RequestInit | undefined)?.signal as AbortSignal
      const stream = new ReadableStream<Uint8Array>({ start() { /* never closes */ } })
      return new Response(stream, { status: 200 })
    })

    const qc = makeQueryClient()
    const { useCronRunsStream } = await import('@/hooks/useCronRunsStream')
    const { unmount } = renderHook(() => useCronRunsStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => expect(fetch).toHaveBeenCalled())
    expect(abortSignal?.aborted).toBe(false)
    act(() => unmount())
    expect(abortSignal?.aborted).toBe(true)
  })

  it('invalidates [admin, cron-schedule, svc] on every cron event (start or complete)', async () => {
    // The cron-schedule tiles surface last_run / next_run / status
    // per task; any cron lifecycle event advances at least one of
    // those fields. Unconditional invalidation (not gated on
    // completed) means tiles flip to "running" immediately and back
    // when the task lands.
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([
        `data: ${JSON.stringify({ event: 'cron_run_changed', run_id: 10, task: 'commit', status: 'running' })}\r\n\r\n`,
      ]),
    )

    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const { useCronRunsStream } = await import('@/hooks/useCronRunsStream')
    renderHook(() => useCronRunsStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => {
      const keys = spy.mock.calls.map(c => JSON.stringify(c[0]?.queryKey))
      expect(keys).toContain(JSON.stringify(['admin', 'cron-schedule', 'svc-1']))
    })
  })

  it('invalidates [admin, iceberg] when an iceberg-mutating task COMPLETES', async () => {
    // Replaces the 30s/60s polls that IcebergStatus/IcebergCalendar
    // used to drive themselves; the cron-runs stream now pushes a
    // single invalidation when the relevant cron lifecycle event
    // lands. Gated on completed-status so the panels don't churn
    // mid-compaction.
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([
        `data: ${JSON.stringify({ event: 'cron_run_changed', run_id: 3, task: 'optimize_iceberg', status: 'success' })}\r\n\r\n`,
      ]),
    )

    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const { useCronRunsStream } = await import('@/hooks/useCronRunsStream')
    renderHook(() => useCronRunsStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => {
      const keys = spy.mock.calls.map(c => JSON.stringify(c[0]?.queryKey))
      expect(keys).toContain(JSON.stringify(['admin', 'iceberg']))
    })
  })

  it('does NOT invalidate [admin, iceberg] for a non-iceberg task', async () => {
    // gap_heal mutates ingest state but doesn't touch Iceberg
    // snapshots — invalidating would force the panels to refetch
    // for nothing.
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([
        `data: ${JSON.stringify({ event: 'cron_run_changed', run_id: 4, task: 'gap_heal', status: 'success' })}\r\n\r\n`,
      ]),
    )

    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const { useCronRunsStream } = await import('@/hooks/useCronRunsStream')
    renderHook(() => useCronRunsStream(true), { wrapper: wrapperWith(qc) })

    // Wait for SOME invalidation (recent) so we know the event landed.
    await waitFor(() => {
      const keys = spy.mock.calls.map(c => JSON.stringify(c[0]?.queryKey))
      expect(keys).toContain(JSON.stringify(['admin', 'cron-logs-recent', 'svc-1']))
    })
    const icebergCalls = spy.mock.calls
      .map(c => JSON.stringify(c[0]?.queryKey))
      .filter(k => k === JSON.stringify(['admin', 'iceberg']))
    expect(icebergCalls).toHaveLength(0)
  })

  it('does NOT invalidate [admin, iceberg] for a RUNNING iceberg-task event', async () => {
    // Mirrors the [admin, cron-logs] table-invalidation rule: the
    // panels show snapshot counts that don't change mid-compaction,
    // so the running-state event should leave them alone.
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([
        `data: ${JSON.stringify({ event: 'cron_run_changed', run_id: 5, task: 'optimize_iceberg', status: 'running' })}\r\n\r\n`,
      ]),
    )

    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const { useCronRunsStream } = await import('@/hooks/useCronRunsStream')
    renderHook(() => useCronRunsStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => {
      const keys = spy.mock.calls.map(c => JSON.stringify(c[0]?.queryKey))
      expect(keys).toContain(JSON.stringify(['admin', 'cron-logs-recent', 'svc-1']))
    })
    const icebergCalls = spy.mock.calls
      .map(c => JSON.stringify(c[0]?.queryKey))
      .filter(k => k === JSON.stringify(['admin', 'iceberg']))
    expect(icebergCalls).toHaveLength(0)
  })

  it('invalidates [admin, audit-logs] and [admin, ingested-files] on a COMPLETED cron event', async () => {
    // Service History + Ingestion tabs have no dedicated stream — they
    // piggyback on cron completion (a run is what writes audit entries /
    // ingests files). Prefix-match invalidation covers the eventFilter
    // child keys on audit-logs.
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([
        `data: ${JSON.stringify({ event: 'cron_run_changed', run_id: 20, task: 'sync', status: 'success' })}\r\n\r\n`,
      ]),
    )

    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const { useCronRunsStream } = await import('@/hooks/useCronRunsStream')
    renderHook(() => useCronRunsStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => {
      const keys = spy.mock.calls.map(c => JSON.stringify(c[0]?.queryKey))
      expect(keys).toContain(JSON.stringify(['admin', 'audit-logs', 'svc-1']))
      expect(keys).toContain(JSON.stringify(['admin', 'ingested-files', 'svc-1']))
    })
  })

  it('invalidates [admin, schema] only when a data-mutating task (sync/full_sync/commit) COMPLETES', async () => {
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([
        `data: ${JSON.stringify({ event: 'cron_run_changed', run_id: 21, task: 'commit', status: 'success' })}\r\n\r\n`,
      ]),
    )

    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const { useCronRunsStream } = await import('@/hooks/useCronRunsStream')
    renderHook(() => useCronRunsStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => {
      const keys = spy.mock.calls.map(c => JSON.stringify(c[0]?.queryKey))
      expect(keys).toContain(JSON.stringify(['admin', 'schema', 'svc-1']))
    })
  })

  it('does NOT invalidate [admin, schema] for a completed non-data-mutating task (audit/ingested still fire)', async () => {
    // `alerts` appends an audit entry but never changes the column set,
    // so schema must stay put while audit-logs/ingested-files refresh.
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([
        `data: ${JSON.stringify({ event: 'cron_run_changed', run_id: 22, task: 'alerts', status: 'success' })}\r\n\r\n`,
      ]),
    )

    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const { useCronRunsStream } = await import('@/hooks/useCronRunsStream')
    renderHook(() => useCronRunsStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => {
      const keys = spy.mock.calls.map(c => JSON.stringify(c[0]?.queryKey))
      expect(keys).toContain(JSON.stringify(['admin', 'audit-logs', 'svc-1']))
    })
    const schemaCalls = spy.mock.calls
      .map(c => JSON.stringify(c[0]?.queryKey))
      .filter(k => k === JSON.stringify(['admin', 'schema', 'svc-1']))
    expect(schemaCalls).toHaveLength(0)
  })

  it('does NOT invalidate audit/ingested/schema for a RUNNING event', async () => {
    // Running-state events don't change those listings; gate on completed
    // so a long sync doesn't churn the secondary tabs while in flight.
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([
        `data: ${JSON.stringify({ event: 'cron_run_changed', run_id: 23, task: 'sync', status: 'running' })}\r\n\r\n`,
      ]),
    )

    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const { useCronRunsStream } = await import('@/hooks/useCronRunsStream')
    renderHook(() => useCronRunsStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => {
      const keys = spy.mock.calls.map(c => JSON.stringify(c[0]?.queryKey))
      expect(keys).toContain(JSON.stringify(['admin', 'cron-logs-recent', 'svc-1']))
    })
    const post = spy.mock.calls.map(c => JSON.stringify(c[0]?.queryKey))
    expect(post).not.toContain(JSON.stringify(['admin', 'audit-logs', 'svc-1']))
    expect(post).not.toContain(JSON.stringify(['admin', 'ingested-files', 'svc-1']))
    expect(post).not.toContain(JSON.stringify(['admin', 'schema', 'svc-1']))
  })

  it('does NOT invalidate [last-sync] when payload is malformed (without status we would over-invalidate)', async () => {
    // Updated 2026-06-16: tracking the "Last Sync" badge requires
    // knowing the status (completed vs. running); a malformed payload
    // can't supply it, so the hook intentionally skips last-sync to
    // avoid resetting the timer mid-run on garbage events. Table
    // invalidations still fire so the row count stays fresh.
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([`data: not-json\r\n\r\n`]),
    )

    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const { useCronRunsStream } = await import('@/hooks/useCronRunsStream')
    renderHook(() => useCronRunsStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => {
      const keys = spy.mock.calls.map(c => JSON.stringify(c[0]?.queryKey))
      expect(keys).toContain(JSON.stringify(['admin', 'cron-logs', 'svc-1']))
    })
    const lastSyncCalls = spy.mock.calls
      .map(c => JSON.stringify(c[0]?.queryKey))
      .filter(k => k === JSON.stringify(['last-sync', 'svc-1']))
    expect(lastSyncCalls).toHaveLength(0)
  })
})
