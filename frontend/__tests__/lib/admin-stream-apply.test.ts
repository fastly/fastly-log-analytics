/**
 * admin-stream-apply — the per-channel apply logic the multiplexed admin
 * event stream demuxes to. Extracted from the old single-purpose hooks;
 * these tests port their coverage to the pure functions.
 *
 * Load-bearing behaviours pinned here:
 *  - applySystemMetrics null-slice skip (a failed sampler slice arrives as
 *    null and MUST NOT blank the last-good cached value).
 *  - makeCronRunsApplier coalescing + the per-task/status invalidation
 *    matrix (table/last-sync/iceberg/audit/ingested/schema gating).
 *
 * @vitest-environment jsdom
 */
import { QueryClient } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { applyShare, applySyncStatus, applySystemMetrics, makeCronRunsApplier } from '@/lib/admin-stream-apply'

function makeQueryClient(): QueryClient {
  // gcTime > 0 — appliers write via setQueryData with no subscriber.
  return createTestQueryClient({ queries: { gcTime: 60_000, staleTime: 0 } })
}

function invalidatedKeys(spy: ReturnType<typeof vi.spyOn>): string[] {
  return spy.mock.calls.map((c: unknown[]) => JSON.stringify((c[0] as { queryKey: unknown })?.queryKey))
}

// ── applySyncStatus ──────────────────────────────────────────────────────────

describe('applySyncStatus', () => {
  it('writes the snapshot to [sync-status, serviceId]', () => {
    const qc = makeQueryClient()
    applySyncStatus(qc, 'svc-1', { local_rows: 7, latest_log_at: '2026-06-26T00:00:00Z' })
    expect(qc.getQueryData(['sync-status', 'svc-1'])).toEqual({
      local_rows: 7,
      latest_log_at: '2026-06-26T00:00:00Z',
    })
  })
})

// ── applySystemMetrics ───────────────────────────────────────────────────────

describe('applySystemMetrics', () => {
  it('fans non-null slices into their cache keys', () => {
    const qc = makeQueryClient()
    const payload = {
      health_snapshot: { status: 'ok' },
      metric_history_1h: [{ t: 1, v: 2 }],
      queries_summary: { total: 42 },
      slow_queries_count: 3,
      log_accounting: { rows: 1000 },
      metadata_storage: { bytes: 9999 },
      system_jobs: [{ id: 'job-1' }],
    }
    applySystemMetrics(qc, 'svc-1', payload)
    expect(qc.getQueryData(['admin', 'health-snapshot'])).toEqual(payload.health_snapshot)
    expect(qc.getQueryData(['admin', 'metric-history-batch', '1h'])).toEqual(payload.metric_history_1h)
    expect(qc.getQueryData(['admin', 'overview', 'queries-summary'])).toEqual(payload.queries_summary)
    // log-accounting + slow-queries-count are per-service: written to the
    // service-scoped key the cards read, so a service switch can't show one
    // service's freshness under another.
    expect(qc.getQueryData(['admin', 'overview', 'slow-queries-count', 'svc-1'])).toEqual(payload.slow_queries_count)
    expect(qc.getQueryData(['admin', 'overview', 'log-accounting', 'svc-1'])).toEqual(payload.log_accounting)
    expect(qc.getQueryData(['admin', 'metadata-storage'])).toEqual(payload.metadata_storage)
    expect(qc.getQueryData(['system-jobs'])).toEqual(payload.system_jobs)
  })

  it('SKIPS null/omitted slices and preserves stale data', () => {
    const qc = makeQueryClient()
    const stale = { status: 'ok' }
    qc.setQueryData(['admin', 'health-snapshot'], stale)
    qc.setQueryData(['admin', 'metadata-storage'], { bytes: 12345 })
    applySystemMetrics(qc, 'svc-1', { health_snapshot: null, queries_summary: { total: 99 }, metadata_storage: null })
    expect(qc.getQueryData(['admin', 'health-snapshot'])).toEqual(stale)
    expect(qc.getQueryData(['admin', 'metadata-storage'])).toEqual({ bytes: 12345 })
    expect(qc.getQueryData(['admin', 'overview', 'queries-summary'])).toEqual({ total: 99 })
  })

  it('ignores a non-object payload without throwing', () => {
    const qc = makeQueryClient()
    expect(() => applySystemMetrics(qc, 'svc-1', null)).not.toThrow()
    expect(() => applySystemMetrics(qc, 'svc-1', 'nope')).not.toThrow()
  })
})

// ── applyShare ───────────────────────────────────────────────────────────────

describe('applyShare', () => {
  it('writes the live payload to [admin, share, live]', () => {
    const qc = makeQueryClient()
    const payload = { sharing_active: true, public_url: 'https://x.test', active_session_count: 2 }
    applyShare(qc, payload)
    expect(qc.getQueryData(['admin', 'share', 'live'])).toEqual(payload)
  })

  it('ignores an undefined payload without clobbering cached data', () => {
    const qc = makeQueryClient()
    const stale = { sharing_active: false }
    qc.setQueryData(['admin', 'share', 'live'], stale)
    applyShare(qc, undefined)
    expect(qc.getQueryData(['admin', 'share', 'live'])).toEqual(stale)
  })
})

// ── makeCronRunsApplier ──────────────────────────────────────────────────────

describe('makeCronRunsApplier', () => {
  beforeEach(() => vi.useFakeTimers())
  afterEach(() => vi.useRealTimers())

  it('coalesces a burst into one flush after 100ms (recent + schedule always)', () => {
    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const applier = makeCronRunsApplier(qc, 'svc-1')

    applier.apply({ task: 'commit', status: 'success' })
    applier.apply({ task: 'sync', status: 'running' })
    // Nothing flushed yet — trailing-edge coalesce.
    expect(spy).not.toHaveBeenCalled()

    vi.advanceTimersByTime(100)
    const keys = invalidatedKeys(spy)
    expect(keys).toContain(JSON.stringify(['admin', 'cron-logs-recent', 'svc-1']))
    expect(keys).toContain(JSON.stringify(['admin', 'cron-schedule', 'svc-1']))
    // Each key invalidated exactly once despite two apply() calls.
    expect(keys.filter((k) => k === JSON.stringify(['admin', 'cron-schedule', 'svc-1']))).toHaveLength(1)
  })

  it('invalidates the heavy table only on a COMPLETED event, not running', () => {
    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const applier = makeCronRunsApplier(qc, 'svc-1')

    applier.apply({ task: 'commit', status: 'running' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).not.toContain(JSON.stringify(['admin', 'cron-logs', 'svc-1']))

    applier.apply({ task: 'commit', status: 'success' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).toContain(JSON.stringify(['admin', 'cron-logs', 'svc-1']))
  })

  it('invalidates [last-sync] for sync events (both running and complete) but not other tasks', () => {
    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const applier = makeCronRunsApplier(qc, 'svc-1')

    applier.apply({ task: 'sync', status: 'running' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).toContain(JSON.stringify(['last-sync', 'svc-1']))

    spy.mockClear()
    applier.apply({ task: 'commit', status: 'success' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).not.toContain(JSON.stringify(['last-sync', 'svc-1']))
  })

  it('invalidates [admin, iceberg] only when an iceberg-mutating task COMPLETES', () => {
    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const applier = makeCronRunsApplier(qc, 'svc-1')

    // running iceberg task → no iceberg invalidation
    applier.apply({ task: 'optimize_iceberg', status: 'running' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).not.toContain(JSON.stringify(['admin', 'iceberg']))

    // non-iceberg completed task → no iceberg invalidation
    spy.mockClear()
    applier.apply({ task: 'gap_heal', status: 'success' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).not.toContain(JSON.stringify(['admin', 'iceberg']))

    // completed iceberg-mutating task → invalidates
    spy.mockClear()
    applier.apply({ task: 'optimize_iceberg', status: 'success' })
    vi.advanceTimersByTime(100)
    expect(invalidatedKeys(spy)).toContain(JSON.stringify(['admin', 'iceberg']))
  })

  it('invalidates audit/ingested on complete, schema only for data-mutating tasks', () => {
    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const applier = makeCronRunsApplier(qc, 'svc-1')

    // data-mutating (commit) completed → audit + ingested + schema
    applier.apply({ task: 'commit', status: 'success' })
    vi.advanceTimersByTime(100)
    let keys = invalidatedKeys(spy)
    expect(keys).toContain(JSON.stringify(['admin', 'audit-logs', 'svc-1']))
    expect(keys).toContain(JSON.stringify(['admin', 'ingested-files', 'svc-1']))
    expect(keys).toContain(JSON.stringify(['admin', 'schema', 'svc-1']))

    // non-data-mutating (alerts) completed → audit/ingested fire, schema does NOT
    spy.mockClear()
    applier.apply({ task: 'alerts', status: 'success' })
    vi.advanceTimersByTime(100)
    keys = invalidatedKeys(spy)
    expect(keys).toContain(JSON.stringify(['admin', 'audit-logs', 'svc-1']))
    expect(keys).not.toContain(JSON.stringify(['admin', 'schema', 'svc-1']))

    // running event → none of audit/ingested/schema
    spy.mockClear()
    applier.apply({ task: 'sync', status: 'running' })
    vi.advanceTimersByTime(100)
    keys = invalidatedKeys(spy)
    expect(keys).not.toContain(JSON.stringify(['admin', 'audit-logs', 'svc-1']))
    expect(keys).not.toContain(JSON.stringify(['admin', 'ingested-files', 'svc-1']))
    expect(keys).not.toContain(JSON.stringify(['admin', 'schema', 'svc-1']))
  })

  it('malformed payload falls back to table, skips last-sync', () => {
    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const applier = makeCronRunsApplier(qc, 'svc-1')

    applier.apply(null)
    vi.advanceTimersByTime(100)
    const keys = invalidatedKeys(spy)
    expect(keys).toContain(JSON.stringify(['admin', 'cron-logs', 'svc-1']))
    expect(keys).not.toContain(JSON.stringify(['last-sync', 'svc-1']))
  })

  it('cleanup() cancels a pending flush so nothing invalidates after teardown', () => {
    const qc = makeQueryClient()
    const spy = vi.spyOn(qc, 'invalidateQueries')
    const applier = makeCronRunsApplier(qc, 'svc-1')

    applier.apply({ task: 'sync', status: 'success' })
    applier.cleanup()
    vi.advanceTimersByTime(100)
    expect(spy).not.toHaveBeenCalled()
  })
})
