/**
 * @vitest-environment jsdom
 */
import { http, HttpResponse } from 'msw'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { QueryKey } from '@tanstack/react-query'

const apiBase = vi.hoisted(() => ({ value: 'http://api.test' }))
vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>()
  return {
    ...actual,
    getApiBase: () => apiBase.value,
  }
})

import { adminJson, adminJsonResult } from '@/lib/api'
import { arrayOrEmpty, recordOrNull } from '@/lib/utils'
import { queryKeys } from '@/lib/query-keys'
import { keepPreviousWithinService } from '@/hooks/useDashboardCards'
import { buildStreamRequest } from '@/hooks/useSSE'
import {
  throwIfStaleAggregates,
  isStaleDashboardViewError,
  STALE_VIEW_RETRY_OPTIONS,
  STALE_VIEW_POLL_MS,
} from '@/lib/api'
import type { components } from '@/types/api'
import { server } from '@/__tests__/helpers/msw/handlers'
import { createTestQueryClient } from '../helpers/page-smoke'

describe('adminJson', () => {
  it('returns the parsed body on 2xx', async () => {
    server.use(
      http.get('*/api/admin/queries/summary', () =>
        HttpResponse.json({ active_total: 2, by_db_type: { duckdb: 2 }, longest_ms: 12 }),
      ),
    )
    const body = await adminJson('/api/admin/queries/summary', '/api/admin/queries/summary')
    expect(body.active_total).toBe(2)
  })

  it('throws the default status message on non-2xx', async () => {
    server.use(
      http.get('*/api/admin/queries/summary', () => HttpResponse.json({}, { status: 503 })),
    )
    await expect(
      adminJson('/api/admin/queries/summary', '/api/admin/queries/summary'),
    ).rejects.toThrow('status 503')
  })

  it('uses the caller-supplied error message', async () => {
    server.use(
      http.get('*/api/admin/queries/summary', () => HttpResponse.json({}, { status: 500 })),
    )
    await expect(
      adminJson('/api/admin/queries/summary', '/api/admin/queries/summary', {}, (s) => `custom ${s}`),
    ).rejects.toThrow('custom 500')
  })
})

describe('adminJsonResult', () => {
  it('returns ok with parsed data on 2xx', async () => {
    server.use(http.get('*/api/admin/queries/summary', () => HttpResponse.json({ active_total: 4 })))
    const res = await adminJsonResult('/api/admin/queries/summary', '/api/admin/queries/summary')
    expect(res.ok).toBe(true)
    if (res.ok) expect(res.data.active_total).toBe(4)
  })

  it('does not throw on non-2xx and exposes status and body text', async () => {
    server.use(
      http.post('*/api/admin/queries/7/cancel', () => new HttpResponse('nope', { status: 409, statusText: 'Conflict' })),
    )
    const res = await adminJsonResult<'/api/admin/queries/{qid}/cancel', components['schemas']['CancelResponse'], 'post'>(
      '/api/admin/queries/{qid}/cancel',
      '/api/admin/queries/7/cancel',
      { method: 'POST' },
    )
    expect(res.ok).toBe(false)
    if (!res.ok) {
      expect(res.status).toBe(409)
      expect(res.errorText).toBe('nope')
      expect(res.statusText).toBe('Conflict')
      expect(res.data).toBeNull()
    }
  })
})

describe('narrow helpers', () => {
  it('recordOrNull keeps objects and drops everything else', () => {
    expect(recordOrNull({ a: 1 })).toEqual({ a: 1 })
    expect(recordOrNull(null)).toBeNull()
    const looseRecordOrNull = recordOrNull as (v: unknown) => unknown
    expect(looseRecordOrNull([])).toBeNull()
    expect(looseRecordOrNull('x')).toBeNull()
  })

  it('arrayOrEmpty returns a copy of arrays and [] otherwise', () => {
    expect(arrayOrEmpty([1, 2])).toEqual([1, 2])
    expect(arrayOrEmpty(null)).toEqual([])
    const looseArrayOrEmpty = arrayOrEmpty as (v: unknown) => unknown
    expect(looseArrayOrEmpty({})).toEqual([])
  })
})

describe('queryKeys.admin shapes', () => {
  const k = queryKeys.admin
  const cases: [string, readonly unknown[], readonly unknown[]][] = [
    ['shareStatus', k.shareStatus(), ['admin', 'share', 'status']],
    ['shareLive', k.shareLive(), ['admin', 'share', 'live']],
    ['metricHistoryBatch', k.metricHistoryBatch('1h'), ['admin', 'metric-history-batch', '1h']],
    ['celeryStatus', k.celeryStatus(), ['admin', 'celery-status']],
    ['celeryLedger', k.celeryLedger('problem', 's', 0), ['admin', 'celery-ledger', 'problem', 's', 0]],
    ['celeryLedgerAll', k.celeryLedgerAll(), ['admin', 'celery-ledger']],
    ['healthSnapshot', k.healthSnapshot(), ['admin', 'health-snapshot']],
    ['metadataStorage', k.metadataStorage(), ['admin', 'metadata-storage']],
    ['debugSettings', k.debugSettings(), ['admin', 'debug-settings']],
    ['status', k.status(), ['admin', 'status']],
    ['overviewAll', k.overviewAll(), ['admin', 'overview']],
    ['overviewQueriesSummary', k.overviewQueriesSummary(), ['admin', 'overview', 'queries-summary']],
    ['overviewLogAccounting', k.overviewLogAccounting('s'), ['admin', 'overview', 'log-accounting', 's']],
    ['overviewSlowQueriesCount', k.overviewSlowQueriesCount('s'), ['admin', 'overview', 'slow-queries-count', 's']],
    ['overviewCeleryStatus', k.overviewCeleryStatus(), ['admin', 'overview', 'celery-status']],
    ['queryMonitorConfig', k.queryMonitorConfig(), ['admin', 'query-monitor', 'config']],
    ['queryMonitorSnapshot', k.queryMonitorSnapshot(true), ['admin', 'query-monitor', 'snapshot', true]],
    ['queryMonitorSnapshotAll', k.queryMonitorSnapshotAll(), ['admin', 'query-monitor', 'snapshot']],
    ['queryMonitorSlowHistory', k.queryMonitorSlowHistory('past_1h', 500), ['admin', 'query-monitor', 'slow-history', 'past_1h', 500]],
    ['queryMonitorDetail', k.queryMonitorDetail(7), ['admin', 'query-monitor', 'detail', 7]],
    ['quarantineSummary', k.quarantineSummary('s'), ['admin', 'quarantine', 'summary', 's']],
    ['quarantineList', k.quarantineList('s', 'all', 50), ['admin', 'quarantine', 'list', 's', 'all', 50]],
    ['quarantineAll', k.quarantineAll(), ['admin', 'quarantine']],
    ['cronLogs', k.cronLogs('s', 'all', 'ok'), ['admin', 'cron-logs', 's', 'all', 'ok']],
    ['cronLogsFor', k.cronLogsFor('s'), ['admin', 'cron-logs', 's']],
    ['cronLogsAll', k.cronLogsAll(), ['admin', 'cron-logs']],
    ['cronLogsRecent', k.cronLogsRecent('s'), ['admin', 'cron-logs-recent', 's']],
    ['cronLogsRecentAll', k.cronLogsRecentAll(), ['admin', 'cron-logs-recent']],
    ['cronSchedule', k.cronSchedule('s'), ['admin', 'cron-schedule', 's']],
    ['auditLogs', k.auditLogs('s', 'login'), ['admin', 'audit-logs', 's', 'login']],
    ['auditLogsFor', k.auditLogsFor('s'), ['admin', 'audit-logs', 's']],
    ['ingestedFiles', k.ingestedFiles('s'), ['admin', 'ingested-files', 's']],
    ['schema', k.schema('s'), ['admin', 'schema', 's']],
    ['schemaAll', k.schemaAll(), ['admin', 'schema']],
    ['iceberg', k.iceberg('s'), ['admin', 'iceberg', 's']],
    ['icebergAll', k.icebergAll(), ['admin', 'iceberg']],
    ['icebergCalendar', k.icebergCalendar('s'), ['admin', 'iceberg', 'calendar', 's']],
    ['icebergInfo', k.icebergInfo('s'), ['admin', 'iceberg-info', 's']],
    ['icebergTree', k.icebergTree('s'), ['admin', 'iceberg-tree', 's']],
    ['rawTree', k.rawTree('s'), ['admin', 'raw-tree', 's']],
    ['fileTree iceberg', k.fileTree('iceberg', 's', 'p/'), ['admin', 'iceberg-tree', 's', 'p/']],
    ['fileTree raw', k.fileTree('raw', 's', 'p/'), ['admin', 'raw-tree', 's', 'p/']],
    ['fileTreeFor iceberg', k.fileTreeFor('iceberg', 's'), ['admin', 'iceberg-tree', 's']],
    ['fileTreeFor raw', k.fileTreeFor('raw', 's'), ['admin', 'raw-tree', 's']],
  ]

  it.each(cases)('%s keeps its original tuple', (_name, actual, expected) => {
    expect([...actual]).toEqual(expected)
  })

  it('prefix keys still match the longer keys they invalidate', () => {
    const qc = createTestQueryClient()
    qc.setQueryData(k.cronLogs('s', 'all', 'all'), 1)
    qc.setQueryData(k.cronLogs('other', 'all', 'all'), 2)
    qc.setQueryData(k.icebergCalendar('s'), 3)
    qc.setQueryData(k.queryMonitorSnapshot(false), 4)

    qc.invalidateQueries({ queryKey: k.cronLogsFor('s') })
    expect(qc.getQueryState(k.cronLogs('s', 'all', 'all'))?.isInvalidated).toBe(true)
    expect(qc.getQueryState(k.cronLogs('other', 'all', 'all'))?.isInvalidated).toBe(false)

    qc.invalidateQueries({ queryKey: k.icebergAll() })
    expect(qc.getQueryState(k.icebergCalendar('s'))?.isInvalidated).toBe(true)

    qc.invalidateQueries({ queryKey: k.queryMonitorSnapshotAll() })
    expect(qc.getQueryState(k.queryMonitorSnapshot(false))?.isInvalidated).toBe(true)
  })

  it('overviewAll invalidates every overview key and nothing else', () => {
    const qc = createTestQueryClient()
    qc.setQueryData(k.overviewQueriesSummary(), 1)
    qc.setQueryData(k.overviewLogAccounting('s'), 2)
    qc.setQueryData(k.overviewCeleryStatus(), 3)
    qc.setQueryData(k.healthSnapshot(), 4)

    qc.invalidateQueries({ queryKey: k.overviewAll() })
    expect(qc.getQueryState(k.overviewQueriesSummary())?.isInvalidated).toBe(true)
    expect(qc.getQueryState(k.overviewLogAccounting('s'))?.isInvalidated).toBe(true)
    expect(qc.getQueryState(k.overviewCeleryStatus())?.isInvalidated).toBe(true)
    expect(qc.getQueryState(k.healthSnapshot())?.isInvalidated).toBe(false)
  })
})

const prev = (key: QueryKey) => ({ queryKey: key })

describe('keepPreviousWithinService', () => {
  it('keeps previous data when only non-service key parts change', () => {
    const fn = keepPreviousWithinService(['logs', 'query', 'svc-a', 'p2'])
    expect(fn('rows-a', prev(['logs', 'query', 'svc-a', 'p1']))).toBe('rows-a')
  })

  it('drops previous data when the service id changes', () => {
    const fn = keepPreviousWithinService(['logs', 'query', 'svc-b'])
    expect(fn('rows-a', prev(['logs', 'query', 'svc-a']))).toBeUndefined()
  })

  it('returns undefined with no previous query', () => {
    expect(keepPreviousWithinService(['a', 'b', 'c'])(undefined, undefined)).toBeUndefined()
  })

  it('keeps previous data when keys are too short to carry a service id', () => {
    const fn = keepPreviousWithinService(['a', 'b'])
    expect(fn('x', prev(['a', 'c']))).toBe('x')
  })

  it('honours a custom service id index', () => {
    const fn = keepPreviousWithinService(['a', 'svc-b'], 1)
    expect(fn('x', prev(['a', 'svc-a']))).toBeUndefined()
  })
})

describe('buildStreamRequest', () => {
  afterEach(() => {
    apiBase.value = 'http://api.test'
  })

  it('concats base + path and always sends Accept', () => {
    const req = buildStreamRequest({ path: '/api/x?a=1' })
    expect(req.url).toBe('http://api.test/api/x?a=1')
    expect(req.headers).toEqual({ Accept: 'text/event-stream' })
    expect(req).not.toHaveProperty('method')
    expect(req).not.toHaveProperty('body')
    expect(req).not.toHaveProperty('cache')
  })

  it('handles an empty base without throwing', () => {
    apiBase.value = ''
    expect(buildStreamRequest({ path: '/api/x' }).url).toBe('/api/x')
  })

  it('adds service header only when present', () => {
    const req = buildStreamRequest({ path: '/p', serviceId: 'svc' })
    expect(req.headers).toEqual({
      Accept: 'text/event-stream',
      'x-service-id': 'svc',
    })
    expect(buildStreamRequest({ path: '/p', serviceId: null }).headers).toEqual({
      Accept: 'text/event-stream',
    })
  })

  it('passes method, body and cache through and lets extraHeaders override', () => {
    const req = buildStreamRequest({
      path: '/p',
      serviceId: 'svc',
      method: 'POST',
      body: '{}',
      cache: 'no-store',
      extraHeaders: { 'x-service-id': 'other', 'Content-Type': 'application/json' },
    })
    expect(req.method).toBe('POST')
    expect(req.body).toBe('{}')
    expect(req.cache).toBe('no-store')
    expect(req.headers).toEqual({
      Accept: 'text/event-stream',
      'x-service-id': 'other',
      'Content-Type': 'application/json',
    })
  })
})

const EMPTY_FIELDS = {
  status: { total: 0, top: [] },
  country: { total: 0, top: [] },
}

const NONEMPTY_FIELDS = {
  status: { total: 12, top: [{ value: '200', count: 12 }] },
}

function aggregates(partial: Record<string, unknown>): Record<string, unknown> {
  return {
    data: EMPTY_FIELDS,
    time_series: [],
    map_data: [],
    ...partial,
  }
}

function isFlaggedStale(data: unknown, window?: { startTime?: string | null; endTime?: string | null }): boolean {
  try {
    throwIfStaleAggregates(data, window)
    return false
  } catch (err) {
    expect(isStaleDashboardViewError(err)).toBe(true)
    return true
  }
}

const WIDE_WINDOW = { startTime: '2026-06-21T00:00:00.000Z', endTime: '2026-06-22T23:59:59.999Z' }

describe('throwIfStaleAggregates discriminator', () => {
  it('does not flag a response with no latest_log_at (service has no data)', () => {
    expect(isFlaggedStale(aggregates({ latest_log_at: null }), WIDE_WINDOW)).toBe(false)
    expect(isFlaggedStale(aggregates({}), WIDE_WINDOW)).toBe(false)
  })

  it('does not flag a non-empty response', () => {
    const data = aggregates({
      latest_log_at: '2026-06-22T12:00:00.000Z',
      data: NONEMPTY_FIELDS,
    })
    expect(isFlaggedStale(data, WIDE_WINDOW)).toBe(false)
  })

  it('does not flag when time_series has points even if fields are empty', () => {
    const data = aggregates({
      latest_log_at: '2026-06-22T12:00:00.000Z',
      time_series: [{ time: '2026-06-22T12:00:00.000Z', value: 5 }],
    })
    expect(isFlaggedStale(data, WIDE_WINDOW)).toBe(false)
  })

  it('flags an empty response when latest_log_at falls INSIDE the queried window', () => {
    const data = aggregates({
      earliest_log_at: '2026-06-21T06:00:00.000Z',
      latest_log_at: '2026-06-22T12:00:00.000Z',
    })
    expect(isFlaggedStale(data, WIDE_WINDOW)).toBe(true)
  })

  it('does NOT flag an empty response when the data span is entirely OUTSIDE the window (fresh-install false positive)', () => {
    const data = aggregates({
      earliest_log_at: '2026-06-18T00:00:00.000Z',
      latest_log_at: '2026-06-19T00:00:00.000Z',
    })
    const defaultWindow = { startTime: '2026-06-21T00:00:00.000Z', endTime: '2026-06-22T00:00:00.000Z' }
    expect(isFlaggedStale(data, defaultWindow)).toBe(false)
  })

  it('does NOT flag when data is newer than the queried window (window ends before the data)', () => {
    const data = aggregates({
      earliest_log_at: '2026-06-22T10:00:00.000Z',
      latest_log_at: '2026-06-22T12:00:00.000Z',
    })
    const pastWindow = { startTime: '2026-06-20T00:00:00.000Z', endTime: '2026-06-21T00:00:00.000Z' }
    expect(isFlaggedStale(data, pastWindow)).toBe(false)
  })

  it('handles date-only ("YYYY-MM-DD") extents — overlapping window flags, non-overlapping does not', () => {
    const data = aggregates({
      earliest_log_at: '2026-06-22',
      latest_log_at: '2026-06-22',
    })
    expect(isFlaggedStale(data, WIDE_WINDOW)).toBe(true)
    const earlierWindow = { startTime: '2026-06-19T00:00:00.000Z', endTime: '2026-06-20T00:00:00.000Z' }
    expect(isFlaggedStale(data, earlierWindow)).toBe(false)
  })

  it('falls back to the window-agnostic emptiness check when no window is supplied', () => {
    const data = aggregates({ latest_log_at: '2026-06-22T12:00:00.000Z' })
    expect(isFlaggedStale(data)).toBe(true)
  })

  it('does NOT flag an empty result when a filter is active (filtered → legitimate empty)', () => {
    const data = aggregates({
      earliest_log_at: '2026-06-21T06:00:00.000Z',
      latest_log_at: '2026-06-22T12:00:00.000Z',
    })
    expect(isFlaggedStale(data, WIDE_WINDOW)).toBe(true)
    expect(throwIfStaleAggregates(data, WIDE_WINDOW, true)).toBe(data)
  })

  it('returns the original data object unchanged when not stale', () => {
    const data = aggregates({ latest_log_at: '2026-06-22T12:00:00.000Z', data: NONEMPTY_FIELDS })
    expect(throwIfStaleAggregates(data, WIDE_WINDOW)).toBe(data)
  })
})

describe('isStaleDashboardViewError', () => {
  it('recognises the stale-view error and rejects others', () => {
    let caught: unknown
    try {
      throwIfStaleAggregates(aggregates({ latest_log_at: '2026-06-22T12:00:00.000Z' }), WIDE_WINDOW)
    } catch (err) {
      caught = err
    }
    expect(isStaleDashboardViewError(caught)).toBe(true)
    expect(isStaleDashboardViewError(new Error('boom'))).toBe(false)
    expect(isStaleDashboardViewError(null)).toBe(false)
    expect(isStaleDashboardViewError('nope')).toBe(false)
  })
})

describe('STALE_VIEW_RETRY_OPTIONS', () => {
  const staleErr = (() => {
    try {
      throwIfStaleAggregates(aggregates({ latest_log_at: '2026-06-22T12:00:00.000Z' }), WIDE_WINDOW)
    } catch (e) {
      return e as Error
    }
    throw new Error('expected a stale error')
  })()

  it('retries the stale-view error up to twice, then stops', () => {
    expect(STALE_VIEW_RETRY_OPTIONS.retry(0, staleErr)).toBe(true)
    expect(STALE_VIEW_RETRY_OPTIONS.retry(1, staleErr)).toBe(true)
    expect(STALE_VIEW_RETRY_OPTIONS.retry(2, staleErr)).toBe(false)
  })

  it('does not retry non-stale errors', () => {
    expect(STALE_VIEW_RETRY_OPTIONS.retry(0, new Error('500'))).toBe(false)
  })

  it('slow-polls only while parked on a stale-view error', () => {
    expect(STALE_VIEW_RETRY_OPTIONS.refetchInterval({ state: { error: staleErr, data: undefined } })).toBe(STALE_VIEW_POLL_MS)
    expect(STALE_VIEW_RETRY_OPTIONS.refetchInterval({ state: { error: staleErr, data: { aggregates: {} } } })).toBe(false)
    expect(STALE_VIEW_RETRY_OPTIONS.refetchInterval({ state: { error: new Error('500'), data: undefined } })).toBe(false)
    expect(STALE_VIEW_RETRY_OPTIONS.refetchInterval({ state: { error: null, data: undefined } })).toBe(false)
  })
})
