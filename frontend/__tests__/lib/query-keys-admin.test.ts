import { QueryClient } from '@tanstack/react-query'
import { describe, expect, it } from 'vitest'

import { queryKeys } from '@/lib/query-keys'

// The literal tuples the admin surface used before the factory existed. SSE
// appliers, seeders and invalidators match on these shapes, so they must not
// drift.
describe('queryKeys.admin shapes', () => {
  const k = queryKeys.admin
  const cases: [string, readonly unknown[], readonly unknown[]][] = [
    ['shareStatus', k.shareStatus(), ['admin', 'share', 'status']],
    ['shareLive', k.shareLive(), ['admin', 'share', 'live']],
    ['metricHistoryBatch', k.metricHistoryBatch('1h'), ['admin', 'metric-history-batch', '1h']],
    ['celeryStatus', k.celeryStatus(), ['admin', 'celery-status']],
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
    const qc = new QueryClient()
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
    const qc = new QueryClient()
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
