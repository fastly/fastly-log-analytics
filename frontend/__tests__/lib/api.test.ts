/**
 * @vitest-environment jsdom
 *
 * Consolidated MSW-driven contract tests for the API client (`lib/api.ts`)
 * and `customFieldsApi` (`lib/api/custom-fields.ts`).
 */
import { renderHook, waitFor } from '@testing-library/react'
import { hashKey, hydrate, QueryClient, useMutation, useQuery, type QueryKey } from '@tanstack/react-query'
import { createTestQueryClient, makeQueryWrapper, invalidInput, setupNextHeadersMock, withTestHttpServer } from '../helpers/page-smoke'
import { http, HttpResponse } from 'msw'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

import { server } from '@/__tests__/helpers/msw/handlers'
import type { components } from '@/types/api.generated'
import {
  seedDehydratedState, fetchStreamingServerSide, fetchBootstrapServerSide,
  fetchPerformanceServerSide, fetchSecurityServerSide, fetchInsightsServerSide,
  fetchOriginServerSide, fetchTosServerSide, fetchCronRunsServerSide,
  fetchUsageLogServerSide, resolveDefaultSsrRangeKey, resolveInsightsDefault,
  PERFORMANCE_SSR_DEFAULTS, SECURITY_SSR_DEFAULTS, SECURITY_SSR_SECTIONS,
  ORIGIN_SSR_DEFAULTS, ORIGIN_SSR_SECTIONS,
} from '@/lib/ssr/upstream'

const { setHeaders, setCookies } = setupNextHeadersMock({ cookies: 'session=abc123' })
const inbound = setHeaders
const PUBLIC_HOST = 'fastly-log-analytics.global.ssl.fastly.net'
const NOW_ISO = new Date('2026-06-29T12:00:30Z')
const API_BASE = 'http://127.0.0.1:8000'
const SVC = 'svc-test'

const apiBase = vi.hoisted(() => ({ value: 'http://127.0.0.1:8000' }))
vi.mock('@/lib/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/api')>()),
  getApiBase: () => apiBase.value,
}))

const mockServiceState = { activeServiceId: 'svc-test' as string | null }

vi.mock('@/lib/utils', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/utils')>()),
  isUserActive: vi.fn(() => true),
  ACTIVE_WINDOW_MS: 120_000,
}))

vi.mock('@/stores/filterStore', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/stores/filterStore')>()
  const useServiceStore = Object.assign(
    vi.fn((selector?: (s: typeof mockServiceState) => unknown) => (selector ? selector(mockServiceState) : mockServiceState)),
    { getState: () => mockServiceState },
  )
  const state = { enabled: false, apiCallsEnabled: false }
  const useDebugStore = Object.assign(vi.fn(() => state), { getState: () => state })
  return { ...actual, useServiceStore, useDebugStore }
})

import {
  client, extractApiError, customFieldsApi, adminJson, adminJsonResult,
  queryKeys, throwIfStaleAggregates, isStaleDashboardViewError,
  STALE_VIEW_RETRY_OPTIONS, STALE_VIEW_POLL_MS, ssrUpstreamGet,
  type CustomField, type VclLintRequest,
} from '@/lib/api'
import {
  isUserActive, arrayOrEmpty, recordOrNull, resolveRangeWire,
  quantizeAnchor, pickInsightsDefault, historyHoursFromExtents,
} from '@/lib/utils'
import { keepPreviousWithinService } from '@/hooks/useDashboardCards'
import { buildStreamRequest } from '@/hooks/useSSE'

function wrapper() {
  const qc = createTestQueryClient({ queries: { gcTime: 0 }, mutations: { retry: false } })
  return makeQueryWrapper(qc)
}

beforeEach(() => {
  mockServiceState.activeServiceId = 'svc-test'
  vi.mocked(isUserActive).mockReturnValue(true)
})

afterEach(() => {
  server.resetHandlers()
})

describe('extractApiError shape coverage', () => {
  it.each([
    ['returns string-detail verbatim', { detail: 'Service not found' }, 'Service not found'],
    ['joins detail.errors[] (FastAPI 422 with our custom shape)', { detail: { errors: ['LOG_FORMAT_TOO_LONG: too big', 'BAD_NAME: nope'] } }, 'LOG_FORMAT_TOO_LONG: too big, BAD_NAME: nope'],
    ['falls back to detail.error scalar', { detail: { error: 'broken' } }, 'broken'],
    ['reads top-level error', { error: 'kapow' }, 'kapow'],
    ['returns a sane default for null', null, 'Unknown error'],
    ['returns a sane default for undefined', undefined, 'Unknown error'],
  ])('%s', (_label, input, expected) => {
    expect(extractApiError(input)).toBe(expected)
  })

  it('formats Pydantic detail[] validation arrays', () => {
    expect(extractApiError({ detail: [{ loc: ['body', 'name'], msg: 'required' }] })).toContain('name: required')
  })
})

async function mutateAndExpectError(mutationFn: () => Promise<unknown>) {
  const { result } = renderHook(() => useMutation({ mutationFn }), { wrapper: wrapper() })
  result.current.mutate()
  await waitFor(() => expect(result.current.isError).toBe(true))
  return result.current.error
}

describe('API client onResponse middleware (MSW)', () => {
  it('422 detail.errors[] reaches the caller as a joined Error.message', async () => {
    server.use(
      http.post(`${API_BASE}/api/services/svc-test/custom-fields`, () =>
        HttpResponse.json({ detail: { errors: ['LOG_FORMAT_TOO_LONG: Log format is 8050 chars; safe max 8000'] } }, { status: 422 }),
      ),
    )
    const err = await mutateAndExpectError(async () => {
      const { data } = await client.POST('/api/services/{service_id}/custom-fields', {
        params: { path: { service_id: 'svc-test' } },
        body: { name: 'whatever' } as components['schemas']['CustomFieldCreate'],
      })
      return data
    })
    expect(err).toBeInstanceOf(Error)
    expect(err?.message).toContain('LOG_FORMAT_TOO_LONG')
    expect(err?.message).toContain('8000')
  })

  it('500 string-detail produces a readable Error.message', async () => {
    server.use(http.post(`${API_BASE}/api/dashboard/aggregates`, () => HttpResponse.json({ detail: 'Internal server error' }, { status: 500 })))
    const err = await mutateAndExpectError(async () => {
      const { data } = await client.POST('/api/dashboard/aggregates', {
        body: { start_time: '2026-01-01T00:00:00Z', end_time: '2026-01-01T01:00:00Z', filters: invalidInput([]), chart_metric: 'requests', chart_interval: 'minute' },
      })
      return data
    })
    expect(err?.message).toBe('Internal server error')
  })

  it('401 plain message becomes a readable Error.message (not "[object Object]")', async () => {
    server.use(http.post(`${API_BASE}/api/dashboard/aggregates`, () => HttpResponse.json({ detail: 'Not authenticated' }, { status: 401 })))
    const err = await mutateAndExpectError(async () => {
      const { data } = await client.POST('/api/dashboard/aggregates', {
        body: invalidInput({ start_time: '2026-01-01T00:00:00Z', end_time: '2026-01-01T01:00:00Z', filters: [] }),
      })
      return data
    })
    expect(err?.message).toBe('Not authenticated')
    expect(err?.message).not.toContain('[object Object]')
    expect(err?.message).not.toContain('{')
  })

  it('500 with a missing body still produces some message', async () => {
    server.use(http.post(`${API_BASE}/api/dashboard/aggregates`, () => HttpResponse.text('', { status: 500 })))
    const err = await mutateAndExpectError(async () => {
      const { data } = await client.POST('/api/dashboard/aggregates', { body: {} as components['schemas']['AggregatesRequest'] })
      return data
    })
    expect(err?.message).toBeTruthy()
    expect(err?.message.length).toBeGreaterThan(0)
  })

  it('useQuery error path leaves isError=true and data undefined', async () => {
    server.use(http.get(`${API_BASE}/api/log-fields/catalog`, () => HttpResponse.json({ detail: 'catalog fetch broke' }, { status: 500 })))
    const { result } = renderHook(
      () => useQuery({ queryKey: ['catalog'], queryFn: async () => (await client.GET('/api/log-fields/catalog')).data }),
      { wrapper: wrapper() },
    )
    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(result.current.data).toBeUndefined()
    expect(result.current.error?.message).toBe('catalog fetch broke')
  })
})

describe('serviceless service-discovery & global-admin observability exemptions (fresh install)', () => {
  beforeEach(() => {
    mockServiceState.activeServiceId = null
  })

  it.each([
    {
      label: 'does NOT abort GET /api/services when no service is active',
      path: '/api/services',
      payload: { services: [] },
      call: () => client.GET('/api/services'),
    },
    {
      label: 'does NOT abort GET /api/log-fields/catalog when no service is active',
      path: '/api/log-fields/catalog',
      payload: { groups: [{ id: 'A' }], fields: [], presets: {}, insights: [] },
      call: () => client.GET('/api/log-fields/catalog'),
    },
    {
      label: 'does NOT abort GET /api/admin/health-snapshot',
      path: '/api/admin/health-snapshot',
      payload: { vcpus: 4 },
      call: () => client.GET('/api/admin/health-snapshot'),
    },
    {
      label: 'does NOT abort GET /api/admin/metric-history/batch',
      path: '/api/admin/metric-history/batch',
      payload: { series: {} },
      call: () => client.GET('/api/admin/metric-history/batch', { params: { query: { since: '1h' } } }),
    },
    {
      label: 'does NOT abort GET /api/admin/bot-sources',
      path: '/api/admin/bot-sources',
      payload: { sources: [], rdns: { total: 0, pending: 0, last_enrichment_at: null } },
      call: () => client.GET('/api/admin/bot-sources'),
    },
  ])('$label', async ({ path, payload, call }) => {
    let reached = false
    server.use(
      http.get(`${API_BASE}${path}`, () => {
        reached = true
        return HttpResponse.json(payload)
      }),
    )
    const { data, error } = await call()
    expect(reached).toBe(true)
    expect(error).toBeUndefined()
    expect(data).toMatchObject(payload)
  })

  it('STILL aborts a service-scoped subpath (/api/services/{id}/…)', async () => {
    let reached = false
    server.use(
      http.get(`${API_BASE}/api/services/:id/log-field-audit`, () => {
        reached = true
        return HttpResponse.json({ ok: true })
      }),
    )
    await expect(
      client.GET('/api/services/{service_id}/log-field-audit', { params: invalidInput({ path: { service_id: 'svc-123' } }) }),
    ).rejects.toMatchObject({ code: 'NO_SERVICE' })
    expect(reached).toBe(false)
  })
})

describe('X-User-Active request injection', () => {
  it.each([
    [true, '1'],
    [false, '0'],
  ] as const)('active=%s sends X-User-Active: %s', async (active, expectedHeader) => {
    vi.mocked(isUserActive).mockReturnValue(active)
    let observed: string | null = null
    server.use(
      http.post(`${API_BASE}/api/dashboard/aggregates`, ({ request }) => {
        observed = request.headers.get('X-User-Active')
        return HttpResponse.json({ ok: true })
      }),
    )
    const { result } = renderHook(
      () =>
        useMutation({
          mutationFn: async () =>
            (await client.POST('/api/dashboard/aggregates', {
              body: { start_time: 't', end_time: 't', filters: {}, chart_interval: '1h', chart_metric: 'requests' },
            })).data,
        }),
      { wrapper: wrapper() },
    )
    result.current.mutate()
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(observed).toBe(expectedHeader)
  })
})

const LINT_BODY: VclLintRequest = { vcl_log_expression: 'req.url', collection_stage: 'edge' }
const FIELD_BODY: Omit<CustomField, 'created_at' | 'updated_at'> = {
  name: 'new_field', label: 'New field', description: '', vcl_log_expression: 'req.url',
  collection_stage: 'edge', origin_log_frequency: 'all', duckdb_type: 'VARCHAR',
  value_type: 'string', bytes_estimate: 32, nullable: true, enabled: true,
  show_in_dashboard: false, show_in_logs: true, filterable: true,
}

describe('customFieldsApi', () => {
  it('listCustomFields returns 200 body and throws on 500 (with or without detail)', async () => {
    const fields = [{ name: 'cookie_session_id', type: 'string', vcl: 'req.http.X-Sess' }]
    server.use(http.get(`${API_BASE}/api/services/${SVC}/custom-fields`, () => HttpResponse.json(fields)))
    expect(await customFieldsApi.listCustomFields(SVC)).toEqual(fields)

    server.use(http.get(`${API_BASE}/api/services/${SVC}/custom-fields`, () => HttpResponse.json({ detail: 'database is sad' }, { status: 500 })))
    await expect(customFieldsApi.listCustomFields(SVC)).rejects.toThrow('database is sad')

    server.use(http.get(`${API_BASE}/api/services/${SVC}/custom-fields`, () => HttpResponse.json({}, { status: 500 })))
    await expect(customFieldsApi.listCustomFields(SVC)).rejects.toThrow()
  })

  it('createCustomField returns created body on 200 and throws on 422 / empty 500', async () => {
    server.use(http.post(`${API_BASE}/api/services/${SVC}/custom-fields`, () => HttpResponse.json(FIELD_BODY)))
    expect(await customFieldsApi.createCustomField(SVC, FIELD_BODY)).toEqual(FIELD_BODY)

    server.use(
      http.post(`${API_BASE}/api/services/${SVC}/custom-fields`, () =>
        HttpResponse.json({ detail: { errors: ['LOG_FORMAT_TOO_LONG: too big', 'BAD_NAME: nope'] } }, { status: 422 }),
      ),
    )
    await expect(customFieldsApi.createCustomField(SVC, { ...FIELD_BODY, name: 'x' })).rejects.toThrow(/LOG_FORMAT_TOO_LONG/)

    server.use(http.post(`${API_BASE}/api/services/${SVC}/custom-fields`, () => HttpResponse.json({}, { status: 500 })))
    await expect(customFieldsApi.createCustomField(SVC, { ...FIELD_BODY, name: 'x' })).rejects.toThrow()
  })

  it('updateCustomField returns updated field on 200 and throws on 404', async () => {
    const updated = { name: 'cookie_id', vcl_log_expression: 'req.http.X-Cookie' }
    server.use(http.patch(`${API_BASE}/api/services/${SVC}/custom-fields/cookie_id`, () => HttpResponse.json(updated)))
    expect(await customFieldsApi.updateCustomField(SVC, 'cookie_id', { vcl_log_expression: 'req.http.X-Cookie' })).toEqual(updated)

    server.use(http.patch(`${API_BASE}/api/services/${SVC}/custom-fields/missing`, () => HttpResponse.json({ detail: 'field not found' }, { status: 404 })))
    await expect(customFieldsApi.updateCustomField(SVC, 'missing', { vcl_log_expression: 'req.url' })).rejects.toThrow('field not found')
  })

  it('deleteCustomField resolves on 200 and throws on 500', async () => {
    server.use(http.delete(`${API_BASE}/api/services/${SVC}/custom-fields/cookie_id`, () => HttpResponse.json({ deleted: true })))
    expect(await customFieldsApi.deleteCustomField(SVC, 'cookie_id')).toEqual({ deleted: true })

    server.use(http.delete(`${API_BASE}/api/services/${SVC}/custom-fields/cookie_id`, () => HttpResponse.json({ detail: 'cannot delete' }, { status: 500 })))
    await expect(customFieldsApi.deleteCustomField(SVC, 'cookie_id')).rejects.toThrow('cannot delete')
  })

  it('validateCustomVcl returns lint result on 200 and throws on 400', async () => {
    const lint = { ok: true, warnings: [], errors: [] }
    server.use(http.post(`${API_BASE}/api/services/${SVC}/custom-fields/validate-vcl`, () => HttpResponse.json(lint)))
    expect(await customFieldsApi.validateCustomVcl(SVC, LINT_BODY)).toEqual(lint)

    server.use(http.post(`${API_BASE}/api/services/${SVC}/custom-fields/validate-vcl`, () => HttpResponse.json({ detail: 'malformed VCL' }, { status: 400 })))
    await expect(customFieldsApi.validateCustomVcl(SVC, { ...LINT_BODY, vcl_log_expression: 'BAD' })).rejects.toThrow('malformed VCL')
  })

  it('exportCustomFields returns CSV Blob on 200 and throws on 500', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/${SVC}/custom-fields/export`, () =>
        HttpResponse.text('name,type,vcl\ncookie,string,req.url\n', { headers: { 'content-type': 'text/csv' } }),
      ),
    )
    const blob = await customFieldsApi.exportCustomFields(SVC)
    expect(typeof blob.size).toBe('number')
    expect(blob.size).toBeGreaterThan(0)
    expect(await blob.text()).toContain('name,type,vcl')

    server.use(http.get(`${API_BASE}/api/services/${SVC}/custom-fields/export`, () => HttpResponse.text('', { status: 500 })))
    await expect(customFieldsApi.exportCustomFields(SVC)).rejects.toThrow(/Failed to export/)
  })

  it('importCustomFields returns import result on 200 and throws on 422', async () => {
    const result = { imported: 3, skipped: 1 }
    server.use(http.post(`${API_BASE}/api/services/${SVC}/custom-fields/import`, () => HttpResponse.json(result)))
    expect(await customFieldsApi.importCustomFields(SVC, [{ name: 'a', vcl_log_expression: 'req.url' }])).toEqual(result)

    server.use(
      http.post(`${API_BASE}/api/services/${SVC}/custom-fields/import`, () =>
        HttpResponse.json({ detail: { errors: ['DUPLICATE_FIELD: cookie'] } }, { status: 422 }),
      ),
    )
    await expect(customFieldsApi.importCustomFields(SVC, [{ name: 'cookie', vcl_log_expression: 'req.url' }])).rejects.toThrow(/DUPLICATE_FIELD/)
  })
})

describe('adminJson & adminJsonResult', () => {
  it('adminJson returns parsed body on 2xx and throws default or custom status message on non-2xx', async () => {
    server.use(http.get('*/api/admin/queries/summary', () => HttpResponse.json({ active_total: 2, by_db_type: { duckdb: 2 }, longest_ms: 12 })))
    expect((await adminJson('/api/admin/queries/summary', '/api/admin/queries/summary')).active_total).toBe(2)

    server.use(http.get('*/api/admin/queries/summary', () => HttpResponse.json({}, { status: 503 })))
    await expect(adminJson('/api/admin/queries/summary', '/api/admin/queries/summary')).rejects.toThrow('status 503')

    server.use(http.get('*/api/admin/queries/summary', () => HttpResponse.json({}, { status: 500 })))
    await expect(adminJson('/api/admin/queries/summary', '/api/admin/queries/summary', {}, (s) => `custom ${s}`)).rejects.toThrow('custom 500')
  })

  it('adminJsonResult returns ok on 2xx and exposes status/errorText without throwing on non-2xx', async () => {
    server.use(http.get('*/api/admin/queries/summary', () => HttpResponse.json({ active_total: 4 })))
    const okRes = await adminJsonResult('/api/admin/queries/summary', '/api/admin/queries/summary')
    expect(okRes.ok).toBe(true)
    if (okRes.ok) expect(okRes.data.active_total).toBe(4)

    server.use(http.post('*/api/admin/queries/7/cancel', () => new HttpResponse('nope', { status: 409, statusText: 'Conflict' })))
    const errRes = await adminJsonResult<'/api/admin/queries/{qid}/cancel', components['schemas']['CancelResponse'], 'post'>(
      '/api/admin/queries/{qid}/cancel', '/api/admin/queries/7/cancel', { method: 'POST' },
    )
    expect(errRes.ok).toBe(false)
    if (!errRes.ok) {
      expect(errRes.status).toBe(409)
      expect(errRes.errorText).toBe('nope')
      expect(errRes.statusText).toBe('Conflict')
      expect(errRes.data).toBeNull()
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
  it('keeps or drops previous data based on service id match', () => {
    expect(keepPreviousWithinService(['logs', 'query', 'svc-a', 'p2'])('rows-a', prev(['logs', 'query', 'svc-a', 'p1']))).toBe('rows-a')
    expect(keepPreviousWithinService(['logs', 'query', 'svc-b'])('rows-a', prev(['logs', 'query', 'svc-a']))).toBeUndefined()
    expect(keepPreviousWithinService(['a', 'b', 'c'])(undefined, undefined)).toBeUndefined()
    expect(keepPreviousWithinService(['a', 'b'])('x', prev(['a', 'c']))).toBe('x')
    expect(keepPreviousWithinService(['a', 'svc-b'], 1)('x', prev(['a', 'svc-a']))).toBeUndefined()
  })
})

describe('buildStreamRequest', () => {
  beforeEach(() => { apiBase.value = 'http://api.test' })
  afterEach(() => { apiBase.value = API_BASE })

  it('concats base + path, handles empty base, and always sends Accept', () => {
    const req = buildStreamRequest({ path: '/api/x?a=1' })
    expect(req.url).toBe('http://api.test/api/x?a=1')
    expect(req.headers).toEqual({ Accept: 'text/event-stream' })
    for (const k of ['method', 'body', 'cache']) expect(req).not.toHaveProperty(k)

    apiBase.value = ''
    expect(buildStreamRequest({ path: '/api/x' }).url).toBe('/api/x')
  })

  it('adds service header only when present and passes method/body/cache/extraHeaders through', () => {
    expect(buildStreamRequest({ path: '/p', serviceId: 'svc' }).headers).toEqual({ Accept: 'text/event-stream', 'x-service-id': 'svc' })
    expect(buildStreamRequest({ path: '/p', serviceId: null }).headers).toEqual({ Accept: 'text/event-stream' })

    const req = buildStreamRequest({
      path: '/p', serviceId: 'svc', method: 'POST', body: '{}', cache: 'no-store',
      extraHeaders: { 'x-service-id': 'other', 'Content-Type': 'application/json' },
    })
    expect(req.method).toBe('POST')
    expect(req.body).toBe('{}')
    expect(req.cache).toBe('no-store')
    expect(req.headers).toEqual({ Accept: 'text/event-stream', 'x-service-id': 'other', 'Content-Type': 'application/json' })
  })
})

const EMPTY_FIELDS = { status: { total: 0, top: [] }, country: { total: 0, top: [] } }
const NONEMPTY_FIELDS = { status: { total: 12, top: [{ value: '200', count: 12 }] } }
const WIDE_WINDOW = { startTime: '2026-06-21T00:00:00.000Z', endTime: '2026-06-22T23:59:59.999Z' }

function aggregates(partial: Record<string, unknown>): Record<string, unknown> {
  return { data: EMPTY_FIELDS, time_series: [], map_data: [], ...partial }
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

describe('throwIfStaleAggregates, isStaleDashboardViewError & STALE_VIEW_RETRY_OPTIONS', () => {
  it.each([
    ['no latest_log_at (null)', aggregates({ latest_log_at: null }), WIDE_WINDOW, false],
    ['no latest_log_at (omitted)', aggregates({}), WIDE_WINDOW, false],
    ['non-empty fields', aggregates({ latest_log_at: '2026-06-22T12:00:00.000Z', data: NONEMPTY_FIELDS }), WIDE_WINDOW, false],
    ['non-empty time_series', aggregates({ latest_log_at: '2026-06-22T12:00:00.000Z', time_series: [{ time: '2026-06-22T12:00:00.000Z', value: 5 }] }), WIDE_WINDOW, false],
    ['empty + latest_log_at INSIDE window', aggregates({ earliest_log_at: '2026-06-21T06:00:00.000Z', latest_log_at: '2026-06-22T12:00:00.000Z' }), WIDE_WINDOW, true],
    ['empty + data span BEFORE window', aggregates({ earliest_log_at: '2026-06-18T00:00:00.000Z', latest_log_at: '2026-06-19T00:00:00.000Z' }), { startTime: '2026-06-21T00:00:00.000Z', endTime: '2026-06-22T00:00:00.000Z' }, false],
    ['empty + data span AFTER window', aggregates({ earliest_log_at: '2026-06-22T10:00:00.000Z', latest_log_at: '2026-06-22T12:00:00.000Z' }), { startTime: '2026-06-20T00:00:00.000Z', endTime: '2026-06-21T00:00:00.000Z' }, false],
    ['date-only extents overlapping window', aggregates({ earliest_log_at: '2026-06-22', latest_log_at: '2026-06-22' }), WIDE_WINDOW, true],
    ['date-only extents non-overlapping window', aggregates({ earliest_log_at: '2026-06-22', latest_log_at: '2026-06-22' }), { startTime: '2026-06-19T00:00:00.000Z', endTime: '2026-06-20T00:00:00.000Z' }, false],
    ['no window supplied (window-agnostic check)', aggregates({ latest_log_at: '2026-06-22T12:00:00.000Z' }), undefined, true],
  ] as const)('throwIfStaleAggregates: %s -> stale=%s', (_label, data, win, expected) => {
    expect(isFlaggedStale(data, win)).toBe(expected)
  })

  it('does NOT flag when a filter is active and returns original data when not stale', () => {
    const staleData = aggregates({ earliest_log_at: '2026-06-21T06:00:00.000Z', latest_log_at: '2026-06-22T12:00:00.000Z' })
    expect(isFlaggedStale(staleData, WIDE_WINDOW)).toBe(true)
    expect(throwIfStaleAggregates(staleData, WIDE_WINDOW, true)).toBe(staleData)

    const validData = aggregates({ latest_log_at: '2026-06-22T12:00:00.000Z', data: NONEMPTY_FIELDS })
    expect(throwIfStaleAggregates(validData, WIDE_WINDOW)).toBe(validData)
  })

  it('isStaleDashboardViewError and STALE_VIEW_RETRY_OPTIONS', () => {
    let staleErr!: Error
    try {
      throwIfStaleAggregates(aggregates({ latest_log_at: '2026-06-22T12:00:00.000Z' }), WIDE_WINDOW)
    } catch (e) {
      staleErr = e as Error
    }
    expect(isStaleDashboardViewError(staleErr)).toBe(true)
    for (const other of [new Error('boom'), null, 'nope']) expect(isStaleDashboardViewError(other)).toBe(false)

    expect(STALE_VIEW_RETRY_OPTIONS.retry(0, staleErr)).toBe(true)
    expect(STALE_VIEW_RETRY_OPTIONS.retry(1, staleErr)).toBe(true)
    expect(STALE_VIEW_RETRY_OPTIONS.retry(2, staleErr)).toBe(false)
    expect(STALE_VIEW_RETRY_OPTIONS.retry(0, new Error('500'))).toBe(false)

    expect(STALE_VIEW_RETRY_OPTIONS.refetchInterval({ state: { error: staleErr, data: undefined } })).toBe(STALE_VIEW_POLL_MS)
    expect(STALE_VIEW_RETRY_OPTIONS.refetchInterval({ state: { error: staleErr, data: { aggregates: {} } } })).toBe(false)
    expect(STALE_VIEW_RETRY_OPTIONS.refetchInterval({ state: { error: new Error('500'), data: undefined } })).toBe(false)
    expect(STALE_VIEW_RETRY_OPTIONS.refetchInterval({ state: { error: null, data: undefined } })).toBe(false)
  })
})

const withJsonServer = <T>(body: unknown, fn: Parameters<typeof withTestHttpServer<T>>[1], status = 200) =>
  withTestHttpServer((_req, res) => {
    res.statusCode = status
    res.setHeader('Content-Type', 'application/json')
    res.end(typeof body === 'string' ? body : JSON.stringify(body))
  }, fn)

describe('fetchBootstrapServerSide & fetchTosServerSide', () => {
  beforeEach(() => {
    delete process.env.ADMIN_GATEWAY_SECRET
    delete process.env.ADMIN_GATEWAY_REQUIRED
    setCookies('session=abc123')
    inbound({ 'x-proxied-by-caddy': 'true', host: PUBLIC_HOST })
  })

  afterEach(() => vi.restoreAllMocks())

  it.each([
    ['fetchBootstrapServerSide', fetchBootstrapServerSide, { active_service_id: 'svc-1' }],
    ['fetchTosServerSide', fetchTosServerSide, { version: 'v1', text: 'TOS body' }],
  ] as const)('%s handles unset proxy, non-2xx, malformed JSON, Caddy analyst, fail-closed, and loopback admin', async (_name, fn, sampleBody) => {
    expect(await fn()).toBeNull()

    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    expect(await fn()).toBeNull()
    expect(warn).toHaveBeenCalled()

    warn.mockClear()
    await withJsonServer('not json {{{', async () => {
      expect(await fn()).toBeNull()
      expect(warn).toHaveBeenCalled()
    })

    setCookies('session=xyz; theme=dark')
    inbound({ 'x-proxied-by-caddy': 'true', host: PUBLIC_HOST })
    await withJsonServer(sampleBody, async (srv) => {
      expect(await fn()).toEqual(sampleBody)
      const h = srv.lastRequest!.headers
      expect(h['x-remote-analyst']).toBe('1')
      expect(h.host).toBe(PUBLIC_HOST)
      expect(h.cookie).toBe('session=xyz; theme=dark')
    })

    for (const hdrs of [{ host: PUBLIC_HOST }, {}]) {
      process.env.API_PROXY_URL = 'http://127.0.0.1:1'
      setCookies('')
      inbound(hdrs as Record<string, string>)
      warn.mockClear()
      expect(await fn()).toBeNull()
      expect(warn).toHaveBeenCalledWith(expect.stringContaining('no X-Proxied-By-Caddy marker'))
    }

    setCookies('')
    inbound({ host: 'localhost:3001' })
    await withJsonServer(sampleBody, async (srv) => {
      expect(await fn()).toEqual(sampleBody)
      expect(srv.lastRequest?.headers['x-remote-analyst']).toBeUndefined()
    })
  })

  it("fetchTosServerSide returns 'unauthenticated' on a 401 upstream response", async () => {
    await withJsonServer({ detail: 'unauthenticated' }, async () => {
      expect(await fetchTosServerSide()).toBe('unauthenticated')
    }, 401)
  })
})

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
      await withJsonServer(payload, async (srv) => {
        const now = new Date('2026-10-23T12:00:37Z')
        const seed = await fetchStreamingServerSide('svc-streaming-test', now)
        expect(seed).not.toBeNull()
        if (!seed) throw new Error('Streaming SSR did not produce a seed')
        const req = srv.lastRequest!
        expect(req.method).toBe('POST')
        expect(req.url).toBe('/api/cmcd/aggregates')
        expect(JSON.parse(req.body)).toEqual({
          filters: {}, bucket_seconds: 300, top_n: 30, range_token: '24h', anchor: '2026-10-23T12:00:00Z',
        })
        expect(req.headers.cookie).toBe('fla_streaming_fixture=test-fixture')
        expect(req.headers['x-service-id']).toBe('svc-streaming-test')
        expect(req.headers['x-remote-analyst']).toBe(analyst ? '1' : undefined)

        const anchor = quantizeAnchor(now.toISOString(), now)
        const { rangeKey } = resolveRangeWire({ relativeRange: null, isAutoRange: true, startTime: null, endTime: null, anchor })
        const seedKey = ['cmcd', 'aggregates', 'svc-streaming-test', seed.rangeToken, seed.anchor, {}]
        const browserKey = ['cmcd', 'aggregates', 'svc-streaming-test', rangeKey, anchor, {}]
        const dehydratedState = seedDehydratedState(seedKey, seed.data)
        expect(dehydratedState).not.toBeNull()
        if (!dehydratedState) throw new Error('Streaming SSR did not produce dehydrated state')
        hydrate(queryClient, dehydratedState)
        expect(queryClient.getQueryData(browserKey)).toEqual(payload)
        expect(queryClient.getQueryState(browserKey)?.dataUpdatedAt).toBeGreaterThan(0)
      })
    } finally {
      queryClient.clear()
    }
  })
})

describe('resolveDefaultSsrRangeKey & SSR-seed key ↔ client first-paint key byte-match', () => {
  it('cold load → rangeToken "24h", anchor floored to the 60s grid', () => {
    const now = new Date('2026-06-29T12:00:37.123Z')
    const got = resolveDefaultSsrRangeKey(now)
    expect(got.rangeToken).toBe('24h')
    expect(got.anchor).toBe('2026-06-29T12:00:00Z')
    expect(got.anchor).toBe(quantizeAnchor(now.toISOString(), now))

    const a = resolveDefaultSsrRangeKey(new Date('2026-06-29T12:00:01Z'))
    const b = resolveDefaultSsrRangeKey(new Date('2026-06-29T12:00:58Z'))
    expect(a.anchor).toBe(b.anchor)
    expect(a.anchor).toBe('2026-06-29T12:00:00Z')
  })

  it.each([
    {
      name: 'Performance',
      buildKeys: (serviceId: string, rangeToken: string, anchor: string) => [
        ['performance', 'aggregates', serviceId, rangeToken, anchor, {}, PERFORMANCE_SSR_DEFAULTS.sortBy],
        ['performance', 'aggregates', serviceId, rangeToken, anchor, {}, 'p99'],
      ],
    },
    {
      name: 'Security',
      buildKeys: (serviceId: string, rangeToken: string, anchor: string) => [
        ['security', 'aggregates', serviceId, rangeToken, anchor, {}, SECURITY_SSR_DEFAULTS.bucketSeconds],
        ['security', 'aggregates', serviceId, rangeToken, anchor, {}, 3600],
      ],
    },
    {
      name: 'Origin',
      buildKeys: (serviceId: string, rangeToken: string, anchor: string) => [
        ['origin', 'aggregates', serviceId, rangeToken, anchor, {}, ORIGIN_SSR_DEFAULTS.bucketMinutes, ORIGIN_SSR_DEFAULTS.metric, ORIGIN_SSR_DEFAULTS.percentile, ORIGIN_SSR_SECTIONS],
        ['origin', 'aggregates', serviceId, rangeToken, anchor, {}, 60, 'ttfb', 'p95', ['summary', 'timeseries', 'slow_urls', 'status_codes', 'path_breakdown', 'pop_latency', 'ip_health']],
      ],
    },
  ])('$name SSR-seed key === client useServiceQuery key', ({ buildKeys }) => {
    const { rangeToken, anchor } = resolveDefaultSsrRangeKey(NOW_ISO)
    const [seedKey, clientKey] = buildKeys('svc-1', rangeToken, anchor)
    expect(hashKey(seedKey)).toBe(hashKey(clientKey))
  })
})

describe('SSR page data fetchers (Performance, Security, Insights, Origin)', () => {
  it('resolveInsightsDefault mirrors useInsightsDefaults and falls back to STATIC_DEFAULT', () => {
    const earliest = new Date(Date.now() - 10 * 24 * 3600 * 1000).toISOString()
    const expected = pickInsightsDefault(historyHoursFromExtents(earliest))
    expect(resolveInsightsDefault(earliest)).toEqual({ windowHours: expected.window, baselineHours: expected.baseline })
    expect(resolveInsightsDefault(null)).toEqual({ windowHours: '24', baselineHours: '168' })
    expect(resolveInsightsDefault(undefined)).toEqual({ windowHours: '24', baselineHours: '168' })
  })

  it.each([
    ['fetchPerformanceServerSide', () => fetchPerformanceServerSide(undefined), () => fetchPerformanceServerSide('svc-1')],
    ['fetchSecurityServerSide', () => fetchSecurityServerSide(undefined), () => fetchSecurityServerSide('svc-1')],
    ['fetchInsightsServerSide', () => fetchInsightsServerSide(undefined, null), () => fetchInsightsServerSide('svc-1', null)],
    ['fetchOriginServerSide', () => fetchOriginServerSide(undefined), () => fetchOriginServerSide('svc-1')],
  ])('%s returns null without serviceId and fails closed on non-loopback Host without Caddy marker', async (_name, callNoSvc, callWithSvc) => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    expect(await callNoSvc()).toBeNull()

    setHeaders({ host: PUBLIC_HOST })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await withJsonServer({}, async (srv) => {
      expect(await callWithSvc()).toBeNull()
      expect(srv.requests).toHaveLength(0)
      expect(warn).toHaveBeenCalled()
    })
  })

  it('fetchPerformanceServerSide admin SSH-tunnel POSTs unified aggregates and fails safely on 500', async () => {
    setHeaders({ host: 'localhost:3001' })
    await withJsonServer({ ok: true }, async (srv) => {
      const out = await fetchPerformanceServerSide('svc-1', NOW_ISO)
      expect(out).toEqual({ data: { ok: true }, rangeToken: '24h', anchor: '2026-06-29T12:00:00Z' })
      expect(srv.requests).toHaveLength(1)
      expect(JSON.parse(srv.requests[0].body)).toEqual({
        filters: {}, sort_by: PERFORMANCE_SSR_DEFAULTS.sortBy, range_token: '24h', anchor: '2026-06-29T12:00:00Z',
      })
      expect(srv.requests[0].headers['x-remote-analyst']).toBeUndefined()
      expect(srv.requests[0].headers['x-service-id']).toBe('svc-1')
    })

    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    try {
      await withJsonServer('boom', async () => {
        expect(await fetchPerformanceServerSide('svc-1')).toBeNull()
      }, 500)
    } finally {
      warn.mockRestore()
    }
  })

  it('fetchSecurityServerSide admin SSH-tunnel and analyst Caddy marker POST expected bodies', async () => {
    setHeaders({ host: 'localhost:3001' })
    await withJsonServer({ ngwaf_verified_bots: { rows: [] } }, async (srv) => {
      const out = await fetchSecurityServerSide('svc-1', NOW_ISO)
      expect(out).toEqual({ data: { ngwaf_verified_bots: { rows: [] } }, rangeToken: '24h', anchor: '2026-06-29T12:00:00Z' })
      const req = srv.lastRequest!
      expect(req.method).toBe('POST')
      expect(req.headers['content-type']).toContain('application/json')
      const body = JSON.parse(req.body)
      expect(body).toEqual({
        filters: {}, bucket_seconds: SECURITY_SSR_DEFAULTS.bucketSeconds,
        sections: [...SECURITY_SSR_SECTIONS], range_token: '24h', anchor: '2026-06-29T12:00:00Z',
      })
      expect(req.headers['x-remote-analyst']).toBeUndefined()
      expect(req.headers['x-service-id']).toBe('svc-1')
    })

    setHeaders({ 'x-proxied-by-caddy': 'true', host: PUBLIC_HOST })
    await withJsonServer({ ngwaf_verified_bots: { rows: [] } }, async (srv) => {
      expect(await fetchSecurityServerSide('svc-1')).not.toBeNull()
      const req = srv.lastRequest!
      expect(req.headers['x-remote-analyst']).toBe('1')
      expect(req.headers['x-service-id']).toBe('svc-1')
    })
  })

  it('fetchInsightsServerSide admin SSH-tunnel and analyst Caddy marker POST expected bodies', async () => {
    setHeaders({ host: 'localhost:3001' })
    await withJsonServer({ insights: [1, 2, 3] }, async (srv) => {
      const earliest = new Date(Date.now() - 10 * 24 * 3600 * 1000).toISOString()
      const out = await fetchInsightsServerSide('svc-1', { earliest_log_at: earliest })
      expect(out).toEqual({ data: { insights: [1, 2, 3] }, windowHours: '24', baselineHours: '168' })
      const req = srv.lastRequest!
      expect(req.method).toBe('POST')
      expect(JSON.parse(req.body)).toEqual({ window_size_hrs: 24, baseline_hours: 168, filters: {} })
      expect(req.headers['x-remote-analyst']).toBeUndefined()
      expect(req.headers['x-service-id']).toBe('svc-1')
    })

    setHeaders({ 'x-proxied-by-caddy': 'true', host: PUBLIC_HOST })
    await withJsonServer({ insights: [1] }, async (srv) => {
      expect((await fetchInsightsServerSide('svc-1', null))?.data).toEqual({ insights: [1] })
      const req = srv.lastRequest!
      expect(req.headers['x-remote-analyst']).toBe('1')
      expect(req.headers['host']).toBe(PUBLIC_HOST)
      expect(req.headers['x-service-id']).toBe('svc-1')
    })
  })

  it('fetchOriginServerSide admin SSH-tunnel and analyst Caddy marker POST expected bodies', async () => {
    setHeaders({ host: 'localhost:3001' })
    await withJsonServer({ summary: { has_data: true } }, async (srv) => {
      const out = await fetchOriginServerSide('svc-1', NOW_ISO)
      expect(out).toEqual({ data: { summary: { has_data: true } }, rangeToken: '24h', anchor: '2026-06-29T12:00:00Z' })
      const req = srv.lastRequest!
      expect(req.method).toBe('POST')
      expect(JSON.parse(req.body)).toEqual({
        filters: {}, bucket_minutes: ORIGIN_SSR_DEFAULTS.bucketMinutes, split_by_leg: false,
        timeseries_metric: ORIGIN_SSR_DEFAULTS.metric, timeseries_percentile: ORIGIN_SSR_DEFAULTS.percentile,
        slow_urls_limit: ORIGIN_SSR_DEFAULTS.slowUrlsLimit, slow_urls_min_requests: ORIGIN_SSR_DEFAULTS.slowUrlsMinRequests,
        pop_latency_limit: ORIGIN_SSR_DEFAULTS.popLatencyLimit, ip_health_limit: ORIGIN_SSR_DEFAULTS.ipHealthLimit,
        sections: [...ORIGIN_SSR_SECTIONS], range_token: '24h', anchor: '2026-06-29T12:00:00Z',
      })
      expect(req.headers['x-remote-analyst']).toBeUndefined()
      expect(req.headers['x-service-id']).toBe('svc-1')
    })

    setHeaders({ 'x-proxied-by-caddy': 'true', host: PUBLIC_HOST })
    await withJsonServer({ summary: { has_data: true } }, async (srv) => {
      expect((await fetchOriginServerSide('svc-1'))?.data).toEqual({ summary: { has_data: true } })
      const req = srv.lastRequest!
      expect(req.headers['x-remote-analyst']).toBe('1')
      expect(req.headers['host']).toBe(PUBLIC_HOST)
      expect(req.headers['x-service-id']).toBe('svc-1')
    })
  })
})

const secret = 'test-gateway-secret-with-at-least-32-characters'
let upstreamBody: object

describe('certificate gateway SSR trust', () => {
  beforeEach(() => {
    upstreamBody = {}
    process.env.ADMIN_GATEWAY_SECRET = secret
    process.env.ADMIN_GATEWAY_REQUIRED = '1'
    vi.spyOn(console, 'warn').mockImplementation(() => {})
  })

  afterEach(() => {
    delete process.env.ADMIN_GATEWAY_SECRET
    delete process.env.ADMIN_GATEWAY_REQUIRED
    vi.restoreAllMocks()
  })

  it('forwards authenticated admin traffic on a non-loopback hostname', async () => {
    inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret })
    await withTestHttpServer(
      (_req, res) => { res.end(JSON.stringify(upstreamBody)) },
      async (srv) => {
        const response = await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })
        expect(response?.statusCode).toBe(200)
        expect(srv.lastRequest?.headers['x-admin-gateway-token']).toBe(secret)
        expect(srv.lastRequest?.headers.host).toBe('admin.example.com')
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBeUndefined()
      },
    )
  })

  it.each([
    ['rejects a forged credential even with a localhost Host', { host: 'localhost:3000', 'x-admin-gateway-token': 'forged' }],
    ['does not prefetch admin data for a cross-site browser request', { host: 'admin.example.com', 'x-admin-gateway-token': secret, 'sec-fetch-site': 'cross-site' }],
    ['disables the old localhost admin shortcut in required mode', { host: 'localhost:3000' }],
    ['never promotes a public marked request even if it carries a gateway token', { host: 'analyst.example.com', 'x-proxied-by-caddy': '1', 'x-admin-gateway-token': secret }],
  ])('%s', async (_label, hdrs) => {
    inbound(hdrs as Record<string, string>)
    await withTestHttpServer(
      (_req, res) => { res.end(JSON.stringify(upstreamBody)) },
      async (srv) => {
        expect(await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })).toBeNull()
        expect(srv.lastRequest).toBeUndefined()
      },
    )
  })

  it('keeps public ingress traffic on the analyst path and prevents extraHeaders from forging gateway credentials', async () => {
    inbound({ host: 'analyst.example.com', 'x-proxied-by-caddy': '1' })
    await withTestHttpServer(
      (_req, res) => { res.end(JSON.stringify(upstreamBody)) },
      async (srv) => {
        expect((await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' }))?.statusCode).toBe(200)
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBe('1')
        expect(srv.lastRequest?.headers['x-admin-gateway-token']).toBeUndefined()
        expect(srv.lastRequest?.headers.host).toBe('analyst.example.com')

        srv.clearRequests()
        await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test', extraHeaders: { 'x-admin-gateway-token': secret } })
        expect(srv.lastRequest?.headers['x-admin-gateway-token']).toBeUndefined()
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBe('1')
      },
    )
  })

  it('isolates bootstrap cache across uncredentialed localhost, public analyst, analyst cookie, and analyst-scoped responses', async () => {
    await withTestHttpServer(
      (_req, res) => { res.end(JSON.stringify(upstreamBody)) },
      async (srv) => {
        inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret, 'x-service-id': 'gateway-cache-test' })
        expect(await fetchBootstrapServerSide()).toEqual({})
        srv.clearRequests()
        inbound({ host: 'localhost:3000', 'x-service-id': 'gateway-cache-test' })
        expect(await fetchBootstrapServerSide()).toBeNull()
        expect(srv.lastRequest).toBeUndefined()

        inbound({ host: 'analyst.example.com', 'x-proxied-by-caddy': '1', 'x-service-id': 'gateway-cache-test' })
        expect(await fetchBootstrapServerSide()).toEqual({})
        srv.clearRequests()
        expect(await fetchBootstrapServerSide()).toEqual({})
        expect(srv.lastRequest?.headers['x-remote-analyst']).toBe('1')

        inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret, 'x-service-id': 'analyst-cookie-cache-test' })
        expect(await fetchBootstrapServerSide()).toEqual({})
        srv.clearRequests()
        setCookies('analyst_session_id=test-session', (name: string) => (name === 'analyst_session_id' ? { value: 'test-session' } : undefined))
        expect(await fetchBootstrapServerSide()).toEqual({})
        expect(srv.lastRequest?.headers).toEqual(expect.objectContaining({ cookie: 'analyst_session_id=test-session' }))
        srv.clearRequests()
        setCookies('')
        expect(await fetchBootstrapServerSide()).toEqual({})
        expect(srv.lastRequest).toBeUndefined()

        inbound({ host: 'admin.example.com', 'x-admin-gateway-token': secret, 'x-service-id': 'analyst-response-cache-test' })
        upstreamBody = { settings: { is_remote_analyst: true } }
        expect(await fetchBootstrapServerSide()).toEqual(upstreamBody)
        srv.clearRequests()
        upstreamBody = { settings: { is_remote_analyst: false } }
        expect(await fetchBootstrapServerSide()).toEqual(upstreamBody)
        expect(srv.lastRequest?.headers).toBeDefined()
      },
    )
  })
})

describe('fetchCronRunsServerSide & fetchUsageLogServerSide', () => {
  const usageArgs = { serviceId: 'svc-1', start: '2026-06-01', end: '2026-06-17', pageSize: 50 }

  beforeEach(() => {
    delete process.env.ADMIN_GATEWAY_SECRET
    delete process.env.ADMIN_GATEWAY_REQUIRED
  })

  afterEach(() => vi.restoreAllMocks())

  it('fetchCronRunsServerSide returns null without a serviceId', async () => {
    process.env.API_PROXY_URL = 'http://127.0.0.1:1'
    expect(await fetchCronRunsServerSide(undefined)).toBeNull()
  })

  it.each([
    ['fetchCronRunsServerSide', () => fetchCronRunsServerSide('svc-1')],
    ['fetchUsageLogServerSide', () => fetchUsageLogServerSide(usageArgs)],
  ])('%s fail-closed: no Caddy marker + non-loopback Host → null, never forwarded', async (_name, call) => {
    inbound({ host: PUBLIC_HOST })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await withJsonServer({ rows: [] }, async (srv) => {
      expect(await call()).toBeNull()
      expect(srv.requests).toHaveLength(0)
      expect(warn).toHaveBeenCalledWith(expect.stringContaining('non-loopback/absent Host'))
    })
  })

  it('forwards admin SSH-tunnel and analyst Caddy marker requests with expected headers/query', async () => {
    inbound({ host: 'localhost:3001' })
    await withJsonServer({ rows: [1, 2, 3] }, async (srv) => {
      expect(await fetchCronRunsServerSide('svc-1')).toEqual({ rows: [1, 2, 3] })
      expect(srv.lastRequest?.headers['x-remote-analyst']).toBeUndefined()
      expect(srv.lastRequest?.headers['x-fastly-service-id']).toBe('svc-1')
    })

    inbound({ 'x-proxied-by-caddy': 'true', host: PUBLIC_HOST })
    await withJsonServer({ rows: [1, 2, 3] }, async (srv) => {
      expect(await fetchCronRunsServerSide('svc-1')).toEqual({ rows: [1, 2, 3] })
      expect(srv.lastRequest?.headers['x-remote-analyst']).toBe('1')
      expect(srv.lastRequest?.headers['x-fastly-service-id']).toBe('svc-1')
    })

    inbound({ host: 'localhost:3001' })
    await withJsonServer({ rows: [1] }, async (srv) => {
      expect(await fetchUsageLogServerSide(usageArgs)).toEqual({ rows: [1] })
      expect(srv.lastRequest?.headers['x-remote-analyst']).toBeUndefined()
      expect(srv.lastRequest?.url).toContain('/api/admin/usage-log?')
      expect(srv.lastRequest?.url).toContain('service_id=svc-1')
    })
  })
})

describe('ssrUpstreamGet — debug-responses cookie propagation & ECONNRESET retry', () => {
  beforeEach(() => {
    delete process.env.ADMIN_GATEWAY_SECRET
    delete process.env.ADMIN_GATEWAY_REQUIRED
  })

  afterEach(() => vi.restoreAllMocks())

  it.each([
    { label: 'cookie=1 + loopback Host (admin branch) → x-debug-responses: 1', cookieVal: '1', hdrs: { host: 'localhost:3001' }, expected: '1' },
    { label: 'cookie=1 + Caddy marker (analyst branch) → header NOT sent', cookieVal: '1', hdrs: { 'x-proxied-by-caddy': 'true', host: PUBLIC_HOST }, expected: undefined },
    { label: 'cookie absent → header not sent', cookieVal: undefined, hdrs: { host: 'localhost:3001' }, expected: undefined },
    { label: 'cookie=0 → header not sent', cookieVal: '0', hdrs: { host: 'localhost:3001' }, expected: undefined },
  ])('$label', async ({ cookieVal, hdrs, expected }) => {
    if (cookieVal === undefined) setCookies('')
    else setCookies(`fla.debugResponses=${cookieVal}`, (k: string) => (k === 'fla.debugResponses' ? { value: cookieVal } : undefined))
    inbound(hdrs as Record<string, string>)
    await withJsonServer({}, async (srv) => {
      expect(await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })).not.toBeNull()
      expect(srv.lastRequest?.headers['x-debug-responses']).toBe(expected)
    })
  })

  it('disables keep-alive socket reuse (connection: close) and retries once on ECONNRESET', async () => {
    let attempts = 0
    let secondConnHeader: string | string[] | undefined
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    setCookies('')
    inbound({ host: 'localhost:3001' })
    await withTestHttpServer(
      (req, res) => {
        attempts += 1
        if (attempts === 1) { req.socket.destroy(); return }
        secondConnHeader = req.headers.connection
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('{"recovered":true}')
      },
      async () => {
        const out = await ssrUpstreamGet({ path: '/api/bootstrap', logPrefix: 'test' })
        expect(out?.statusCode).toBe(200)
        expect(out?.body).toBe('{"recovered":true}')
        expect(attempts).toBe(2)
        expect(secondConnHeader).toBe('close')
        expect(warnSpy).not.toHaveBeenCalled()
      },
    )
  })
})

describe('Insights page hydration', () => {
  it.each([undefined, 'svc-2'])('scopes first-render extents to the bootstrap service (override: %s)', async (service) => {
    vi.resetModules()
    setHeaders({ host: 'localhost:3001' })
    const extents = { earliest_log_at: new Date(Date.now() - 36 * 3600_000).toISOString(), latest_log_at: new Date().toISOString() }
    await withTestHttpServer(
      (req, res) => {
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify(req.url === '/api/bootstrap' ? { active_service_id: 'svc-1', log_extents: extents } : { insights: [] }))
      },
      async () => {
        const { default: InsightsPage } = await import('@/app/insights/page')
        const page = await InsightsPage({ searchParams: Promise.resolve({ service }) })
        const queries = page.props.state.queries
        expect(queries).toEqual(expect.arrayContaining([expect.objectContaining({ queryKey: ['insights', service ?? 'svc-1', '4', '24'] })]))
        const extentsQuery = queries.find((query: { queryKey: readonly unknown[] }) => query.queryKey[0] === 'log-extents')
        if (service) expect(extentsQuery).toBeUndefined()
        else expect(extentsQuery).toMatchObject({ queryKey: ['log-extents', 'svc-1'], state: { data: extents } })
      },
    )
  })
})
