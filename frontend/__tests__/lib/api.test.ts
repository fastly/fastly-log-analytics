/**
 * @vitest-environment jsdom
 *
 * Consolidated MSW-driven contract tests for the API client (`lib/api.ts`)
 * and `customFieldsApi` (`lib/api/custom-fields.ts`).
 */
import { renderHook, waitFor } from '@testing-library/react'
import { useMutation, useQuery, type QueryKey } from '@tanstack/react-query'
import { createTestQueryClient, makeQueryWrapper, invalidInput } from '../helpers/page-smoke'
import { http, HttpResponse } from 'msw'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

import { server } from '@/__tests__/helpers/msw/handlers'
import type { components } from '@/types/api.generated'

const API_BASE = 'http://127.0.0.1:8000'
const SVC = 'svc-test'

const apiBase = vi.hoisted(() => ({ value: 'http://127.0.0.1:8000' }))
vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>()
  return {
    ...actual,
    getApiBase: () => apiBase.value,
  }
})

const mockServiceState = { activeServiceId: 'svc-test' as string | null }

vi.mock('@/lib/utils', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/utils')>()),
  isUserActive: vi.fn(() => true),
  ACTIVE_WINDOW_MS: 120_000,
}))

vi.mock('@/stores/filterStore', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/stores/filterStore')>()
  const useServiceStore = Object.assign(
    vi.fn((selector?: (s: typeof mockServiceState) => unknown) =>
      selector ? selector(mockServiceState) : mockServiceState,
    ),
    { getState: () => mockServiceState },
  )
  const state = { enabled: false, apiCallsEnabled: false }
  const useDebugStore = Object.assign(
    vi.fn(() => state),
    { getState: () => state },
  )
  return { ...actual, useServiceStore, useDebugStore }
})

import {
  client,
  extractApiError,
  customFieldsApi,
  adminJson,
  adminJsonResult,
  queryKeys,
  throwIfStaleAggregates,
  isStaleDashboardViewError,
  STALE_VIEW_RETRY_OPTIONS,
  STALE_VIEW_POLL_MS,
  type CustomField,
  type VclLintRequest,
} from '@/lib/api'
import { isUserActive, arrayOrEmpty, recordOrNull } from '@/lib/utils'
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
  it('returns string-detail verbatim', () => {
    expect(extractApiError({ detail: 'Service not found' })).toBe('Service not found')
  })

  it('joins detail.errors[] (FastAPI 422 with our custom shape)', () => {
    expect(
      extractApiError({ detail: { errors: ['LOG_FORMAT_TOO_LONG: too big', 'BAD_NAME: nope'] } }),
    ).toBe('LOG_FORMAT_TOO_LONG: too big, BAD_NAME: nope')
  })

  it('falls back to detail.error scalar', () => {
    expect(extractApiError({ detail: { error: 'broken' } })).toBe('broken')
  })

  it('formats Pydantic detail[] validation arrays', () => {
    const msg = extractApiError({
      detail: [{ loc: ['body', 'name'], msg: 'required' }],
    })
    expect(msg).toContain('name: required')
  })

  it('reads top-level error', () => {
    expect(extractApiError({ error: 'kapow' })).toBe('kapow')
  })

  it('returns a sane default for null/undefined', () => {
    expect(extractApiError(null)).toBe('Unknown error')
    expect(extractApiError(undefined)).toBe('Unknown error')
  })
})

describe('API client onResponse middleware (MSW)', () => {
  it('422 detail.errors[] reaches the caller as a joined Error.message', async () => {
    server.use(
      http.post(`${API_BASE}/api/services/svc-test/custom-fields`, () =>
        HttpResponse.json(
          {
            detail: {
              errors: ['LOG_FORMAT_TOO_LONG: Log format is 8050 chars; safe max 8000'],
            },
          },
          { status: 422 },
        ),
      ),
    )

    const { result } = renderHook(
      () =>
        useMutation({
          mutationFn: async () => {
            const { data } = await client.POST(
              '/api/services/{service_id}/custom-fields',
              {
                params: { path: { service_id: 'svc-test' } },
                body: { name: 'whatever' } as components['schemas']['CustomFieldCreate'],
              },
            )
            return data
          },
        }),
      { wrapper: wrapper() },
    )

    result.current.mutate()
    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(result.current.error).toBeInstanceOf(Error)
    expect(result.current.error?.message).toContain('LOG_FORMAT_TOO_LONG')
    expect(result.current.error?.message).toContain('8000')
  })

  it('500 string-detail produces a readable Error.message', async () => {
    server.use(
      http.post(`${API_BASE}/api/dashboard/aggregates`, () =>
        HttpResponse.json({ detail: 'Internal server error' }, { status: 500 }),
      ),
    )

    const { result } = renderHook(
      () =>
        useMutation({
          mutationFn: async () => {
            const { data } = await client.POST('/api/dashboard/aggregates', {
              body: {
                start_time: '2026-01-01T00:00:00Z',
                end_time: '2026-01-01T01:00:00Z',
                filters: invalidInput([]),
                chart_metric: 'requests',
                chart_interval: 'minute',
              },
            })
            return data
          },
        }),
      { wrapper: wrapper() },
    )

    result.current.mutate()
    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(result.current.error?.message).toBe('Internal server error')
  })

  it('401 plain message becomes a readable Error.message (not "[object Object]")', async () => {
    server.use(
      http.post(`${API_BASE}/api/dashboard/aggregates`, () =>
        HttpResponse.json({ detail: 'Not authenticated' }, { status: 401 }),
      ),
    )

    const { result } = renderHook(
      () =>
        useMutation({
          mutationFn: async () => {
            const { data } = await client.POST('/api/dashboard/aggregates', {
              body: invalidInput({
                start_time: '2026-01-01T00:00:00Z',
                end_time: '2026-01-01T01:00:00Z',
                filters: [],
              }),
            })
            return data
          },
        }),
      { wrapper: wrapper() },
    )

    result.current.mutate()
    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(result.current.error?.message).toBe('Not authenticated')
    expect(result.current.error?.message).not.toContain('[object Object]')
    expect(result.current.error?.message).not.toContain('{')
  })

  it('500 with a missing body still produces some message', async () => {
    server.use(
      http.post(`${API_BASE}/api/dashboard/aggregates`, () =>
        HttpResponse.text('', { status: 500 }),
      ),
    )

    const { result } = renderHook(
      () =>
        useMutation({
          mutationFn: async () => {
            const { data } = await client.POST('/api/dashboard/aggregates', {
              body: {} as components['schemas']['AggregatesRequest'],
            })
            return data
          },
        }),
      { wrapper: wrapper() },
    )

    result.current.mutate()
    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(result.current.error?.message).toBeTruthy()
    expect(result.current.error?.message.length).toBeGreaterThan(0)
  })

  it('useQuery error path leaves isError=true and data undefined', async () => {
    server.use(
      http.get(`${API_BASE}/api/log-fields/catalog`, () =>
        HttpResponse.json({ detail: 'catalog fetch broke' }, { status: 500 }),
      ),
    )

    const { result } = renderHook(
      () =>
        useQuery({
          queryKey: ['catalog'],
          queryFn: async () => {
            const { data } = await client.GET('/api/log-fields/catalog')
            return data
          },
        }),
      { wrapper: wrapper() },
    )

    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(result.current.data).toBeUndefined()
    expect(result.current.error?.message).toBe('catalog fetch broke')
  })
})

describe('serviceless service-discovery exemption (fresh install)', () => {
  beforeEach(() => {
    mockServiceState.activeServiceId = null
  })

  it('does NOT abort GET /api/services when no service is active', async () => {
    let reached = false
    server.use(
      http.get(`${API_BASE}/api/services`, () => {
        reached = true
        return HttpResponse.json({ services: [] })
      }),
    )
    const { data, error } = await client.GET('/api/services')
    expect(reached).toBe(true)
    expect(error).toBeUndefined()
    expect(data).toEqual({ services: [] })
  })

  it('does NOT abort GET /api/log-fields/catalog when no service is active', async () => {
    let reached = false
    server.use(
      http.get(`${API_BASE}/api/log-fields/catalog`, () => {
        reached = true
        return HttpResponse.json({ groups: [{ id: 'A' }], fields: [], presets: {}, insights: [] })
      }),
    )
    const { data, error } = await client.GET('/api/log-fields/catalog')
    expect(reached).toBe(true)
    expect(error).toBeUndefined()
    expect(data).toMatchObject({ groups: [{ id: 'A' }] })
  })

  it('STILL aborts a service-scoped subpath (/api/services/{id}/…)', async () => {
    let reached = false
    server.use(
      http.get(`${API_BASE}/api/services/:id/lake-info`, () => {
        reached = true
        return HttpResponse.json({ ok: true })
      }),
    )
    await expect(
      client.GET('/api/services/{service_id}/lake-info', {
        params: invalidInput({ path: { service_id: 'svc-123' } }),
      }),
    ).rejects.toMatchObject({ code: 'NO_SERVICE' })
    expect(reached).toBe(false)
  })
})

describe('serviceless global-admin observability exemption (fresh install)', () => {
  beforeEach(() => {
    mockServiceState.activeServiceId = null
  })

  it('does NOT abort GET /api/admin/health-snapshot', async () => {
    let reached = false
    server.use(
      http.get(`${API_BASE}/api/admin/health-snapshot`, () => {
        reached = true
        return HttpResponse.json({ vcpus: 4 })
      }),
    )
    const { error } = await client.GET('/api/admin/health-snapshot')
    expect(reached).toBe(true)
    expect(error).toBeUndefined()
  })

  it('does NOT abort GET /api/admin/metric-history/batch', async () => {
    let reached = false
    server.use(
      http.get(`${API_BASE}/api/admin/metric-history/batch`, () => {
        reached = true
        return HttpResponse.json({ series: {} })
      }),
    )
    const { error } = await client.GET('/api/admin/metric-history/batch', {
      params: { query: { since: '1h' } },
    })
    expect(reached).toBe(true)
    expect(error).toBeUndefined()
  })

  it('does NOT abort GET /api/admin/bot-sources', async () => {
    let reached = false
    server.use(
      http.get(`${API_BASE}/api/admin/bot-sources`, () => {
        reached = true
        return HttpResponse.json({ sources: [], rdns: { total: 0, pending: 0, last_enrichment_at: null } })
      }),
    )
    const { error } = await client.GET('/api/admin/bot-sources')
    expect(reached).toBe(true)
    expect(error).toBeUndefined()
  })
})

function fireDashboard() {
  return renderHook(
    () =>
      useMutation({
        mutationFn: async () => {
          const { data } = await client.POST('/api/dashboard/aggregates', {
            body: { start_time: 't', end_time: 't', filters: {}, chart_interval: '1h', chart_metric: 'requests' },
          })
          return data
        },
      }),
    { wrapper: wrapper() },
  )
}

describe('X-User-Active request injection', () => {
  it('sends X-User-Active: 1 when the user is active', async () => {
    vi.mocked(isUserActive).mockReturnValue(true)
    let observed: string | null = null
    server.use(
      http.post(`${API_BASE}/api/dashboard/aggregates`, ({ request }) => {
        observed = request.headers.get('X-User-Active')
        return HttpResponse.json({ ok: true })
      }),
    )
    const { result } = fireDashboard()
    result.current.mutate()
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(observed).toBe('1')
  })

  it('sends X-User-Active: 0 when the user is idle (suppresses idle-clock reset)', async () => {
    vi.mocked(isUserActive).mockReturnValue(false)
    let observed: string | null = null
    server.use(
      http.post(`${API_BASE}/api/dashboard/aggregates`, ({ request }) => {
        observed = request.headers.get('X-User-Active')
        return HttpResponse.json({ ok: true })
      }),
    )
    const { result } = fireDashboard()
    result.current.mutate()
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(observed).toBe('0')
  })
})

const LINT_BODY: VclLintRequest = { vcl_log_expression: 'req.url', collection_stage: 'edge' }

const FIELD_BODY: Omit<CustomField, 'created_at' | 'updated_at'> = {
  name: 'new_field',
  label: 'New field',
  description: '',
  vcl_log_expression: 'req.url',
  collection_stage: 'edge',
  origin_log_frequency: 'all',
  duckdb_type: 'VARCHAR',
  value_type: 'string',
  bytes_estimate: 32,
  nullable: true,
  enabled: true,
  show_in_dashboard: false,
  show_in_logs: true,
  filterable: true,
}

describe('customFieldsApi.listCustomFields', () => {
  it('returns the response body on 200', async () => {
    const fields = [
      { name: 'cookie_session_id', type: 'string', vcl: 'req.http.X-Sess' },
    ]
    server.use(
      http.get(`${API_BASE}/api/services/${SVC}/custom-fields`, () =>
        HttpResponse.json(fields),
      ),
    )
    const result = await customFieldsApi.listCustomFields(SVC)
    expect(result).toEqual(fields)
  })

  it('throws with the API error message on 500', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/${SVC}/custom-fields`, () =>
        HttpResponse.json({ detail: 'database is sad' }, { status: 500 }),
      ),
    )
    await expect(customFieldsApi.listCustomFields(SVC)).rejects.toThrow('database is sad')
  })

  it('still throws an Error when the body is empty (no silent success)', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/${SVC}/custom-fields`, () =>
        HttpResponse.json({}, { status: 500 }),
      ),
    )
    await expect(customFieldsApi.listCustomFields(SVC)).rejects.toThrow()
  })
})

describe('customFieldsApi.createCustomField', () => {
  it('returns the created field body on 200', async () => {
    const created = FIELD_BODY
    server.use(
      http.post(`${API_BASE}/api/services/${SVC}/custom-fields`, () =>
        HttpResponse.json(created),
      ),
    )
    const result = await customFieldsApi.createCustomField(SVC, created)
    expect(result).toEqual(created)
  })

  it('throws on 422 with the detail.errors[] joined into the message', async () => {
    server.use(
      http.post(`${API_BASE}/api/services/${SVC}/custom-fields`, () =>
        HttpResponse.json(
          { detail: { errors: ['LOG_FORMAT_TOO_LONG: too big', 'BAD_NAME: nope'] } },
          { status: 422 },
        ),
      ),
    )
    await expect(
      customFieldsApi.createCustomField(SVC, { ...FIELD_BODY, name: 'x' }),
    ).rejects.toThrow(/LOG_FORMAT_TOO_LONG/)
  })

  it('still throws an Error when the API returns 500 with an empty body', async () => {
    server.use(
      http.post(`${API_BASE}/api/services/${SVC}/custom-fields`, () =>
        HttpResponse.json({}, { status: 500 }),
      ),
    )
    await expect(
      customFieldsApi.createCustomField(SVC, { ...FIELD_BODY, name: 'x' }),
    ).rejects.toThrow()
  })
})

describe('customFieldsApi.updateCustomField', () => {
  it('returns the updated field on 200', async () => {
    const updated = { name: 'cookie_id', vcl_log_expression: 'req.http.X-Cookie' }
    server.use(
      http.patch(`${API_BASE}/api/services/${SVC}/custom-fields/cookie_id`, () =>
        HttpResponse.json(updated),
      ),
    )
    const result = await customFieldsApi.updateCustomField(SVC, 'cookie_id', {
      vcl_log_expression: 'req.http.X-Cookie',
    })
    expect(result).toEqual(updated)
  })

  it('throws on 404 with the API detail', async () => {
    server.use(
      http.patch(`${API_BASE}/api/services/${SVC}/custom-fields/missing`, () =>
        HttpResponse.json({ detail: 'field not found' }, { status: 404 }),
      ),
    )
    await expect(
      customFieldsApi.updateCustomField(SVC, 'missing', { vcl_log_expression: 'req.url' }),
    ).rejects.toThrow('field not found')
  })
})

describe('customFieldsApi.deleteCustomField', () => {
  it('resolves on 200', async () => {
    server.use(
      http.delete(`${API_BASE}/api/services/${SVC}/custom-fields/cookie_id`, () =>
        HttpResponse.json({ deleted: true }),
      ),
    )
    const result = await customFieldsApi.deleteCustomField(SVC, 'cookie_id')
    expect(result).toEqual({ deleted: true })
  })

  it('throws on 500', async () => {
    server.use(
      http.delete(`${API_BASE}/api/services/${SVC}/custom-fields/cookie_id`, () =>
        HttpResponse.json({ detail: 'cannot delete' }, { status: 500 }),
      ),
    )
    await expect(customFieldsApi.deleteCustomField(SVC, 'cookie_id')).rejects.toThrow(
      'cannot delete',
    )
  })
})

describe('customFieldsApi.validateCustomVcl', () => {
  it('returns the lint result on 200', async () => {
    const lint = { ok: true, warnings: [], errors: [] }
    server.use(
      http.post(
        `${API_BASE}/api/services/${SVC}/custom-fields/validate-vcl`,
        () => HttpResponse.json(lint),
      ),
    )
    const result = await customFieldsApi.validateCustomVcl(SVC, LINT_BODY)
    expect(result).toEqual(lint)
  })

  it('throws on 400 with the lint error detail', async () => {
    server.use(
      http.post(
        `${API_BASE}/api/services/${SVC}/custom-fields/validate-vcl`,
        () =>
          HttpResponse.json({ detail: 'malformed VCL' }, { status: 400 }),
      ),
    )
    await expect(
      customFieldsApi.validateCustomVcl(SVC, { ...LINT_BODY, vcl_log_expression: 'BAD' }),
    ).rejects.toThrow('malformed VCL')
  })
})

describe('customFieldsApi.exportCustomFields', () => {
  it('returns a Blob-like body on 200 (CSV body, raw fetch path)', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/${SVC}/custom-fields/export`, () =>
        HttpResponse.text('name,type,vcl\ncookie,string,req.url\n', {
          headers: { 'content-type': 'text/csv' },
        }),
      ),
    )
    const blob = await customFieldsApi.exportCustomFields(SVC)
    expect(typeof blob.size).toBe('number')
    expect(blob.size).toBeGreaterThan(0)
    expect(typeof blob.text).toBe('function')
    expect(await blob.text()).toContain('name,type,vcl')
  })

  it('throws a generic message on non-OK response', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/${SVC}/custom-fields/export`, () =>
        HttpResponse.text('', { status: 500 }),
      ),
    )
    await expect(customFieldsApi.exportCustomFields(SVC)).rejects.toThrow(
      /Failed to export/,
    )
  })
})

describe('customFieldsApi.importCustomFields', () => {
  it('returns the import result on 200', async () => {
    const result = { imported: 3, skipped: 1 }
    server.use(
      http.post(`${API_BASE}/api/services/${SVC}/custom-fields/import`, () =>
        HttpResponse.json(result),
      ),
    )
    const got = await customFieldsApi.importCustomFields(SVC, [
      { name: 'a', vcl_log_expression: 'req.url' },
    ])
    expect(got).toEqual(result)
  })

  it('throws on 422 with the joined error detail', async () => {
    server.use(
      http.post(`${API_BASE}/api/services/${SVC}/custom-fields/import`, () =>
        HttpResponse.json(
          { detail: { errors: ['DUPLICATE_FIELD: cookie'] } },
          { status: 422 },
        ),
      ),
    )
    await expect(
      customFieldsApi.importCustomFields(SVC, [
        { name: 'cookie', vcl_log_expression: 'req.url' },
      ]),
    ).rejects.toThrow(/DUPLICATE_FIELD/)
  })
})

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
  beforeEach(() => {
    apiBase.value = 'http://api.test'
  })

  afterEach(() => {
    apiBase.value = API_BASE
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
