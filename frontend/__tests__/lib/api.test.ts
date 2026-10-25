/**
 * @vitest-environment jsdom
 *
 * Consolidated MSW-driven contract tests for the API client (`lib/api.ts`)
 * and `customFieldsApi` (`lib/api/custom-fields.ts`).
 */
import { renderHook, waitFor } from '@testing-library/react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/page-smoke'
import { http, HttpResponse } from 'msw'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

import { server } from '@/__tests__/helpers/msw/handlers'
import { invalidInput } from '@/__tests__/helpers/page-smoke'
import type { components } from '@/types/api.generated'

const API_BASE = 'http://127.0.0.1:8000'
const SVC = 'svc-test'

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

import { client, extractApiError, customFieldsApi, type CustomField, type VclLintRequest } from '@/lib/api'
import { isUserActive } from '@/lib/utils'

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
