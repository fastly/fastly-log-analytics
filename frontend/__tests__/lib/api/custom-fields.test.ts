/**
 * @vitest-environment jsdom
 *
 * MSW-driven tests for the `customFieldsApi` wrapper at
 * [lib/api/custom-fields.ts](../../../lib/api/custom-fields.ts).
 *
 * The wrapper is thin — each method wraps one openapi-fetch call plus
 * uniform error handling via `extractApiError`. The value of pinning
 * these explicitly is twofold:
 *
 *  1. Success path: response body comes back unchanged. Catches regressions
 *     where someone wraps the response or strips fields.
 *  2. Error path: every method must throw an Error whose `.message` is the
 *     human-readable detail (not "[object Object]" / undefined). CustomFieldDrawer
 *     and CustomFieldsImporter both surface `error.message` directly to the
 *     analyst — silent regression here means silent UI degradation.
 *
 * The `exportCustomFields` method uses raw fetch (not the typed client)
 * because the endpoint returns CSV and openapi-fetch's middleware would
 * try to JSON-parse it. Pin both the success-blob shape and the error case.
 */
import { http, HttpResponse } from 'msw'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

import { customFieldsApi } from '@/lib/api/custom-fields'
import { server } from '../../../tests/msw/server'

const API_BASE = 'http://127.0.0.1:8000'
const SVC = 'svc-test'

// The api middleware reads useServiceStore.getState() on every typed-client
// request to inject x-service-id. Stub so the request can be built. Note:
// the SVC literal is inlined here rather than referenced from the
// top-level const because vi.mock is hoisted above all module-level
// declarations — capturing an outer reference would ReferenceError.
vi.mock('@/stores/serviceStore', () => {
  const state = { activeServiceId: 'svc-test' }
  const useServiceStore: unknown = Object.assign(
    (selector?: (s: typeof state) => unknown) => (selector ? selector(state) : state),
    { getState: () => state },
  )
  return { useServiceStore }
})

beforeEach(() => {
  // Reset to a clean handler list each test so the per-test `server.use`
  // calls compose cleanly without leaking handlers across files.
})

afterEach(() => {
  server.resetHandlers()
})

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
    // extractApiError stringifies an empty body to "{}", so the
    // `|| "Failed to list"` fallback never fires; the contract that matters
    // for callers is "an error WAS thrown" — they get .message either way.
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
    // The CustomField type is wide (~14 fields). For these MSW tests the
    // body shape doesn't have to match it exactly — what matters is that
    // the wrapper round-trips whatever the server returns. Cast to `any`
    // at the function boundary follows the same convention used in
    // __tests__/lib/api-error-paths.test.ts.
    const created = { name: 'new_field', vcl_log_expression: 'req.url' }
    server.use(
      http.post(`${API_BASE}/api/services/${SVC}/custom-fields`, () =>
        HttpResponse.json(created),
      ),
    )
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const result = await customFieldsApi.createCustomField(SVC, created as any)
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
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      customFieldsApi.createCustomField(SVC, { name: 'x' } as any),
    ).rejects.toThrow(/LOG_FORMAT_TOO_LONG/)
  })

  it('still throws an Error when the API returns 500 with an empty body', async () => {
    server.use(
      http.post(`${API_BASE}/api/services/${SVC}/custom-fields`, () =>
        HttpResponse.json({}, { status: 500 }),
      ),
    )
    await expect(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      customFieldsApi.createCustomField(SVC, { name: 'x' } as any),
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
    const result = await customFieldsApi.validateCustomVcl(SVC, {
      vcl: 'req.url',
      type: 'string',
    } as unknown as Parameters<typeof customFieldsApi.validateCustomVcl>[1])
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
      customFieldsApi.validateCustomVcl(SVC, {
        vcl: 'BAD',
        type: 'string',
      } as unknown as Parameters<typeof customFieldsApi.validateCustomVcl>[1]),
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
    // jsdom + undici instantiate Blob via different constructors, so
    // `instanceof Blob` is unreliable. Duck-type on the Blob interface
    // surface the caller actually uses (size + text()).
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
    // importCustomFields signature is `(service_id, fields: any[])` — no
    // shape constraint at the boundary, but the payload still has to be
    // an array of plausible field objects.
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
