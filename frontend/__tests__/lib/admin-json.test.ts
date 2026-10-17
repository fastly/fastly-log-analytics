/**
 * @vitest-environment jsdom
 */
import { http, HttpResponse } from 'msw'
import { describe, it, expect } from 'vitest'

import { adminJson, adminJsonResult } from '@/lib/api'
import { arrayOrEmpty, recordOrNull } from '@/lib/narrow'
import type { components } from '@/types/api'
import { server } from '../../tests/msw/server'

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
    expect(recordOrNull([] as unknown as object)).toBeNull()
    expect(recordOrNull('x' as unknown as object)).toBeNull()
  })

  it('arrayOrEmpty returns a copy of arrays and [] otherwise', () => {
    expect(arrayOrEmpty([1, 2])).toEqual([1, 2])
    expect(arrayOrEmpty(null)).toEqual([])
    expect(arrayOrEmpty({} as unknown as number[])).toEqual([])
  })
})
