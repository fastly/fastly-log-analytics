/**
 * @vitest-environment jsdom
 */
import { http, HttpResponse } from 'msw'
import { describe, it, expect } from 'vitest'

import { adminJson } from '@/lib/api'
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
