/**
 * @vitest-environment jsdom
 *
 * MSW-driven contract test for the service-discovery exemption in the
 * [lib/api.ts](../../lib/api.ts) onRequest middleware.
 *
 * Regression: on a FRESH INSTALL there is no active service yet — and the
 * one screen meant to fix that (admin → Service Management) opens by calling
 * GET /api/services to list what exists. The middleware aborts serviceless
 * requests with "No active service — request aborted" unless the path is
 * exempt, so the list call was being aborted and surfacing a bogus
 * "Couldn't load services / No active service" banner. Pin two things:
 *   1. GET /api/services is NOT aborted when no service is active.
 *   2. The exemption is EXACT — service-scoped subpaths (/api/services/{id}/…)
 *      still abort, so we don't accidentally fire them without x-service-id.
 */
import { http, HttpResponse } from 'msw'
import { describe, it, expect, vi } from 'vitest'

import { server } from '../../tests/msw/server'

const API_BASE = 'http://127.0.0.1:8000'

// No active service — the fresh-install state.
vi.mock('@/stores/serviceStore', () => {
  const state = { activeServiceId: null as string | null }
  const useServiceStore: any = vi.fn((s?: (x: any) => any) => (s ? s(state) : state))
  useServiceStore.getState = () => state
  return { useServiceStore }
})
vi.mock('@/stores/adminTokenStore', () => {
  const state = { token: null as string | null }
  const useAdminTokenStore: any = vi.fn(() => state)
  useAdminTokenStore.getState = () => state
  return { useAdminTokenStore }
})
vi.mock('@/stores/debugStore', () => {
  const state = { enabled: false, apiCallsEnabled: false }
  const useDebugStore: any = vi.fn(() => state)
  useDebugStore.getState = () => state
  return { useDebugStore }
})

// Import AFTER mocks so the client middleware picks them up.
import { client } from '@/lib/api'

describe('serviceless service-discovery exemption (fresh install)', () => {
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

  // The provision wizard's "Log Fields" step (step 6) fetches the GLOBAL
  // catalog with no service param. On a fresh install there is no active
  // service, so without the exemption the request aborted with NO_SERVICE
  // and the step rendered zero groups/fields (nothing to select).
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
      client.GET('/api/services/{service_id}/lake-info' as any, {
        params: { path: { service_id: 'svc-123' } },
      } as any),
    ).rejects.toMatchObject({ code: 'NO_SERVICE' })
    expect(reached).toBe(false)
  })
})

// Global admin observability endpoints take no service dimension on the
// backend (host/process metrics). They must reach the server even with no
// active service, or the System Health card hangs forever on the NO_SERVICE
// loading state on a fresh install.
describe('serviceless global-admin observability exemption (fresh install)', () => {
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

  // Bot intelligence sources / rDNS stats are global (no service dimension on
  // the backend). On a fresh install the panel's ['bot-sources'] query was
  // aborted with NO_SERVICE, leaving "Bot Intelligence Sources" stuck on
  // "Loading…" forever (the panel renders the loading row whenever !data).
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
