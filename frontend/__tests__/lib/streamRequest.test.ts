import { afterEach, describe, expect, it, vi } from 'vitest'

const apiBase = vi.hoisted(() => ({ value: 'http://api.test' }))
vi.mock('@/lib/api', () => ({ getApiBase: () => apiBase.value }))

import { useAdminTokenStore } from '@/stores/adminTokenStore'
import { buildAdminHeaders } from '@/lib/adminHeaders'
import { buildStreamRequest } from '@/lib/streamRequest'

describe('buildAdminHeaders', () => {
  afterEach(() => {
    useAdminTokenStore.setState({ token: null })
  })

  it('sets x-service-id and X-Admin-Token only when present', () => {
    expect(buildAdminHeaders({ serviceId: 'svc', adminToken: 'tok' })).toEqual({
      'x-service-id': 'svc',
      'X-Admin-Token': 'tok',
    })
    expect(buildAdminHeaders({ serviceId: null, adminToken: '' })).toEqual({})
    expect(buildAdminHeaders({ adminToken: null })).toEqual({})
  })

  it('reads the admin-token store when no token is passed, and sends nothing when empty', () => {
    expect(buildAdminHeaders({ serviceId: 'svc' })).toEqual({ 'x-service-id': 'svc' })
    useAdminTokenStore.setState({ token: 'store-tok' })
    expect(buildAdminHeaders({ serviceId: 'svc' })).toEqual({
      'x-service-id': 'svc',
      'X-Admin-Token': 'store-tok',
    })
    expect(buildAdminHeaders()).toEqual({ 'X-Admin-Token': 'store-tok' })
  })

  it('an explicit null overrides a populated store (no token leak to analyst paths)', () => {
    useAdminTokenStore.setState({ token: 'store-tok' })
    expect(buildAdminHeaders({ serviceId: 'svc', adminToken: null })).toEqual({ 'x-service-id': 'svc' })
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

  it('adds service and admin-token headers only when present', () => {
    const req = buildStreamRequest({ path: '/p', serviceId: 'svc', adminToken: 'tok' })
    expect(req.headers).toEqual({
      Accept: 'text/event-stream',
      'x-service-id': 'svc',
      'X-Admin-Token': 'tok',
    })
    expect(buildStreamRequest({ path: '/p', serviceId: null, adminToken: '' }).headers).toEqual({
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
