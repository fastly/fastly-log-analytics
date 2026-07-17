/**
 * @vitest-environment jsdom
 *
 * adminTokenStore — session-scoped shared-secret token for admin calls.
 * The api-client middleware injects it as X-Admin-Token on every outbound
 * request. Must NOT be persisted to localStorage.
 */
import { beforeEach, describe, expect, it } from 'vitest'
import { useAdminTokenStore } from '@/stores/adminTokenStore'

beforeEach(() => {
  useAdminTokenStore.setState({ token: null })
})

describe('adminTokenStore', () => {
  it('defaults to null token', () => {
    expect(useAdminTokenStore.getState().token).toBeNull()
  })

  it('setToken stores the value', () => {
    useAdminTokenStore.getState().setToken('secret-abc')
    expect(useAdminTokenStore.getState().token).toBe('secret-abc')
  })

  it('setToken(null) clears the token', () => {
    useAdminTokenStore.getState().setToken('secret-abc')
    useAdminTokenStore.getState().setToken(null)
    expect(useAdminTokenStore.getState().token).toBeNull()
  })

  it('is not persisted to localStorage', () => {
    useAdminTokenStore.getState().setToken('secret-abc')
    const keys = Object.keys(window.localStorage)
    const tokenKey = keys.find((k) => k.toLowerCase().includes('admin') || k.toLowerCase().includes('token'))
    expect(tokenKey).toBeUndefined()
  })
})
