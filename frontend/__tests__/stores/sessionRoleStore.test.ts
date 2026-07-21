/**
 * @vitest-environment jsdom
 *
 * sessionRoleStore — client-side RBAC boundary flag.
 * Controls whether a 401 redirects to /share-login (analyst) or surfaces
 * as a normal error (admin). Defaults to admin (false).
 */
import { beforeEach, describe, expect, it } from 'vitest'
import { useSessionRoleStore } from '@/stores/sessionRoleStore'

beforeEach(() => {
  useSessionRoleStore.setState({ isRemoteAnalyst: false })
})

describe('sessionRoleStore', () => {
  it('defaults to admin (isRemoteAnalyst=false)', () => {
    expect(useSessionRoleStore.getState().isRemoteAnalyst).toBe(false)
  })

  it('setIsRemoteAnalyst(true) marks session as analyst', () => {
    useSessionRoleStore.getState().setIsRemoteAnalyst(true)
    expect(useSessionRoleStore.getState().isRemoteAnalyst).toBe(true)
  })

  it('setIsRemoteAnalyst(false) reverts to admin', () => {
    useSessionRoleStore.getState().setIsRemoteAnalyst(true)
    useSessionRoleStore.getState().setIsRemoteAnalyst(false)
    expect(useSessionRoleStore.getState().isRemoteAnalyst).toBe(false)
  })

  it('is not persisted to localStorage', () => {
    useSessionRoleStore.getState().setIsRemoteAnalyst(true)
    const keys = Object.keys(window.localStorage)
    const roleKey = keys.find((k) => k.toLowerCase().includes('role') || k.toLowerCase().includes('analyst'))
    expect(roleKey).toBeUndefined()
  })
})
