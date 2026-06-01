/**
 * @vitest-environment jsdom
 *
 * serviceStore is the active-service registry — drives the service switcher,
 * the headers attached to API calls, and the access-level gating in AppLayout.
 */
import { beforeEach, describe, expect, it } from 'vitest'
import { useServiceStore, type Service } from '@/stores/serviceStore'

beforeEach(() => {
  useServiceStore.setState({
    activeServiceId: null,
    services: [],
    isInitialized: false,
  })
})

describe('setActiveServiceId', () => {
  it('sets the active id', () => {
    useServiceStore.getState().setActiveServiceId('svc-1')
    expect(useServiceStore.getState().activeServiceId).toBe('svc-1')
  })

  it('accepts null to clear the active service', () => {
    useServiceStore.setState({ activeServiceId: 'svc-1' })
    useServiceStore.getState().setActiveServiceId(null)
    expect(useServiceStore.getState().activeServiceId).toBeNull()
  })
})

describe('setServices', () => {
  it('replaces the entire services list', () => {
    const a: Service = { id: 'a', name: 'A', accessLevel: 'read_write' }
    const b: Service = { id: 'b', name: 'B', accessLevel: 'read_only' }
    useServiceStore.getState().setServices([a, b])
    expect(useServiceStore.getState().services).toEqual([a, b])

    useServiceStore.getState().setServices([])
    expect(useServiceStore.getState().services).toEqual([])
  })
})

describe('setInitialized', () => {
  it('flips the bootstrap flag', () => {
    expect(useServiceStore.getState().isInitialized).toBe(false)
    useServiceStore.getState().setInitialized(true)
    expect(useServiceStore.getState().isInitialized).toBe(true)
  })
})

describe('persistence partialize', () => {
  it('only persists activeServiceId and services (not isInitialized)', () => {
    // The persist middleware's partialize is exercised by zustand internally.
    // We sanity-check by reading the persisted shape from localStorage after a
    // mutation — isInitialized must NOT be there (it's derived per-session).
    useServiceStore.setState({
      activeServiceId: 'svc-1',
      services: [{ id: 'svc-1', name: 'S' }],
      isInitialized: true,
    })
    const raw = window.localStorage.getItem('service-storage')
    expect(raw).toBeTruthy()
    const parsed = JSON.parse(raw!)
    expect(parsed.state.activeServiceId).toBe('svc-1')
    expect(parsed.state.services).toEqual([{ id: 'svc-1', name: 'S' }])
    expect(parsed.state.isInitialized).toBeUndefined()
  })
})
