/**
 * @vitest-environment jsdom
 *
 * Consolidated store tests: debugStore, popGeoStore, serviceStore,
 * sessionRoleStore, and timezoneStore.
 */
import { beforeEach, describe, expect, it } from 'vitest'
import { useDebugStore } from '@/stores/debugStore'
import { usePopGeoStore } from '@/stores/popGeoStore'
import { useServiceStore, type Service } from '@/stores/serviceStore'
import { useSessionRoleStore } from '@/stores/sessionRoleStore'
import { useTimezoneStore } from '@/stores/timezoneStore'

beforeEach(() => {
  window.localStorage.clear()
  useDebugStore.setState({ enabled: false, apiCallsEnabled: false })
  usePopGeoStore.setState({ map: {} })
  useServiceStore.setState({
    activeServiceId: null,
    services: [],
    isInitialized: false,
  })
  useSessionRoleStore.setState({ isRemoteAnalyst: false })
  useTimezoneStore.setState({ mode: 'system', timezone: 'UTC' })
})

describe('debugStore', () => {
  it('setEnabled flips independently of apiCallsEnabled', () => {
    useDebugStore.getState().setEnabled(true)
    expect(useDebugStore.getState().enabled).toBe(true)
    expect(useDebugStore.getState().apiCallsEnabled).toBe(false)
  })

  it('setApiCallsEnabled flips independently of enabled', () => {
    useDebugStore.getState().setApiCallsEnabled(true)
    expect(useDebugStore.getState().apiCallsEnabled).toBe(true)
    expect(useDebugStore.getState().enabled).toBe(false)
  })

  it('persists to fastly-debug-settings', () => {
    useDebugStore.getState().setEnabled(true)
    useDebugStore.getState().setApiCallsEnabled(true)
    const raw = window.localStorage.getItem('fastly-debug-settings')
    expect(raw).toBeTruthy()
    const parsed = JSON.parse(raw!)
    expect(parsed.state.enabled).toBe(true)
    expect(parsed.state.apiCallsEnabled).toBe(true)
  })
})

describe('popGeoStore', () => {
  it('defaults to empty map', () => {
    expect(usePopGeoStore.getState().map).toEqual({})
  })

  it('setMap populates the PoP lookup', () => {
    const geo = {
      SJC: { city: 'San Jose', region: 'California', country: 'US' },
      AMS: { city: 'Amsterdam', region: 'North Holland', country: 'NL' },
    }
    usePopGeoStore.getState().setMap(geo)
    expect(usePopGeoStore.getState().map).toEqual(geo)
    expect(usePopGeoStore.getState().map.SJC.city).toBe('San Jose')
  })

  it('setMap replaces the entire map', () => {
    usePopGeoStore.getState().setMap({ SJC: { city: 'San Jose', region: 'CA', country: 'US' } })
    usePopGeoStore.getState().setMap({ AMS: { city: 'Amsterdam', region: 'NH', country: 'NL' } })
    expect(usePopGeoStore.getState().map).toEqual({
      AMS: { city: 'Amsterdam', region: 'NH', country: 'NL' },
    })
    expect(usePopGeoStore.getState().map.SJC).toBeUndefined()
  })
})

describe('serviceStore', () => {
  it('sets and clears the active id', () => {
    useServiceStore.getState().setActiveServiceId('svc-1')
    expect(useServiceStore.getState().activeServiceId).toBe('svc-1')
    useServiceStore.getState().setActiveServiceId(null)
    expect(useServiceStore.getState().activeServiceId).toBeNull()
  })

  it('replaces the entire services list', () => {
    const a: Service = { id: 'a', name: 'A', accessLevel: 'read_write' }
    const b: Service = { id: 'b', name: 'B', accessLevel: 'read_only' }
    useServiceStore.getState().setServices([a, b])
    expect(useServiceStore.getState().services).toEqual([a, b])

    useServiceStore.getState().setServices([])
    expect(useServiceStore.getState().services).toEqual([])
  })

  it('flips the bootstrap flag', () => {
    expect(useServiceStore.getState().isInitialized).toBe(false)
    useServiceStore.getState().setInitialized(true)
    expect(useServiceStore.getState().isInitialized).toBe(true)
  })

  it('only persists activeServiceId and services (not isInitialized)', () => {
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

describe('sessionRoleStore', () => {
  it('defaults to admin (isRemoteAnalyst=false)', () => {
    expect(useSessionRoleStore.getState().isRemoteAnalyst).toBe(false)
  })

  it('setIsRemoteAnalyst toggles analyst role', () => {
    useSessionRoleStore.getState().setIsRemoteAnalyst(true)
    expect(useSessionRoleStore.getState().isRemoteAnalyst).toBe(true)
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

describe('timezoneStore', () => {
  it('setTimezone updates the value and switches mode to manual', () => {
    useTimezoneStore.getState().setTimezone('America/New_York')
    expect(useTimezoneStore.getState().timezone).toBe('America/New_York')
    expect(useTimezoneStore.getState().mode).toBe('manual')
  })

  it('accepts arbitrary strings (the consumer validates)', () => {
    useTimezoneStore.getState().setTimezone('not-a-real-tz')
    expect(useTimezoneStore.getState().timezone).toBe('not-a-real-tz')
  })

  it('setSystemTimezone updates the value and keeps/restores mode as system', () => {
    useTimezoneStore.getState().setTimezone('Europe/London')
    expect(useTimezoneStore.getState().mode).toBe('manual')

    useTimezoneStore.getState().setSystemTimezone('Asia/Tokyo')
    expect(useTimezoneStore.getState().mode).toBe('system')
    expect(useTimezoneStore.getState().timezone).toBe('Asia/Tokyo')
  })

  it('writes through to localStorage with the timezone-storage key', () => {
    useTimezoneStore.getState().setTimezone('Europe/London')
    const raw = window.localStorage.getItem('timezone-storage')
    expect(raw).toBeTruthy()
    const parsed = JSON.parse(raw!)
    expect(parsed.state.timezone).toBe('Europe/London')
    expect(parsed.state.mode).toBe('manual')
  })

  it('stamps mode: manual onto a pre-v1 persisted blob that only had `timezone`', () => {
    window.localStorage.setItem(
      'timezone-storage',
      JSON.stringify({ state: { timezone: 'America/Denver' }, version: 0 }),
    )
    useTimezoneStore.persist.rehydrate()
    expect(useTimezoneStore.getState().timezone).toBe('America/Denver')
    expect(useTimezoneStore.getState().mode).toBe('manual')
  })

  it('leaves an already-migrated v1 blob untouched', () => {
    window.localStorage.setItem(
      'timezone-storage',
      JSON.stringify({ state: { timezone: 'Asia/Tokyo', mode: 'system' }, version: 1 }),
    )
    useTimezoneStore.persist.rehydrate()
    expect(useTimezoneStore.getState().timezone).toBe('Asia/Tokyo')
    expect(useTimezoneStore.getState().mode).toBe('system')
  })
})
