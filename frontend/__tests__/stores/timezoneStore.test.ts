/**
 * @vitest-environment jsdom
 *
 * timezoneStore — drives the timezone switcher and any client-side date
 * formatting that respects the user's preference.
 */
import { beforeEach, describe, expect, it } from 'vitest'
import { useTimezoneStore } from '@/stores/timezoneStore'

beforeEach(() => {
  window.localStorage.clear()
  useTimezoneStore.setState({ mode: 'system', timezone: 'UTC' })
})

describe('setTimezone', () => {
  it('updates the value and switches mode to manual', () => {
    useTimezoneStore.getState().setTimezone('America/New_York')
    expect(useTimezoneStore.getState().timezone).toBe('America/New_York')
    expect(useTimezoneStore.getState().mode).toBe('manual')
  })

  it('accepts arbitrary strings (the consumer validates)', () => {
    useTimezoneStore.getState().setTimezone('not-a-real-tz')
    expect(useTimezoneStore.getState().timezone).toBe('not-a-real-tz')
  })
})

describe('setSystemTimezone', () => {
  it('updates the value and keeps mode as system', () => {
    useTimezoneStore.getState().setSystemTimezone('Asia/Tokyo')
    expect(useTimezoneStore.getState().timezone).toBe('Asia/Tokyo')
    expect(useTimezoneStore.getState().mode).toBe('system')
  })

  it('switches mode back to system after a prior manual override', () => {
    useTimezoneStore.getState().setTimezone('Europe/London')
    expect(useTimezoneStore.getState().mode).toBe('manual')

    useTimezoneStore.getState().setSystemTimezone('Asia/Tokyo')
    expect(useTimezoneStore.getState().mode).toBe('system')
    expect(useTimezoneStore.getState().timezone).toBe('Asia/Tokyo')
  })
})

describe('persistence', () => {
  it('writes through to localStorage with the timezone-storage key', () => {
    useTimezoneStore.getState().setTimezone('Europe/London')
    const raw = window.localStorage.getItem('timezone-storage')
    expect(raw).toBeTruthy()
    const parsed = JSON.parse(raw!)
    expect(parsed.state.timezone).toBe('Europe/London')
    expect(parsed.state.mode).toBe('manual')
  })
})

describe('migrate', () => {
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
