/**
 * @vitest-environment jsdom
 *
 * timezoneStore — drives the timezone switcher and any client-side date
 * formatting that respects the user's preference.
 */
import { beforeEach, describe, expect, it } from 'vitest'
import { useTimezoneStore } from '@/stores/timezoneStore'

beforeEach(() => {
  useTimezoneStore.setState({ timezone: 'UTC' })
})

describe('setTimezone', () => {
  it('updates the value', () => {
    useTimezoneStore.getState().setTimezone('America/New_York')
    expect(useTimezoneStore.getState().timezone).toBe('America/New_York')
  })

  it('accepts arbitrary strings (the consumer validates)', () => {
    useTimezoneStore.getState().setTimezone('not-a-real-tz')
    expect(useTimezoneStore.getState().timezone).toBe('not-a-real-tz')
  })
})

describe('persistence', () => {
  it('writes through to localStorage with the timezone-storage key', () => {
    useTimezoneStore.getState().setTimezone('Europe/London')
    const raw = window.localStorage.getItem('timezone-storage')
    expect(raw).toBeTruthy()
    const parsed = JSON.parse(raw!)
    expect(parsed.state.timezone).toBe('Europe/London')
  })
})
