/**
 * @vitest-environment jsdom
 *
 * StoreHydrator — triggers Zustand persist rehydration post-mount so
 * SSR-rendered defaults don't diverge from localStorage values (React #418).
 *
 * Verifies:
 *   1. Stores start with SSR-safe defaults before mount
 *   2. After mount, persisted values are rehydrated from localStorage
 *   3. Timezone resolves to the live system zone on mount whenever mode is
 *      'system' (default, a persisted system blob, or a migrated pre-v1
 *      blob), and is left untouched once mode is 'manual'
 */
import { render, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import React from 'react'

import { StoreHydrator } from '@/components/StoreHydrator'
import { useServiceStore } from '@/stores/serviceStore'
import { useTimezoneStore } from '@/stores/timezoneStore'
import { useDebugStore } from '@/stores/debugStore'

function mockSystemTimezone(timeZone: string) {
  vi.spyOn(Intl, 'DateTimeFormat').mockImplementation(
    () => ({ resolvedOptions: () => ({ timeZone }) }) as unknown as Intl.DateTimeFormat,
  )
}

beforeEach(() => {
  window.localStorage.clear()
  useServiceStore.setState({
    activeServiceId: null,
    services: [],
    isInitialized: false,
  })
  useTimezoneStore.setState({ mode: 'system', timezone: 'UTC' })
  useDebugStore.setState({ enabled: false, apiCallsEnabled: false })
})

afterEach(() => {
  vi.restoreAllMocks()
})

describe('StoreHydrator', () => {
  it('renders nothing (null)', () => {
    const { container } = render(<StoreHydrator />)
    expect(container.innerHTML).toBe('')
  })

  it('rehydrates serviceStore from localStorage after mount', async () => {
    window.localStorage.setItem(
      'service-storage',
      JSON.stringify({
        state: { activeServiceId: 'svc-persisted', services: [{ id: 'svc-persisted', name: 'Persisted' }], isInitialized: true },
        version: 0,
      }),
    )

    render(<StoreHydrator />)

    await waitFor(() => {
      expect(useServiceStore.getState().activeServiceId).toBe('svc-persisted')
    })
  })

  it('rehydrates debugStore from localStorage after mount', async () => {
    window.localStorage.setItem(
      'fastly-debug-settings',
      JSON.stringify({
        state: { enabled: true, apiCallsEnabled: true },
        version: 0,
      }),
    )

    render(<StoreHydrator />)

    await waitFor(() => {
      expect(useDebugStore.getState().enabled).toBe(true)
      expect(useDebugStore.getState().apiCallsEnabled).toBe(true)
    })
  })

  it('resolves the live system zone on mount when no zone was ever persisted (default mode: system)', async () => {
    mockSystemTimezone('Asia/Tokyo')

    render(<StoreHydrator />)

    await waitFor(() => {
      expect(useTimezoneStore.getState().timezone).toBe('Asia/Tokyo')
    })
    expect(useTimezoneStore.getState().mode).toBe('system')
  })

  it('re-resolves the live system zone on mount for a persisted system-mode blob', async () => {
    window.localStorage.setItem(
      'timezone-storage',
      JSON.stringify({ state: { timezone: 'UTC', mode: 'system' }, version: 1 }),
    )
    mockSystemTimezone('Europe/Berlin')

    render(<StoreHydrator />)

    await waitFor(() => {
      expect(useTimezoneStore.getState().timezone).toBe('Europe/Berlin')
    })
  })

  it('keeps a persisted manual-mode timezone untouched on mount, even if the system zone differs', async () => {
    window.localStorage.setItem(
      'timezone-storage',
      JSON.stringify({ state: { timezone: 'America/New_York', mode: 'manual' }, version: 1 }),
    )
    mockSystemTimezone('Asia/Tokyo')

    render(<StoreHydrator />)

    // Give the mount effect a tick to run (it rehydrates serviceStore too),
    // then assert the timezone resolver left manual mode alone.
    await waitFor(() => {
      expect(useServiceStore.getState().isInitialized).toBe(false)
    })
    expect(useTimezoneStore.getState().mode).toBe('manual')
    expect(useTimezoneStore.getState().timezone).toBe('America/New_York')
  })

  it('migrates a pre-v1 persisted blob (no mode field) to manual and keeps its zone', async () => {
    window.localStorage.setItem(
      'timezone-storage',
      JSON.stringify({ state: { timezone: 'America/New_York' }, version: 0 }),
    )
    mockSystemTimezone('Asia/Tokyo')

    render(<StoreHydrator />)

    await waitFor(() => {
      expect(useTimezoneStore.getState().mode).toBe('manual')
    })
    expect(useTimezoneStore.getState().timezone).toBe('America/New_York')
  })
})
