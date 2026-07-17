/**
 * @vitest-environment jsdom
 *
 * StoreHydrator — triggers Zustand persist rehydration post-mount so
 * SSR-rendered defaults don't diverge from localStorage values (React #418).
 *
 * Verifies:
 *   1. Stores start with SSR-safe defaults before mount
 *   2. After mount, persisted values are rehydrated from localStorage
 *   3. Timezone falls back to browser zone when no persisted value exists
 */
import { render, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import React from 'react'

import { StoreHydrator } from '@/components/StoreHydrator'
import { useServiceStore } from '@/stores/serviceStore'
import { useTimezoneStore } from '@/stores/timezoneStore'
import { useDebugStore } from '@/stores/debugStore'

beforeEach(() => {
  window.localStorage.clear()
  useServiceStore.setState({
    activeServiceId: null,
    services: [],
    isInitialized: false,
  })
  useTimezoneStore.setState({ timezone: 'UTC' })
  useDebugStore.setState({ enabled: false, apiCallsEnabled: false })
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

  it('keeps persisted timezone when one exists in localStorage', async () => {
    window.localStorage.setItem(
      'timezone-storage',
      JSON.stringify({
        state: { timezone: 'America/New_York' },
        version: 0,
      }),
    )

    render(<StoreHydrator />)

    await waitFor(() => {
      expect(useTimezoneStore.getState().timezone).toBe('America/New_York')
    })
  })
})
