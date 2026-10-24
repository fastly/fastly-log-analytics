/**
 * @vitest-environment jsdom
 *
 * Consolidated StoreHydrator and ReloadLoopGuard tests.
 */
import { stubSystemTimeZone } from '@/__tests__/helpers/browser-stubs'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, test, vi } from 'vitest'
import React from 'react'

import { StoreHydrator } from '@/components/StoreHydrator'
import { ReloadLoopGuard } from '@/components/ReloadLoopGuard'
import { useServiceStore } from '@/stores/serviceStore'
import { useTimezoneStore } from '@/stores/timezoneStore'
import { useDebugStore } from '@/stores/debugStore'

beforeEach(() => {
  window.localStorage.clear()
  sessionStorage.clear()
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
  vi.unstubAllGlobals()
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
    stubSystemTimeZone('Asia/Tokyo')

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
    stubSystemTimeZone('Europe/Berlin')

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
    stubSystemTimeZone('Asia/Tokyo')

    render(<StoreHydrator />)

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
    stubSystemTimeZone('Asia/Tokyo')

    render(<StoreHydrator />)

    await waitFor(() => {
      expect(useTimezoneStore.getState().mode).toBe('manual')
    })
    expect(useTimezoneStore.getState().timezone).toBe('America/New_York')
  })
})

const CHILD = 'app-subtree-rendered'

function loadGuard() {
  return render(
    <ReloadLoopGuard>
      <div>{CHILD}</div>
    </ReloadLoopGuard>,
  )
}

describe('ReloadLoopGuard', () => {
  test('renders the app subtree under the reload threshold', () => {
    for (let i = 0; i < 5; i++) {
      const { unmount } = loadGuard()
      expect(screen.getByText(CHILD)).toBeInTheDocument()
      unmount()
    }
  })

  test('trips after 6 same-path loads in the window — shows recovery prompt, hides app', () => {
    let last: ReturnType<typeof loadGuard> | null = null
    for (let i = 0; i < 6; i++) {
      last?.unmount()
      last = loadGuard()
    }
    expect(screen.queryByText(CHILD)).not.toBeInTheDocument()
    expect(screen.getByText(/kept reloading/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /refresh now/i })).toBeInTheDocument()
  })

  test('"Continue anyway" dismisses the prompt and renders the app', () => {
    let last: ReturnType<typeof loadGuard> | null = null
    for (let i = 0; i < 6; i++) {
      last?.unmount()
      last = loadGuard()
    }
    expect(screen.getByText(/kept reloading/i)).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /continue anyway/i }))
    expect(screen.getByText(CHILD)).toBeInTheDocument()
    expect(sessionStorage.getItem('__reload_loop_history')).toBeNull()
  })

  test('"Refresh now" clears history and reloads', () => {
    const reloadSpy = vi.spyOn(window.location, 'reload').mockImplementation(() => {})
    let last: ReturnType<typeof loadGuard> | null = null
    for (let i = 0; i < 6; i++) {
      last?.unmount()
      last = loadGuard()
    }
    fireEvent.click(screen.getByRole('button', { name: /refresh now/i }))
    expect(sessionStorage.getItem('__reload_loop_history')).toBeNull()
    expect(reloadSpy).toHaveBeenCalledTimes(1)
    reloadSpy.mockRestore()
  })

  test('does not trip when loads are spread across different paths', () => {
    const spy = vi.spyOn(window.location, 'pathname', 'get')
    let last: ReturnType<typeof loadGuard> | null = null
    for (let i = 0; i < 8; i++) {
      spy.mockReturnValue(`/page-${i}`)
      last?.unmount()
      last = loadGuard()
      expect(screen.getByText(CHILD)).toBeInTheDocument()
    }
    spy.mockRestore()
  })
})
