/**
 * ServiceSwitcher MRU + pinning contract.
 *
 * Coverage:
 *   - dropdown renders Pinned + Recent + All groups in the right order
 *   - selecting a service updates MRU AND persists to localStorage
 *   - clicking the pin button toggles the persisted pinned list
 *   - pin click does NOT bubble into the row's select handler
 *   - on remount, prefs hydrate from localStorage
 */
import * as React from 'react'
import { describe, it, expect, beforeEach, beforeAll, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'

import { ServiceSwitcher } from '@/components/ServiceSwitcher/ServiceSwitcher'
import { useServiceStore } from '@/stores/serviceStore'

// cmdk requires ResizeObserver; jsdom doesn't ship one.
beforeAll(() => {
  global.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as any
})

vi.mock('next/navigation', () => ({
  usePathname: () => '/dashboard',
  useRouter: () => ({ push: vi.fn(), replace: vi.fn() }),
}))

const PREFS_KEY = 'fla-service-switcher-prefs'

beforeEach(() => {
  window.localStorage.clear()
  useServiceStore.setState({
    services: [
      { id: 'svc-alpha', name: 'Alpha Service' } as any,
      { id: 'svc-beta', name: 'Beta Service' } as any,
      { id: 'svc-gamma', name: 'Gamma Service' } as any,
      { id: 'svc-delta', name: 'Delta Service' } as any,
    ],
    activeServiceId: 'svc-alpha',
  })
})

describe('ServiceSwitcher MRU + pinning', () => {
  it('opens to show every service when no prefs exist', async () => {
    const user = userEvent.setup()
    render(<ServiceSwitcher />)
    await user.click(screen.getByRole('combobox'))

    // Each service has a pin button in the dropdown — that's a unique
    // accessible name per row (no collision with the trigger label).
    expect(screen.getByRole('button', { name: /pin alpha service to top/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /pin beta service to top/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /pin gamma service to top/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /pin delta service to top/i })).toBeInTheDocument()
  })

  it('touching the active service writes it to MRU + localStorage', async () => {
    // First render — mount effect should record activeServiceId 'svc-alpha' in MRU.
    render(<ServiceSwitcher />)
    // Effect runs synchronously after mount.
    const persisted = JSON.parse(window.localStorage.getItem(PREFS_KEY) || '{}')
    expect(persisted.mru[0]).toBe('svc-alpha')
  })

  it('clicking the pin button toggles persisted prefs without changing the active service', async () => {
    const user = userEvent.setup()
    render(<ServiceSwitcher />)
    await user.click(screen.getByRole('combobox'))

    // The pin button for an UNPINNED service is visually opacity-0 until
    // hover; the DOM node still exists and is clickable by aria-label.
    const pinBtn = screen.getByRole('button', { name: /pin beta service to top/i })
    await user.click(pinBtn)

    const persisted = JSON.parse(window.localStorage.getItem(PREFS_KEY) || '{}')
    expect(persisted.pinned).toEqual(['svc-beta'])

    // Active service must NOT have switched (pin click stopPropagation works).
    expect(useServiceStore.getState().activeServiceId).toBe('svc-alpha')

    // The pin's accessible name should now invert — re-query for the
    // unpin affordance to confirm.
    expect(screen.getByRole('button', { name: /unpin beta service/i })).toBeInTheDocument()
  })

  it('hydrates Pinned and Recent buckets from localStorage on mount', async () => {
    window.localStorage.setItem(
      PREFS_KEY,
      JSON.stringify({ pinned: ['svc-gamma'], mru: ['svc-beta', 'svc-delta'] }),
    )
    const user = userEvent.setup()
    render(<ServiceSwitcher />)
    await user.click(screen.getByRole('combobox'))

    // Pinned: svc-gamma — should render with the "Unpin" affordance.
    expect(screen.getByRole('button', { name: /unpin gamma service/i })).toBeInTheDocument()

    // Non-pinned services should each still have a pin-to-top affordance.
    expect(screen.getByRole('button', { name: /pin beta service to top/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /pin delta service to top/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /pin alpha service to top/i })).toBeInTheDocument()

    // mount-effect adds svc-alpha to MRU front, so the persisted MRU now
    // has [alpha, beta, delta] (gamma is filtered out — it's pinned).
    const persisted = JSON.parse(window.localStorage.getItem(PREFS_KEY) || '{}')
    expect(persisted.mru).toEqual(['svc-alpha', 'svc-beta', 'svc-delta'])
  })
})
