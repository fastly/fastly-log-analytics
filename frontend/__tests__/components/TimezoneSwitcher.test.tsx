/**
 * @vitest-environment jsdom
 *
 * TimezoneSwitcher — "System Time" default + manual override contract.
 *
 * Coverage:
 *   - "System Time" renders as the first item, above a separator, ahead of
 *     the fixed common-zone list
 *   - trigger shows "System Time (<abbr>)" while mode is system (short
 *     abbreviation, not the full IANA string — keeps the fixed-width
 *     trigger from overflowing)
 *   - picking a concrete zone flips the store to manual mode
 *   - picking "System Time" resolves the live browser zone immediately
 *   - the "not in the common list" fallback item only appears in manual mode
 */
import { stubSystemTimeZone } from '@/__tests__/helpers/browser-stubs'
import { selectMockFactory } from '@/__tests__/helpers/ui-mocks'
import * as React from 'react'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'

// Mock @/components/ui/select — the real implementation uses base-ui which
// is hard to drive from userEvent in jsdom (portals, native event
// sequencing).
vi.mock('@/components/ui/select', () => selectMockFactory())

import { TimezoneSwitcher } from '@/components/TimezoneSwitcher/TimezoneSwitcher'
import { useTimezoneStore } from '@/stores/timezoneStore'
import { getTimezoneAbbr } from '@/lib/date'

beforeEach(() => {
  window.localStorage.clear()
  useTimezoneStore.setState({ mode: 'system', timezone: 'UTC' })
  vi.restoreAllMocks()
  vi.unstubAllGlobals()
})

describe('TimezoneSwitcher', () => {
  it('renders "System Time" first, then a separator, then the common zones in order', async () => {
    render(<TimezoneSwitcher />)

    const content = await waitFor(() => screen.getByTestId('mock-select-content'))
    const entries = Array.from(
      content.querySelectorAll('[data-select-item-value], [data-testid="mock-select-separator"]'),
    ).map((el) => el.getAttribute('data-select-item-value') ?? 'SEPARATOR')

    expect(entries[0]).toBe('__system__')
    expect(entries[1]).toBe('SEPARATOR')
    expect(entries.slice(2)).toEqual([
      'UTC',
      'America/New_York',
      'America/Chicago',
      'America/Denver',
      'America/Los_Angeles',
      'Europe/London',
      'Europe/Berlin',
      'Asia/Tokyo',
      'Australia/Sydney',
    ])
  })

  it('shows "System Time (<abbr>)" in the trigger while mode is system', async () => {
    useTimezoneStore.setState({ mode: 'system', timezone: 'America/Chicago' })
    render(<TimezoneSwitcher />)

    // Abbreviation (e.g. CST/CDT) depends on the real current date's DST
    // status, not a fixed string — compute it the same way the component
    // does rather than hardcoding one.
    const abbr = getTimezoneAbbr(new Date(), 'America/Chicago')
    await waitFor(() => {
      expect(screen.getByTestId('mock-select-value')).toHaveTextContent(
        `System Time (${abbr})`,
      )
    })
  })

  it('picking a concrete zone flips the store to manual mode', async () => {
    const user = userEvent.setup()
    render(<TimezoneSwitcher />)

    const tokyoItem = await waitFor(() =>
      screen.getAllByRole('button').find((b) => b.getAttribute('data-select-item-value') === 'Asia/Tokyo'),
    )
    expect(tokyoItem).toBeTruthy()

    await user.click(tokyoItem!)

    expect(useTimezoneStore.getState().mode).toBe('manual')
    expect(useTimezoneStore.getState().timezone).toBe('Asia/Tokyo')
  })

  it('picking "System Time" resolves the live browser zone immediately', async () => {
    useTimezoneStore.setState({ mode: 'manual', timezone: 'Europe/London' })
    stubSystemTimeZone('Australia/Sydney')
    const user = userEvent.setup()
    render(<TimezoneSwitcher />)

    const systemItem = await waitFor(() =>
      screen.getAllByRole('button').find((b) => b.getAttribute('data-select-item-value') === '__system__'),
    )
    expect(systemItem).toBeTruthy()

    await user.click(systemItem!)

    expect(useTimezoneStore.getState().mode).toBe('system')
    expect(useTimezoneStore.getState().timezone).toBe('Australia/Sydney')
  })

  it('only shows the manual-mode fallback item for an uncommon zone when mode is manual', async () => {
    useTimezoneStore.setState({ mode: 'manual', timezone: 'Asia/Kolkata' })
    const { rerender } = render(<TimezoneSwitcher />)

    await waitFor(() => {
      expect(
        screen.getAllByRole('button').some((b) => b.getAttribute('data-select-item-value') === 'Asia/Kolkata'),
      ).toBe(true)
    })

    useTimezoneStore.setState({ mode: 'system', timezone: 'Asia/Kolkata' })
    rerender(<TimezoneSwitcher />)

    expect(
      screen.getAllByRole('button').some((b) => b.getAttribute('data-select-item-value') === 'Asia/Kolkata'),
    ).toBe(false)
  })
})
