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
import * as React from 'react'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'

// Mock @/components/ui/select — the real implementation uses base-ui which
// is hard to drive from userEvent in jsdom (portals, native event
// sequencing). Mirrors the shim in
// __tests__/components/Map/NetworkMap/controls.test.tsx: every SelectItem
// renders as a <button data-select-item-value="..."/>, SelectValue's
// children render verbatim when provided, and clicking an item fires
// onValueChange.
vi.mock('@/components/ui/select', () => {
  const SelectCtx = React.createContext<((v: string) => void) | null>(null)

  const Select = ({
    value,
    onValueChange,
    children,
  }: {
    value?: string
    onValueChange?: (v: string) => void
    children?: React.ReactNode
  }) => (
    <SelectCtx.Provider value={onValueChange ?? null}>
      <div data-testid="mock-select" data-select-value={value ?? ''}>
        {children}
      </div>
    </SelectCtx.Provider>
  )

  const SelectTrigger = ({ children }: { children?: React.ReactNode }) => (
    <div data-testid="mock-select-trigger">{children}</div>
  )

  const SelectValue = ({ children }: { children?: React.ReactNode }) => (
    <span data-testid="mock-select-value">{children}</span>
  )

  const SelectContent = ({ children }: { children?: React.ReactNode }) => (
    <div data-testid="mock-select-content">{children}</div>
  )

  const SelectSeparator = () => <hr data-testid="mock-select-separator" />

  const SelectItem = ({
    value,
    children,
  }: {
    value: string
    children?: React.ReactNode
  }) => {
    const onValueChange = React.useContext(SelectCtx)
    return (
      <button
        type="button"
        data-select-item-value={value}
        onClick={() => onValueChange?.(value)}
      >
        {children}
      </button>
    )
  }

  return { Select, SelectTrigger, SelectValue, SelectContent, SelectItem, SelectSeparator }
})

import { TimezoneSwitcher } from '@/components/TimezoneSwitcher/TimezoneSwitcher'
import { useTimezoneStore } from '@/stores/timezoneStore'
import { getTimezoneAbbr } from '@/lib/date'

function mockSystemTimezone(timeZone: string) {
  // TimezoneSwitcher renders getTimezoneAbbr(), which calls date-fns-tz,
  // which calls `new Intl.DateTimeFormat(locale, options)` internally to
  // compute the abbreviation — a real constructor call with args. Only the
  // browser-zone RESOLUTION path (our own code) calls the no-arg function
  // form. Delegate anything with args to the real constructor so the
  // abbreviation lookup still works; only fake the no-arg case.
  // Arrow functions can't be used with `new` (no [[Construct]]) — date-fns-tz
  // calls `new Intl.DateTimeFormat(...)`, so the mock implementation must be
  // a real `function` to remain constructible.
  const RealDTF = Intl.DateTimeFormat
  vi.spyOn(Intl, 'DateTimeFormat').mockImplementation(function (
    this: unknown,
    ...args: unknown[]
  ) {
    if (args.length === 0) {
      return { resolvedOptions: () => ({ timeZone }) } as unknown as Intl.DateTimeFormat
    }
    return new (RealDTF as unknown as new (...a: unknown[]) => Intl.DateTimeFormat)(...args)
  } as unknown as typeof Intl.DateTimeFormat)
}

beforeEach(() => {
  window.localStorage.clear()
  useTimezoneStore.setState({ mode: 'system', timezone: 'UTC' })
  vi.restoreAllMocks()
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
    mockSystemTimezone('Australia/Sydney')
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
