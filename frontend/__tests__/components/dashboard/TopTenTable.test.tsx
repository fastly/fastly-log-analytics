/**
 * Component-level coverage for `TopTenTable` — the workhorse card on the
 * dashboard. Pins down the empty-state branching (catalog-driven hints +
 * virtual NGWAF / User-Agent fallbacks), the NotLoggedIndicator gate,
 * row interaction, CSV copy, and the pos/neg/zero delta color branches.
 *
 * `FieldSearchDialog` and `useLogFieldsCatalog` are mocked so the tests
 * stay focused on TopTenTable's own rendering logic and don't pull in
 * the React Query / MSW machinery those modules need.
 *
 * @vitest-environment jsdom
 */
import { render, screen, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import React from 'react'

// FieldSearchDialog pulls in the filter store, debounced field-values
// hook, and a Radix Dialog. We only care that TopTenTable mounts it when
// `field` is set, so a leaf stub keeps the assertions tight.
vi.mock('@/components/Dashboard/FieldSearchDialog', () => ({
  FieldSearchDialog: ({ field, title }: { field: string; title: string }) => (
    <div data-testid="field-search-dialog" data-field={field} data-title={title} />
  ),
}))

// The catalog hook normally fetches via React Query. We swap in a
// vi.fn() the tests can re-program per case so we can drive the
// empty-state messaging branches deterministically.
const useLogFieldsCatalogMock = vi.fn()
vi.mock('@/hooks/useLogFieldsCatalog', () => ({
  useLogFieldsCatalog: () => useLogFieldsCatalogMock(),
}))

import { TopTenTable } from '@/components/Dashboard/TopTenTable'

const SAMPLE_CATALOG = {
  fields: [
    { id: 'ngwaf_action', group: 'ngwaf' },
    { id: 'cache_status', group: 'cache' },
  ],
  groups: [
    { id: 'ngwaf', label: 'NGWAF' },
    { id: 'cache', label: 'Cache' },
  ],
}

describe('TopTenTable', () => {
  beforeEach(() => {
    useLogFieldsCatalogMock.mockReset()
    useLogFieldsCatalogMock.mockReturnValue({ data: undefined })
  })

  it('renders the bare "No data available" empty state when no field is supplied', () => {
    render(<TopTenTable title="No Field" data={{ top: [], total: 0 }} />)
    expect(screen.getByText('No data available')).toBeInTheDocument()
    // No group hint should appear when there's nothing to look up.
    expect(screen.queryByText(/Requires/i)).toBeNull()
    // FieldSearchDialog is only mounted when `field` is set.
    expect(screen.queryByTestId('field-search-dialog')).toBeNull()
  })

  it('surfaces the catalog group hint when the field belongs to a disabled group', () => {
    useLogFieldsCatalogMock.mockReturnValue({ data: SAMPLE_CATALOG })
    render(
      <TopTenTable
        title="NGWAF Action"
        field="ngwaf_action"
        data={{ top: [], total: 0 }}
        inActiveFormat={false}
      />,
    )
    expect(screen.getByText('No data available')).toBeInTheDocument()
    expect(
      screen.getByText('Requires NGWAF fields to be enabled in Fastly logging.'),
    ).toBeInTheDocument()
  })

  it('shows the virtual NGWAF hint for `_ngwaf_bot_name`', () => {
    useLogFieldsCatalogMock.mockReturnValue({ data: SAMPLE_CATALOG })
    render(
      <TopTenTable
        title="NGWAF Bot"
        field="_ngwaf_bot_name"
        data={{ top: [], total: 0 }}
      />,
    )
    expect(
      screen.getByText('Requires NGWAF fields to be enabled in Fastly logging.'),
    ).toBeInTheDocument()
  })

  it('shows the User-Agent hint for `_bot_name`', () => {
    useLogFieldsCatalogMock.mockReturnValue({ data: SAMPLE_CATALOG })
    render(
      <TopTenTable
        title="Bot Name"
        field="_bot_name"
        data={{ top: [], total: 0 }}
      />,
    )
    expect(
      screen.getByText('Requires User-Agent field to be enabled in Fastly logging.'),
    ).toBeInTheDocument()
  })

  it('renders populated rows and routes clicks through onRowClick', async () => {
    const onRowClick = vi.fn()
    const user = userEvent.setup()
    const data = {
      total: 30,
      top: [
        { value: 'a', label: 'Alpha', count: 20 },
        { value: 'b', label: 'Beta', count: 10 },
      ],
    }
    render(
      <TopTenTable
        title="Populated"
        field="status"
        data={data}
        onRowClick={onRowClick}
      />,
    )
    // Both row labels render.
    expect(screen.getByText('Alpha')).toBeInTheDocument()
    expect(screen.getByText('Beta')).toBeInTheDocument()
    // A-4: each row is a real <button> with an accessible name, reachable
    // by name via getByRole — covers both pointer and keyboard users.
    const alphaRow = screen.getByRole('button', { name: /filter to alpha/i })
    expect(alphaRow.tagName).toBe('BUTTON')
    await user.click(alphaRow)
    expect(onRowClick).toHaveBeenCalledWith('status', 'a')
  })

  it('activates a row via the Enter key (keyboard reachability)', async () => {
    // A-4 regression guard: keyboard-only users must be able to add a
    // filter without a mouse. Native <button> gives us Enter/Space for
    // free, so we just need to confirm focus + Enter routes through
    // onRowClick.
    const onRowClick = vi.fn()
    const user = userEvent.setup()
    render(
      <TopTenTable
        title="Keyboard"
        field="status"
        data={{ total: 1, top: [{ value: 'a', label: 'Alpha', count: 1 }] }}
        onRowClick={onRowClick}
      />,
    )
    const row = screen.getByRole('button', { name: /filter to alpha/i })
    row.focus()
    await user.keyboard('{Enter}')
    expect(onRowClick).toHaveBeenCalledWith('status', 'a')
  })

  it('copies the table to the clipboard as CSV and toggles the copied state', () => {
    // jsdom doesn't ship a Clipboard API and `userEvent.setup()` installs
    // its own clipboard shim that would shadow this one — so we use
    // fireEvent here to keep our mock in place and assert directly on it.
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText },
      configurable: true,
    })

    const data = {
      total: 3,
      top: [
        { value: 'a', label: 'Alpha', count: 2 },
        { value: 'b', label: 'Beta', count: 1 },
      ],
    }
    render(<TopTenTable title="CSV" field="status" data={data} />)

    const copyBtn = screen.getByRole('button', { name: /copy table as csv/i })
    fireEvent.click(copyBtn)

    expect(writeText).toHaveBeenCalledTimes(1)
    expect(writeText).toHaveBeenCalledWith('status,count\n"Alpha",2\n"Beta",1')
    // After clicking the button's aria-label flips to "Copied!" until the
    // 2s timeout re-arms it.
    expect(screen.getByRole('button', { name: /copied!/i })).toBeInTheDocument()
  })

  it('escapes CSV/formula-injection payloads in attacker-controlled fields (finding 020)', () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText },
      configurable: true,
    })

    const data = {
      total: 2,
      top: [
        { value: '=1+1', count: 5 }, // formula-leading → prefixed with '
        { value: 'a"b', count: 9 }, // embedded quote → doubled
      ],
    }
    render(<TopTenTable title="CSV" field="ua" data={data} />)
    fireEvent.click(screen.getByRole('button', { name: /copy table as csv/i }))

    expect(writeText).toHaveBeenCalledTimes(1)
    // Formula-leading value gets a leading apostrophe so the spreadsheet
    // treats it as text; the embedded quote is doubled so the value can't
    // break out of its quoted column.
    expect(writeText).toHaveBeenCalledWith('ua,count\n"\'=1+1",5\n"a""b",9')
  })

  it('mounts the NotLoggedIndicator when inActiveFormat is false', () => {
    useLogFieldsCatalogMock.mockReturnValue({ data: SAMPLE_CATALOG })
    const { container } = render(
      <TopTenTable
        title="Not Logged"
        field="status"
        data={{
          total: 1,
          top: [{ value: 'a', label: 'Alpha', count: 1 }],
        }}
        inActiveFormat={false}
      />,
    )
    // The indicator's <span> wrapper carries the inline-flex class; it
    // contains the lucide EyeOff svg whose accessible signature is the
    // wrapping span's class. Use a structural query rather than tooltip
    // text (TooltipContent renders inside a Portal that isn't always
    // present until interaction).
    const eyeOffSvg = container.querySelector('svg.lucide-eye-off')
    expect(eyeOffSvg).not.toBeNull()
  })

  it('colors delta indicators red for positive, green for negative, muted for zero, with a direction-bearing aria-label and icon (A-5)', () => {
    const data = {
      total: 6,
      top: [
        { value: 'up', label: 'Up', count: 20 },
        { value: 'down', label: 'Down', count: 5 },
        { value: 'flat', label: 'Flat', count: 10 },
      ],
    }
    const compareData = {
      total: 6,
      top: [
        { value: 'up', label: 'Up', count: 10 }, // +100% → red, TrendingUp
        { value: 'down', label: 'Down', count: 10 }, // -50% → green, TrendingDown
        { value: 'flat', label: 'Flat', count: 10 }, // 0% → muted, no icon
      ],
    }
    const { container } = render(
      <TopTenTable
        title="Deltas"
        field="status"
        data={data}
        compareData={compareData}
      />,
    )
    // A-5: aria-label conveys direction without relying on color.
    const pos = screen.getByLabelText(/increased by 100%/i)
    const neg = screen.getByLabelText(/decreased by 50%/i)
    const zero = screen.getByLabelText(/no change/i)
    // Color classes still apply (sighted users + design continuity).
    expect(pos.className).toMatch(/text-red-500/)
    expect(neg.className).toMatch(/text-green-500/)
    expect(zero.className).toMatch(/text-muted-foreground/)
    // The visible percent text is still rendered (aria-hidden so screen
    // readers consume the parent aria-label instead of double-announcing).
    expect(pos.textContent).toContain('+100%')
    expect(neg.textContent).toContain('-50%')
    expect(zero.textContent).toContain('0%')
    // Direction is now also carried by an icon — TrendingUp for positive,
    // TrendingDown for negative — so color-blind users see the trend too.
    expect(container.querySelector('svg.lucide-trending-up')).not.toBeNull()
    expect(container.querySelector('svg.lucide-trending-down')).not.toBeNull()
  })
})
