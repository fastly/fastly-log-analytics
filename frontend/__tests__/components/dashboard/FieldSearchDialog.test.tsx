/**
 * @vitest-environment jsdom
 *
 * Tests for FieldSearchDialog — the browse/search modal opened from a
 * TopTenTable card header (e.g. the Edge PoP card). Focus: the pinned
 * "Selected" section — filters already active for the dialog's field must
 * render at the top of the list, ahead of the fetched values, follow the
 * search text, and be removable in place without closing the dialog.
 *
 * `useFieldValues` is mocked (the fetch layer is exercised elsewhere); the
 * filter store is the real zustand store, reset per test.
 */
import { render, screen, cleanup, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import React from 'react'
import { FieldSearchDialog } from '@/components/Dashboard/FieldSearchDialog'
import { useFilterStore } from '@/stores/filterStore'
import { usePopGeoStore } from '@/stores/popGeoStore'

// Fixed field-values payload — counts descending, like the real endpoint.
let __fieldValues: { values: { value: string; count: number; label?: string }[] } | undefined = {
  values: [
    { value: 'jfk', count: 500 },
    { value: 'lhr', count: 300 },
    { value: 'den', count: 200 },
    { value: 'syd', count: 100 },
  ],
}
let __isLoading = false

vi.mock('@/hooks/useFieldValues', () => ({
  useFieldValues: () => ({ data: __fieldValues, isLoading: __isLoading, isFetching: false }),
}))

function seedFilters(filters: { column: string; value: string; mode?: 'include' | 'exclude' }[]) {
  useFilterStore.setState({
    filters: filters.map((f, i) => ({
      id: `pill-${i}`,
      column: f.column,
      value: f.value,
      mode: f.mode ?? 'include',
    })),
  })
}

async function openDialog(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole('button', { name: 'Search Edge PoP' }))
  return screen.findByRole('dialog')
}

describe('FieldSearchDialog', () => {
  beforeEach(() => {
    useFilterStore.setState({ filters: [] })
    usePopGeoStore.setState({ map: {} })
    __fieldValues = {
      values: [
        { value: 'jfk', count: 500 },
        { value: 'lhr', count: 300 },
        { value: 'den', count: 200 },
        { value: 'syd', count: 100 },
      ],
    }
    __isLoading = false
  })

  afterEach(() => {
    cleanup()
    vi.clearAllMocks()
  })

  it('pins currently selected values at the top, above the fetched list', async () => {
    // DEN is 3rd by traffic — with an active den filter it must render first.
    seedFilters([{ column: 'pop', value: 'den' }])
    const user = userEvent.setup()
    render(<FieldSearchDialog field="pop" title="Edge PoP" />)
    const dialog = await openDialog(user)

    expect(within(dialog).getByText('Selected')).toBeInTheDocument()

    const pinned = within(dialog).getByRole('button', { name: 'Remove include filter den' })
    const rows = within(dialog).getAllByRole('button').filter(b =>
      /DEN|JFK|LHR|SYD/.test(b.textContent ?? '')
    )
    expect(rows[0]).toBe(pinned)
    // The pinned value is deduped out of the fetched list below.
    expect(rows.filter(b => (b.textContent ?? '').includes('DEN'))).toHaveLength(1)
    // Pinned row shows the count from the fetched data.
    expect(pinned.textContent).toContain('200')
  })

  it('pins selected values even when they are not in the fetched values', async () => {
    seedFilters([{ column: 'pop', value: 'ams' }])
    const user = userEvent.setup()
    render(<FieldSearchDialog field="pop" title="Edge PoP" />)
    const dialog = await openDialog(user)

    expect(
      within(dialog).getByRole('button', { name: 'Remove include filter ams' })
    ).toBeInTheDocument()
  })

  it('only pins filters for its own field', async () => {
    seedFilters([
      { column: 'country', value: 'US' },
      { column: 'pop', value: 'lhr' },
    ])
    const user = userEvent.setup()
    render(<FieldSearchDialog field="pop" title="Edge PoP" />)
    const dialog = await openDialog(user)

    expect(within(dialog).getByRole('button', { name: 'Remove include filter lhr' })).toBeInTheDocument()
    expect(within(dialog).queryByRole('button', { name: /filter US/ })).not.toBeInTheDocument()
  })

  it('clicking a pinned value removes the filter and keeps the dialog open', async () => {
    seedFilters([
      { column: 'pop', value: 'den' },
      { column: 'pop', value: 'syd' },
    ])
    const user = userEvent.setup()
    render(<FieldSearchDialog field="pop" title="Edge PoP" />)
    const dialog = await openDialog(user)

    await user.click(within(dialog).getByRole('button', { name: 'Remove include filter den' }))

    expect(useFilterStore.getState().filters.map(f => f.value)).toEqual(['syd'])
    expect(screen.getByRole('dialog')).toBeInTheDocument()
    // The removed value reappears in the unpinned fetched list.
    expect(within(dialog).getByRole('button', { name: /Remove include filter syd/ })).toBeInTheDocument()
  })

  it('marks exclude filters distinctly in the pinned section', async () => {
    seedFilters([{ column: 'pop', value: 'lhr', mode: 'exclude' }])
    const user = userEvent.setup()
    render(<FieldSearchDialog field="pop" title="Edge PoP" />)
    const dialog = await openDialog(user)

    expect(
      within(dialog).getByRole('button', { name: 'Remove exclude filter lhr' })
    ).toBeInTheDocument()
  })

  it('narrows pinned values by the search text', async () => {
    seedFilters([
      { column: 'pop', value: 'den' },
      { column: 'pop', value: 'syd' },
    ])
    const user = userEvent.setup()
    render(<FieldSearchDialog field="pop" title="Edge PoP" />)
    const dialog = await openDialog(user)

    await user.type(within(dialog).getByPlaceholderText('Search for a Edge PoP...'), 'sy')

    expect(within(dialog).getByRole('button', { name: 'Remove include filter syd' })).toBeInTheDocument()
    expect(
      within(dialog).queryByRole('button', { name: 'Remove include filter den' })
    ).not.toBeInTheDocument()
  })

  it('renders pinned pop values through PopLabel (code + geo)', async () => {
    usePopGeoStore.setState({ map: { DEN: { city: 'Denver', region: 'CO', country: 'us' } } })
    seedFilters([{ column: 'pop', value: 'den' }])
    const user = userEvent.setup()
    render(<FieldSearchDialog field="pop" title="Edge PoP" />)
    const dialog = await openDialog(user)

    const pinned = within(dialog).getByRole('button', { name: 'Remove include filter den' })
    expect(pinned.textContent).toContain('DEN')
    expect(pinned.textContent).toContain('Denver, CO - USA')
  })

  it('clicking an unpinned value still adds an include filter and closes', async () => {
    const user = userEvent.setup()
    render(<FieldSearchDialog field="pop" title="Edge PoP" />)
    const dialog = await openDialog(user)

    await user.click(within(dialog).getByRole('button', { name: /JFK/ }))

    const pills = useFilterStore.getState().filters
    expect(pills).toHaveLength(1)
    expect(pills[0]).toMatchObject({ column: 'pop', value: 'jfk', mode: 'include' })
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
  })
})
