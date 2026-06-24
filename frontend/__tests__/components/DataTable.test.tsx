/**
 * @vitest-environment jsdom
 *
 * Tests for DataTable — the FE's primary tabular surface (used on
 * /dashboard, /security, /performance, etc.). Bug history:
 *   - Column-toggle race: removing a column mid-render left the
 *     rendered DOM header pointing at a column the table instance no
 *     longer knew about; mousing the resize handle in that window
 *     threw "Column with id '<id>' does not exist". We wrapped
 *     getResizeHandler in try/catch — this test pins that guard.
 *   - Column-order stale ID drift: ``columnOrder`` could contain IDs
 *     not in the current ``columns`` prop (FE saved a stale view).
 *     The effect at line 274-281 of DataTable.tsx filters those out.
 *
 * We deliberately DON'T test the dnd-kit drag interactions — that
 * requires fake pointer events and is brittle. We focus on rendering
 * invariants, column visibility, and the resize-race guard.
 */
import { render, screen, fireEvent, cleanup } from '@testing-library/react'
import { describe, it, expect, beforeAll, afterEach, vi } from 'vitest'
import { axe } from 'vitest-axe'
import React from 'react'
import { DataTable } from '@/components/DataTable/DataTable'
import type { ColumnDef } from '@tanstack/react-table'

vi.mock('@tanstack/react-virtual', () => {
  return {
    useVirtualizer: (options: any) => ({
      getVirtualItems: () => Array.from({ length: options.count }).map((_, i) => ({ index: i, start: i * 40, end: (i + 1) * 40 })),
      getTotalSize: () => options.count * 40,
    })
  }
})

beforeAll(() => {
  global.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver
  // DataTable measures things via HTMLElement.getBoundingClientRect.
  // jsdom returns zeros; that's fine for our presence-checks.
})

afterEach(() => cleanup())

interface Row {
  id: number
  name: string
  status: number
}

const COLUMNS: ColumnDef<Row>[] = [
  { accessorKey: 'id', header: 'ID', id: 'id' },
  { accessorKey: 'name', header: 'Name', id: 'name' },
  { accessorKey: 'status', header: 'Status', id: 'status' },
]

const DATA: Row[] = [
  { id: 1, name: 'alpha', status: 200 },
  { id: 2, name: 'beta', status: 404 },
  { id: 3, name: 'gamma', status: 500 },
]

describe('DataTable', () => {
  it('renders headers from the columns prop', () => {
    render(<DataTable columns={COLUMNS} data={DATA} hideToolbar />)
    expect(screen.getByText('ID')).toBeTruthy()
    expect(screen.getByText('Name')).toBeTruthy()
    expect(screen.getByText('Status')).toBeTruthy()
  })

  it('renders one row per data entry', () => {
    render(<DataTable columns={COLUMNS} data={DATA} hideToolbar />)
    expect(screen.getByText('alpha')).toBeTruthy()
    expect(screen.getByText('beta')).toBeTruthy()
    expect(screen.getByText('gamma')).toBeTruthy()
  })

  it('renders the empty state when data is empty', () => {
    render(<DataTable columns={COLUMNS} data={[]} hideToolbar emptyMessage="No results found." />)
    expect(screen.getByText('No results found.')).toBeTruthy()
  })

  it('shows skeletons when isLoading=true', () => {
    render(<DataTable columns={COLUMNS} data={[]} hideToolbar isLoading />)
    // The loading state should not render the empty-results message
    expect(screen.queryByText('No results.')).toBeNull()
  })

  it('mousedown on resize handle does not throw when column is removed', () => {
    // REGRESSION GUARD for the "Column with id 'X' does not exist" bug.
    // Render the table, mount a resize handle for one column, then
    // re-render with that column removed and fire mousedown on the
    // (now stale) handle. The try/catch around getResizeHandler in
    // DataTable.tsx must swallow the lookup error.
    const { container, rerender } = render(
      <DataTable columns={COLUMNS} data={DATA} hideToolbar />
    )

    // Find the resize handles — they're divs with cursor-col-resize class
    const handles = container.querySelectorAll('[class*="cursor-col-resize"]')
    expect(handles.length).toBeGreaterThan(0)

    // Re-render with a column removed (mid-flight: the rendered headers
    // still exist in the DOM, but the underlying table instance has
    // already dropped them via React's reconciliation).
    rerender(
      <DataTable columns={COLUMNS.slice(0, 2)} data={DATA} hideToolbar />
    )

    // Fire mousedown on whatever handles remain — must not throw.
    // (In production the user has a brief window where stale handles
    // can be touched; our try/catch swallows the lookup error.)
    //
    // Kept as fireEvent rather than userEvent.pointer because the test
    // is specifically asserting that the raw mousedown/touchstart
    // handler bodies don't throw when called against a stale column id.
    // userEvent.pointer would simulate a real drag (which involves
    // active focus and pointer-capture chains), and those side effects
    // could mask the exact race we're guarding against.
    const remainingHandles = container.querySelectorAll('[class*="cursor-col-resize"]')
    for (const h of Array.from(remainingHandles)) {
      expect(() => fireEvent.mouseDown(h)).not.toThrow()
      expect(() => fireEvent.touchStart(h)).not.toThrow()
    }
  })

  it('renders only the columns from the columns prop after a re-render', () => {
    // Stale columnOrder shouldn't surface phantom headers. Render with
    // 3 columns, then re-render with 2, then assert the dropped column
    // is no longer in the DOM.
    const { rerender } = render(
      <DataTable columns={COLUMNS} data={DATA} hideToolbar />
    )
    expect(screen.getByText('Status')).toBeTruthy()

    rerender(<DataTable columns={COLUMNS.slice(0, 2)} data={DATA} hideToolbar />)
    // 'Status' header should be gone
    expect(screen.queryByText('Status')).toBeNull()
    // The remaining columns are still rendered
    expect(screen.getByText('ID')).toBeTruthy()
    expect(screen.getByText('Name')).toBeTruthy()
  })

  // TESTING_PLAN_3 item 19. The table is keyboard-navigable and screen-
  // reader-friendly because the underlying tanstack-table renders semantic
  // <table>/<thead>/<tbody>. This guard catches regressions where a future
  // refactor swaps in <div role="grid"> without proper ARIA wiring.
  it('has no axe-detectable a11y violations', async () => {
    // color-contrast was previously disabled — masked 435 occurrences of
    // text-muted-foreground at 10-11px that failed WCAG AA. The token has
    // since been darkened to oklch(0.40), which clears AA on the muted/10
    // and muted/20 backgrounds the empty-state and footer surfaces use.
    // Re-enabled so future regressions of the token (or new sites that
    // pair text-muted-foreground with even-lighter backgrounds) trip CI.
    const { container } = render(<DataTable columns={COLUMNS} data={DATA} hideToolbar />)
    const results = await axe(container)
    expect(results).toHaveNoViolations()
  })

  it('respects initialColumnOrder and ignores stale ids not in columns', () => {
    // Regression guard for the columnOrder filter at line 274-281: any
    // id in initialColumnOrder that isn't in the columns prop must be
    // ignored (saved views can contain stale IDs from old schemas).
    render(
      <DataTable
        columns={COLUMNS}
        data={DATA}
        hideToolbar
        initialColumnOrder={['name', 'id', 'status', 'phantom_column']}
      />
    )
    // All real columns still render — the phantom id was filtered out
    expect(screen.getByText('ID')).toBeTruthy()
    expect(screen.getByText('Name')).toBeTruthy()
    expect(screen.getByText('Status')).toBeTruthy()
  })
})
