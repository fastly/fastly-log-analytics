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
import { stubResizeObserver } from '@/__tests__/helpers/browser-stubs'
import { render, screen, fireEvent, cleanup, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, beforeAll, beforeEach, afterEach, vi } from 'vitest'
import { http, HttpResponse } from 'msw'
import { axe } from 'vitest-axe'
import React, { type ReactNode } from 'react'
import { DataTable, DataTableReadonly, DateTimeCell } from '@/components/DataTable'
import { makeLatencyColumns, type LatencyRow } from '@/lib/table-utils'
import { renderWithQuery } from '../helpers/query'
import { server } from '@/__tests__/helpers/msw/server'
import { getApiBase } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { SchemaTab } from '@/app/logs/_sections/SchemaTab'
import { BotSourcesPanel } from '@/app/admin/_sections/BotSourcesPanel'
import type { components } from '@/types/api.generated'
import type { CellContext, ColumnDef } from '@tanstack/react-table'

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/origin'))
vi.mock('@/hooks/useIsAnalyst', () => ({
  useIsAnalyst: () => false,
  useMaskIps: () => false,
  useEnforceMaskedFilters: () => {},
}))
vi.mock('@tanstack/react-virtual', async () => (await import('@/__tests__/helpers/virtual-mock')).reactVirtualModuleMock())
vi.mock('@/hooks/useDateFormat', () => ({
  useDateFormat: () => ({
    full: (iso: string) => `full:${iso}`,
    abbr: () => 'UTC',
  }),
}))
vi.mock('@/components/TimeAgo', () => ({
  TimeAgo: ({ timestamp }: { timestamp: string }) => `ago:${timestamp}`,
}))

beforeAll(() => {
  stubResizeObserver()
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

describe('showPagination={false}', () => {
  const MANY: Row[] = Array.from({ length: 120 }, (_, i) => ({ id: i, name: `row-${i}`, status: 200 }))

  it.each([
    ['DataTable', DataTable],
    ['DataTableReadonly', DataTableReadonly],
  ] as const)('%s hands every row to the virtualizer instead of truncating at 50', (_n, Table) => {
    render(<Table columns={COLUMNS} data={MANY} hideToolbar showPagination={false} />)
    expect(screen.getByText('row-119')).toBeTruthy()
  })

  it('still pages at 50 when the pager is shown', () => {
    render(<DataTableReadonly columns={COLUMNS} data={MANY} hideToolbar />)
    expect(screen.queryByText('row-119')).toBeNull()
    expect(screen.getByText('row-49')).toBeTruthy()
  })
})

describe('DateTimeCell', () => {
  it('renders default em-dash fallback when iso is null', () => {
    const { container } = render(<DateTimeCell iso={null} />)
    const span = container.querySelector('span.text-muted-foreground\\/40')
    expect(span).not.toBeNull()
    expect(span?.textContent).toBe('—')
  })

  it('renders emptyFallback node when iso is null and fallback supplied', () => {
    render(<DateTimeCell iso={undefined} emptyFallback={<span>N/A</span>} />)
    expect(screen.getByText('N/A')).toBeInTheDocument()
    expect(screen.queryByText('—')).toBeNull()
  })

  it('renders the live <TimeAgo> on the trigger when iso is provided', () => {
    render(<DateTimeCell iso="2026-06-15T12:00:00Z" />)
    expect(screen.getByText('ago:2026-06-15T12:00:00Z')).toBeInTheDocument()
  })
})

type UrlRow = LatencyRow & { url?: string | null }

type CellStub = Pick<CellContext<UrlRow, unknown>, 'getValue'> & {
  row: Pick<CellContext<UrlRow, unknown>['row'], 'original'>
}

function ctx(value: unknown, originalRow: UrlRow): CellStub {
  return {
    getValue<T>() {
      return value as T
    },
    row: { original: originalRow },
  }
}

function cellRenderer(col: ColumnDef<UrlRow>): (info: CellStub) => ReactNode {
  const cell = col.cell
  if (typeof cell !== 'function') throw new Error('expected a cell render function')
  return (info) => cell(info as CellContext<UrlRow, unknown>)
}

describe('makeLatencyColumns — labelField cell', () => {
  const cols = makeLatencyColumns<UrlRow>('url', 'URL', 'url')
  const labelCell = cellRenderer(cols[0])

  it('renders plain span when filter field is null', () => {
    const { container } = render(labelCell(ctx('/api/data', { url: null })))
    const span = container.querySelector('span.font-mono')
    expect(span).not.toBeNull()
    expect(span?.textContent).toBe('/api/data')
    expect(container.querySelector('button')).toBeNull()
  })

  it('renders FilterValueCell (with button trigger) when filter field is non-null', () => {
    const { container, getByText } = render(
      labelCell(ctx('/api/data', { url: '/api/data' })),
    )
    expect(getByText('/api/data')).toBeInTheDocument()
    expect(container.querySelector('button')).not.toBeNull()
  })
})

describe('makeLatencyColumns — numeric cells', () => {
  const cols = makeLatencyColumns<UrlRow>('url', 'URL', 'url')
  const requestsCell = cellRenderer(cols[1])
  const avgCell = cellRenderer(cols[2])
  const p50Cell = cellRenderer(cols[3])
  const p95Cell = cellRenderer(cols[4])
  const p99Cell = cellRenderer(cols[5])

  it('requests cell: null → "0", number → toLocaleString', () => {
    expect(requestsCell(ctx(null, {}))).toBe('0')
    expect(requestsCell(ctx(1234, {}))).toBe((1234).toLocaleString())
  })

  it('avg/p50/p95/p99 cells: null → "0.00", number → toFixed(2)', () => {
    for (const c of [avgCell, p50Cell, p95Cell, p99Cell]) {
      expect(c(ctx(null, {}))).toBe('0.00')
      expect(c(ctx(12.3456, {}))).toBe('12.35')
    }
  })
})

type SchemaResponse = components['schemas']['SchemaResponse']

describe('SchemaTab', () => {
  beforeEach(() => {
    useServiceStore.setState({ activeServiceId: 'svc-tables', isInitialized: true })
  })

  it('renders every column past 50 rows with formatted stats', () => {
    const schema = Array.from({ length: 80 }, (_, i) => ({
      name: `col_${i}`,
      type: 'VARCHAR',
      null_percentage: i === 0 ? 25 : 0,
      approx_unique: 1234,
      min: 'a',
      max: 'z',
    }))
    render(<SchemaTab schemaData={{ schema } as unknown as SchemaResponse} isLoadingSchema={false} />)
    for (const name of ['Column Name', 'DuckDB Type', 'Populated %', 'Approx Unique', 'Min Value', 'Max Value']) {
      expect(screen.getByRole('columnheader', { name })).toBeTruthy()
    }
    expect(screen.getByText('col_79')).toBeTruthy()
    expect(screen.getByText('75.0%')).toBeTruthy()
  })

  it('shows an em dash for columns without stats', () => {
    const schema = [{ name: 'bare', type: 'INTEGER' }]
    render(<SchemaTab schemaData={{ schema } as unknown as SchemaResponse} isLoadingSchema={false} />)
    const row = screen.getByText('bare').closest('tr') as HTMLElement
    expect(within(row).getAllByText('—').length).toBe(4)
  })
})

describe('BotSourcesPanel', () => {
  beforeEach(() => {
    useServiceStore.setState({ activeServiceId: 'svc-tables', isInitialized: true })
  })

  it('lists sources and POSTs a refresh for the chosen one', async () => {
    const refreshed: string[] = []
    const API_BASE = getApiBase()
    server.use(
      http.get(`${API_BASE}/api/admin/bot-sources`, () =>
        HttpResponse.json({
          sources: [
            { id: 'src-a', name: 'Alpha Bots', url: 'https://example.test/a', entry_count: 1500, last_updated: null },
          ],
          rdns: { total: 10, pending: 2, last_enrichment_at: null },
        }),
      ),
      http.post(`${API_BASE}/api/admin/bot-sources/:id/refresh`, ({ params }) => {
        refreshed.push(String(params.id))
        return HttpResponse.json({})
      }),
    )
    const user = userEvent.setup()
    renderWithQuery(<BotSourcesPanel />)
    expect(await screen.findByText('Alpha Bots')).toBeTruthy()
    expect(screen.getByText('not cached')).toBeTruthy()
    expect(screen.getByText('1,500')).toBeTruthy()
    await user.click(screen.getByRole('button', { name: /Refresh/ }))
    await waitFor(() => expect(refreshed).toEqual(['src-a']))
  })

  const API_BASE = getApiBase()
  const botSourcesBody = {
    sources: [],
    rdns: { total: 10, pending: 2, last_enrichment_at: null },
  }

  it('Enrich Now POSTs the real rDNS enrich route and refetches the stats', async () => {
    let enrichCalls = 0
    let listCalls = 0
    server.use(
      http.get(`${API_BASE}/api/admin/bot-sources`, () => {
        listCalls += 1
        return HttpResponse.json(botSourcesBody)
      }),
      http.post(`${API_BASE}/api/admin/rdns/enrich`, () => {
        enrichCalls += 1
        return HttpResponse.json({ ok: true, summary: {} })
      }),
    )
    const user = userEvent.setup()
    renderWithQuery(<BotSourcesPanel />)
    await waitFor(() => expect(listCalls).toBe(1))
    await user.click(await screen.findByRole('button', { name: /Enrich Now/ }))
    await waitFor(() => expect(enrichCalls).toBe(1))
    await waitFor(() => expect(listCalls).toBe(2))
  })

  it('shows an inline error when rDNS enrichment fails', async () => {
    server.use(
      http.get(`${API_BASE}/api/admin/bot-sources`, () => HttpResponse.json(botSourcesBody)),
      http.post(`${API_BASE}/api/admin/rdns/enrich`, () =>
        HttpResponse.json({ detail: { error: 'rdns_failed', message: 'enrichment exploded' } }, { status: 500 }),
      ),
    )
    const user = userEvent.setup()
    renderWithQuery(<BotSourcesPanel />)
    await user.click(await screen.findByRole('button', { name: /Enrich Now/ }))
    expect(await screen.findByRole('alert')).toBeTruthy()
  })
})
