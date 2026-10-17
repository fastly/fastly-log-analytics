/**
 * @vitest-environment jsdom
 */
import { render, cleanup } from '@testing-library/react'
import { describe, it, expect, vi, afterEach } from 'vitest'
import type { ReactNode } from 'react'
import type { CellContext, ColumnDef } from '@tanstack/react-table'

vi.mock('next/navigation', () => ({
  usePathname: () => '/origin',
}))

// FilterValueCell (rendered by makeLatencyColumns) now reads useMaskIps,
// which needs a QueryClient. Stub it off so the cells render in isolation.
vi.mock('@/hooks/useMaskIps', () => ({
  useMaskIps: () => false,
}))

import { makeLatencyColumns, type LatencyRow } from '@/lib/table-utils'

type UrlRow = LatencyRow & { url?: string | null }

afterEach(() => cleanup())

// Helper: build a synthetic tanstack-table CellContext-shaped object so we
// can invoke the column cell renderers directly without standing up a
// table instance.
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
    // FilterValueCell would render a button — this branch must not
    expect(container.querySelector('button')).toBeNull()
  })

  it('renders FilterValueCell (with button trigger) when filter field is non-null', () => {
    const { container, getByText } = render(
      labelCell(ctx('/api/data', { url: '/api/data' })),
    )
    expect(getByText('/api/data')).toBeInTheDocument()
    // FilterValueCell renders a dropdown trigger button
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
