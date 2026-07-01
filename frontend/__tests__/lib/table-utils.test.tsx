/**
 * @vitest-environment jsdom
 */
import { render, cleanup } from '@testing-library/react'
import { describe, it, expect, vi, afterEach } from 'vitest'
import React from 'react'

vi.mock('next/navigation', () => ({
  usePathname: () => '/origin',
}))

// FilterValueCell (rendered by makeLatencyColumns) now reads useMaskIps,
// which needs a QueryClient. Stub it off so the cells render in isolation.
vi.mock('@/hooks/useMaskIps', () => ({
  useMaskIps: () => false,
}))

import { makeLatencyColumns } from '@/lib/table-utils'

afterEach(() => cleanup())

// Helper: build a synthetic tanstack-table CellContext-shaped object so we
// can invoke the column cell renderers directly without standing up a
// table instance.
function ctx(value: any, originalRow: Record<string, any>) {
  return {
    getValue: () => value,
    row: { original: originalRow },
  }
}

describe('makeLatencyColumns — labelField cell', () => {
  const cols = makeLatencyColumns('url', 'URL', 'url')
  const labelCell = cols[0].cell as (info: any) => React.ReactElement

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
  const cols = makeLatencyColumns('url', 'URL', 'url')
  const requestsCell = cols[1].cell as (info: any) => string
  const avgCell = cols[2].cell as (info: any) => string
  const p50Cell = cols[3].cell as (info: any) => string
  const p95Cell = cols[4].cell as (info: any) => string
  const p99Cell = cols[5].cell as (info: any) => string

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
