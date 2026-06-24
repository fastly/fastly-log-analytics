/**
 * R-3a C-4. ChartA11yTable is the sr-only companion to PlotlyChart. The
 * markup contract (caption text, scope='col' headers, fallback for empty
 * shapes) is what assistive tech actually reads, so pin it here.
 *
 * @vitest-environment jsdom
 */
import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { ChartA11yTable } from '@/components/PlotlyChart/ChartA11yTable'
import { tracesToTable } from '@/components/PlotlyChart/tracesToTable'

describe('ChartA11yTable', () => {
  it('renders an sr-only table from a tracesToTable-shaped input', () => {
    const traces = [
      { x: ['a', 'b'], y: [1, 2], name: 'Series 1' },
      { x: ['a', 'b'], y: [3, 4], name: 'Series 2' },
    ]
    const shape = tracesToTable(traces, 'Requests over time')
    const { container } = render(<ChartA11yTable shape={shape} />)
    const table = container.querySelector('table')
    expect(table).not.toBeNull()
    expect(table?.className).toContain('sr-only')
    // Header cells include the trace names + x column.
    expect(screen.getByRole('columnheader', { name: 'x' })).toBeInTheDocument()
    expect(screen.getByRole('columnheader', { name: 'Series 1' })).toBeInTheDocument()
    expect(screen.getByRole('columnheader', { name: 'Series 2' })).toBeInTheDocument()
    // Rows = data points; check a known cell exists.
    expect(screen.getAllByRole('cell').length).toBeGreaterThan(0)
  })

  it('emits a no-data fallback (not a table) for an empty shape', () => {
    const shape = tracesToTable([], 'Empty chart')
    const { container } = render(<ChartA11yTable shape={shape} />)
    expect(container.querySelector('table')).toBeNull()
    expect(container.textContent).toContain('Empty chart')
    expect(container.textContent).toContain('no readable data')
  })

  it('uses the title from the shape as the caption', () => {
    const traces = [{ x: [1], y: [1], name: 'A' }]
    const shape = tracesToTable(traces, 'My descriptive title')
    render(<ChartA11yTable shape={shape} />)
    const caption = document.querySelector('caption')
    expect(caption?.textContent).toBe('My descriptive title')
  })

  it('sets scope="col" on every header cell', () => {
    const shape = tracesToTable([{ x: [1, 2], y: [3, 4], name: 'S' }])
    const { container } = render(<ChartA11yTable shape={shape} />)
    const ths = container.querySelectorAll('thead th')
    expect(ths.length).toBeGreaterThan(0)
    ths.forEach((th) => expect(th.getAttribute('scope')).toBe('col'))
  })

  it('skips heatmap/z-matrix traces instead of zipping x against y', () => {
    // x and y are independent axes; the prior xy path zipped them into one
    // bogus row per x point (the /network heatmap yielded ~4k hidden rows).
    const heatmap = [
      {
        type: 'heatmap',
        x: ['t1', 't2', 't3', 't4'],
        y: ['asnA', 'asnB', 'asnC'],
        z: [
          [1, 2, 3, 4],
          [5, 6, 7, 8],
          [9, 10, 11, 12],
        ],
      },
    ]
    const shape = tracesToTable(heatmap, 'ASN heatmap')
    expect(shape.empty).toBe(true)
    expect(shape.rows).toHaveLength(0)
    const { container } = render(<ChartA11yTable shape={shape} />)
    expect(container.querySelector('table')).toBeNull()
  })

  it('caps very large flat tables and announces the truncation', () => {
    const x = Array.from({ length: 1200 }, (_, i) => i)
    const y = x.map((n) => n * 2)
    const shape = tracesToTable([{ x, y, name: 'S' }], 'Big chart')
    expect(shape.rows).toHaveLength(500)
    expect(shape.truncatedFrom).toBe(1200)
    render(<ChartA11yTable shape={shape} />)
    expect(document.querySelector('caption')?.textContent).toContain(
      'showing first 500 of 1200 rows',
    )
  })
})
