import { describe, expect, it } from 'vitest'

import { tracesToTable } from '../tracesToTable'

describe('tracesToTable', () => {
  it('returns an empty shape when data is not an array', () => {
    expect(tracesToTable(null as any)).toMatchObject({ empty: true, rows: [] })
    expect(tracesToTable({} as any)).toMatchObject({ empty: true, rows: [] })
    expect(tracesToTable(undefined as any)).toMatchObject({ empty: true, rows: [] })
  })

  it('returns an empty shape when data is an empty array', () => {
    expect(tracesToTable([])).toMatchObject({ empty: true, rows: [] })
  })

  it('converts a single line/scatter trace to a 2-column table', () => {
    const data = [
      { type: 'scatter', mode: 'lines', name: 'Requests', x: ['t1', 't2', 't3'], y: [10, 20, 30] },
    ]
    const out = tracesToTable(data, 'Time series')
    expect(out.empty).toBe(false)
    expect(out.title).toBe('Time series')
    expect(out.columns).toEqual(['x', 'Requests'])
    expect(out.rows).toEqual([
      ['t1', 10],
      ['t2', 20],
      ['t3', 30],
    ])
  })

  it('unions x values across multiple traces with the same x axis', () => {
    const data = [
      { type: 'bar', name: 'A', x: ['t1', 't2', 't3'], y: [1, 2, 3] },
      { type: 'bar', name: 'B', x: ['t1', 't2', 't3'], y: [4, 5, 6] },
    ]
    const out = tracesToTable(data)
    expect(out.columns).toEqual(['x', 'A', 'B'])
    expect(out.rows).toEqual([
      ['t1', 1, 4],
      ['t2', 2, 5],
      ['t3', 3, 6],
    ])
  })

  it('fills with empty string when traces have different x ranges', () => {
    const data = [
      { type: 'scatter', name: 'A', x: ['t1', 't2'], y: [1, 2] },
      { type: 'scatter', name: 'B', x: ['t2', 't3'], y: [5, 6] },
    ]
    const out = tracesToTable(data)
    expect(out.columns).toEqual(['x', 'A', 'B'])
    expect(out.rows).toEqual([
      ['t1', 1, ''], // B has no t1 → empty cell
      ['t2', 2, 5],
      ['t3', '', 6], // A has no t3 → empty cell
    ])
  })

  it('handles a pie trace with labels + values', () => {
    const data = [
      { type: 'pie', labels: ['US', 'CN', 'DE'], values: [100, 50, 20], name: 'Country share' },
    ]
    const out = tracesToTable(data)
    expect(out.title).toBe('Country share')
    expect(out.columns).toEqual(['Label', 'Value'])
    expect(out.rows).toEqual([
      ['US', 100],
      ['CN', 50],
      ['DE', 20],
    ])
  })

  it('uses a default name "Series N" when a trace omits name', () => {
    const data = [{ type: 'bar', x: ['a', 'b'], y: [1, 2] }]
    const out = tracesToTable(data)
    expect(out.columns).toEqual(['x', 'Series 1'])
  })

  it('replaces null/undefined y values with empty string (no "undefined" rendering)', () => {
    const data = [{ type: 'scatter', name: 'A', x: ['t1', 't2', 't3'], y: [1, null as any, undefined as any] }]
    const out = tracesToTable(data)
    expect(out.rows).toEqual([
      ['t1', 1],
      ['t2', ''],
      ['t3', ''],
    ])
  })

  it('returns empty for an unsupported trace shape (e.g., heatmap)', () => {
    const data = [{ type: 'heatmap', z: [[1, 2], [3, 4]] }]
    expect(tracesToTable(data)).toMatchObject({ empty: true })
  })

  it('falls back to the provided default title', () => {
    expect(tracesToTable([], 'My chart').title).toBe('My chart')
    expect(tracesToTable([]).title).toBe('Chart data')
  })
})
