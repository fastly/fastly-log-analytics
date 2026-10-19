import { describe, it, expect } from 'vitest'
import { denseBarTrace, lineTrace, secondaryYAxis, withSecondaryAxis } from '@/lib/chart-builders'

type Row = { t: string | null; v: number | null }

describe('denseBarTrace', () => {
  const rows: Row[] = [
    { t: '2026-10-01T00:00:00+00:00', v: 4 },
    { t: '2026-10-01T03:00:00+00:00', v: 9 },
    { t: null, v: 7 },
  ]
  const base = { rows, getTime: (r: Row) => r.t, getValue: (r: Row) => r.v, intervalSeconds: 3600, color: '#111' }

  it('zero-fills gaps and matches +00:00 rows to the Z grid by epoch ms', () => {
    const t = denseBarTrace({ ...base, name: 'n' })
    expect(t.x).toHaveLength(4)
    expect(t.y).toEqual([4, 0, 0, 9])
    expect(t).toMatchObject({ type: 'bar', name: 'n', marker: { color: '#111' } })
    expect('yaxis' in t).toBe(false)
  })

  it('uses a caller-supplied grid and passes yaxis/showlegend through', () => {
    const grid = ['2026-10-01T00:00:00.000Z', '2026-10-01T01:00:00.000Z']
    const t = denseBarTrace({ ...base, name: 'n', grid, yaxis: 'y2', showlegend: true })
    expect(t.x).toEqual(grid)
    expect(t.y).toEqual([4, 0])
    expect(t).toMatchObject({ yaxis: 'y2', showlegend: true })
  })

  it('falls back to present buckets when no grid can be built', () => {
    const t = denseBarTrace({ ...base, name: 'n', intervalSeconds: 0 })
    expect(t.x).toEqual(['2026-10-01T00:00:00+00:00', '2026-10-01T03:00:00+00:00'])
    expect(t.y).toEqual([4, 9])
  })
})

describe('lineTrace', () => {
  it('defaults to width 2 and omits optional keys', () => {
    const t = lineTrace({ name: 'l', x: ['a'], y: [1], color: '#222' })
    expect(t).toMatchObject({ type: 'scatter', mode: 'lines', line: { color: '#222', width: 2 } })
    expect('yaxis' in t).toBe(false)
    expect('connectgaps' in t).toBe(false)
  })
})

describe('secondary axis', () => {
  it('builds a right-hand overlaying axis', () => {
    expect(secondaryYAxis('Rate', { ticksuffix: '%' })).toEqual({
      title: 'Rate',
      overlaying: 'y',
      side: 'right',
      showgrid: false,
      rangemode: 'tozero',
      ticksuffix: '%',
    })
  })

  it('widens the right margin while keeping the others', () => {
    const l = withSecondaryAxis({ margin: { l: 50, r: 20 } as { r?: number } }, 'Rate')
    expect(l.margin).toEqual({ l: 50, r: 50 })
    expect(l.yaxis2.title).toBe('Rate')
  })
})
