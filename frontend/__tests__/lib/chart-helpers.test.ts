/**
 * R-3a C-4. Pin the contract of the two pure helpers PlotlyChart-shaped
 * callers rely on: TIME_HOVER_LAYOUT (literal shape) and makeTimeXAxis
 * (start/end/timezone -> Plotly xaxis spec).
 */
import { describe, it, expect } from 'vitest'
import { TIME_HOVER_LAYOUT, makeTimeXAxis } from '@/lib/chart-helpers'

describe('TIME_HOVER_LAYOUT', () => {
  it('exposes the hover + legend keys callers spread into Plotly layouts', () => {
    expect(TIME_HOVER_LAYOUT).toMatchObject({
      hovermode: 'x unified',
      legend: expect.objectContaining({
        orientation: 'h',
        xanchor: 'right',
        yanchor: 'bottom',
      }),
    })
  })
})

describe('makeTimeXAxis', () => {
  it('returns a Plotly xaxis spec with type=date and a 2-element range', () => {
    const axis = makeTimeXAxis('2026-01-01T00:00:00Z', '2026-01-01T01:00:00Z', 'UTC')
    expect(axis.type).toBe('date')
    expect(Array.isArray(axis.range)).toBe(true)
    expect(axis.range).toHaveLength(2)
    expect(axis.range[0]).not.toBe('')
    expect(axis.range[1]).not.toBe('')
    expect(axis.tickformatstops).toBeDefined()
  })

  it('formats range bounds in the supplied timezone', () => {
    const utc = makeTimeXAxis('2026-01-01T12:00:00Z', '2026-01-01T13:00:00Z', 'UTC')
    const ny = makeTimeXAxis('2026-01-01T12:00:00Z', '2026-01-01T13:00:00Z', 'America/New_York')
    // Same instant rendered in different zones must differ.
    expect(utc.range[0]).not.toBe(ny.range[0])
    expect(utc.range[0]).toContain('12:00:00')
    expect(ny.range[0]).toContain('07:00:00')
  })

  it('returns empty-string bounds for null/undefined start/end without throwing', () => {
    const both = makeTimeXAxis(null, null, 'UTC')
    expect(both.range).toEqual(['', ''])
    const undef = makeTimeXAxis(undefined, undefined, 'UTC')
    expect(undef.range).toEqual(['', ''])
    const mixed = makeTimeXAxis(null, '2026-01-01T00:00:00Z', 'UTC')
    expect(mixed.range[0]).toBe('')
    expect(mixed.range[1]).not.toBe('')
  })
})
