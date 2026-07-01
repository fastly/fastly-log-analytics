/**
 * R-3a C-4. Pin the contract of the two pure helpers PlotlyChart-shaped
 * callers rely on: TIME_HOVER_LAYOUT (literal shape) and makeTimeXAxis
 * (start/end/timezone -> Plotly xaxis spec).
 */
import { describe, it, expect } from 'vitest'
import { TIME_HOVER_LAYOUT, makeTimeXAxis, denseTimeGrid } from '@/lib/chart-helpers'

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

describe('denseTimeGrid', () => {
  it('fills the gaps between the first and last bucket on the hourly grid', () => {
    // 09:00 and 12:00 present → a contiguous 4-bucket hourly grid.
    const grid = denseTimeGrid(
      ['2026-06-30T09:00:00Z', '2026-06-30T12:00:00Z'],
      3600,
    )
    expect(grid).not.toBeNull()
    expect(grid).toHaveLength(4)
    expect(grid!.map((iso) => Date.parse(iso))).toEqual([
      Date.parse('2026-06-30T09:00:00Z'),
      Date.parse('2026-06-30T10:00:00Z'),
      Date.parse('2026-06-30T11:00:00Z'),
      Date.parse('2026-06-30T12:00:00Z'),
    ])
  })

  it('treats a `+00:00` suffix (scoring `hour` format) the same as `Z`', () => {
    // The scoring endpoints serialize tz-aware datetimes as `...+00:00`; the
    // grid emits `...Z`. They must resolve to the same instants so a ms-keyed
    // lookup against the grid can't miss.
    const grid = denseTimeGrid(
      ['2026-06-30T09:00:00+00:00', '2026-06-30T11:00:00+00:00'],
      3600,
    )
    expect(grid).not.toBeNull()
    expect(grid!.map((iso) => Date.parse(iso))).toEqual([
      Date.parse('2026-06-30T09:00:00+00:00'),
      Date.parse('2026-06-30T10:00:00+00:00'),
      Date.parse('2026-06-30T11:00:00+00:00'),
    ])
  })

  it('fills a per-minute grid for short scoring windows', () => {
    const grid = denseTimeGrid(
      ['2026-06-30T09:00:00Z', '2026-06-30T09:03:00Z'],
      60,
    )
    expect(grid).toHaveLength(4)
  })

  it('no-ops (returns null) for unknown/zero interval', () => {
    const times = ['2026-06-30T09:00:00Z', '2026-06-30T12:00:00Z']
    expect(denseTimeGrid(times, undefined)).toBeNull()
    expect(denseTimeGrid(times, 0)).toBeNull()
  })

  it('no-ops when there are fewer than 2 distinct buckets', () => {
    expect(denseTimeGrid(['2026-06-30T09:00:00Z'], 3600)).toBeNull()
    // Duplicate timestamps collapse to one distinct bucket.
    expect(
      denseTimeGrid(['2026-06-30T09:00:00Z', '2026-06-30T09:00:00Z'], 3600),
    ).toBeNull()
  })

  it('no-ops when the series is already contiguous (no gaps)', () => {
    expect(
      denseTimeGrid(
        ['2026-06-30T09:00:00Z', '2026-06-30T10:00:00Z', '2026-06-30T11:00:00Z'],
        3600,
      ),
    ).toBeNull()
  })

  it('no-ops when buckets are off the interval grid (mixed grain)', () => {
    expect(
      denseTimeGrid(
        ['2026-06-30T09:00:00Z', '2026-06-30T09:30:00Z', '2026-06-30T12:00:00Z'],
        3600,
      ),
    ).toBeNull()
  })

  it('no-ops when the dense grid would exceed the safety cap', () => {
    // 1-second grid across ~2h = 7201 buckets > MAX_DENSE_BUCKETS (5000).
    expect(
      denseTimeGrid(['2026-06-30T09:00:00Z', '2026-06-30T11:00:01Z'], 1),
    ).toBeNull()
  })
})
