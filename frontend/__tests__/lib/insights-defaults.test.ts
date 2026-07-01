import { describe, it, expect } from 'vitest'
import {
  pickInsightsDefault,
  historyHoursFromExtents,
  WINDOW_OPTIONS,
  BASELINE_OPTIONS,
  STATIC_DEFAULT,
} from '@/lib/insights-defaults'

const WIN = new Set(WINDOW_OPTIONS.map((o) => o.value))
const BASE = new Set(BASELINE_OPTIONS.map((o) => o.value))

describe('pickInsightsDefault', () => {
  it('returns the static default when history is null', () => {
    expect(pickInsightsDefault(null)).toEqual({
      window: STATIC_DEFAULT.window,
      baseline: STATIC_DEFAULT.baseline,
    })
  })

  it.each([
    [0.5, '0.25', '1'],
    [2, '1', '1'],
    [12, '4', '1'],
    [36, '4', '24'],
    [96, '24', '24'],
    [200, '1', '168'],
    [800, '1', '720'],
  ])('history %sh -> window %s / baseline %s', (h, w, b) => {
    expect(pickInsightsDefault(h)).toEqual({ window: w, baseline: b })
  })

  // Half-open intervals: an exact boundary selects the higher bucket.
  it.each([
    [0.999, '0.25', '1'],
    [1, '1', '1'],
    [3.999, '1', '1'],
    [4, '4', '1'],
    [23.999, '4', '1'],
    [24, '4', '24'],
    [47.999, '4', '24'],
    [48, '24', '24'],
    [167.999, '24', '24'],
    [168, '1', '168'],
    [719.999, '1', '168'],
    [720, '1', '720'],
  ])('boundary %sh -> %s / %s', (h, w, b) => {
    expect(pickInsightsDefault(h)).toEqual({ window: w, baseline: b })
  })

  it('only ever returns real dropdown option values', () => {
    for (const h of [null, 0, 0.5, 1, 2, 4, 12, 24, 36, 96, 168, 200, 720, 2000, NaN]) {
      const r = pickInsightsDefault(h as number | null)
      expect(WIN.has(r.window)).toBe(true)
      expect(BASE.has(r.baseline)).toBe(true)
    }
  })

  it('treats NaN as no-data (static default)', () => {
    expect(pickInsightsDefault(NaN)).toEqual({
      window: STATIC_DEFAULT.window,
      baseline: STATIC_DEFAULT.baseline,
    })
  })
})

describe('historyHoursFromExtents', () => {
  const NOW = Date.parse('2026-06-15T12:00:00.000Z')

  it('returns null for null/undefined/empty earliest', () => {
    expect(historyHoursFromExtents(null, NOW)).toBeNull()
    expect(historyHoursFromExtents(undefined, NOW)).toBeNull()
    expect(historyHoursFromExtents('', NOW)).toBeNull()
  })

  it('computes hours from a full ISO earliest', () => {
    expect(historyHoursFromExtents('2026-06-15T10:00:00.000Z', NOW)).toBeCloseTo(2, 6)
  })

  it('widens a date-only earliest to UTC start-of-day', () => {
    // 2026-06-14T00:00:00Z -> 2026-06-15T12:00:00Z = 36h
    expect(historyHoursFromExtents('2026-06-14', NOW)).toBeCloseTo(36, 6)
  })

  it('returns null for an unparseable earliest', () => {
    expect(historyHoursFromExtents('not-a-date', NOW)).toBeNull()
  })

  it('clamps a future earliest to 0 (never negative)', () => {
    expect(historyHoursFromExtents('2026-06-15T13:00:00.000Z', NOW)).toBe(0)
  })
})
