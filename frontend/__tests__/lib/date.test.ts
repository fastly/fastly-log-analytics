import { describe, it, expect, test, vi, beforeEach, afterEach } from 'vitest'
import { toUTCDate, formatTimeAgo, formatLocalDateTime } from '@/lib/date'
import { resolveRangeWire, DEFAULT_RANGE_TOKEN } from '@/lib/range-wire'
import { ANCHOR_QUANTUM_SECONDS, quantizeAnchor } from '@/lib/time-window'

describe('toUTCDate', () => {
  it('parses ISO strings', () => {
    const d = toUTCDate('2024-01-01T00:00:00Z')
    expect(d.toISOString()).toBe('2024-01-01T00:00:00.000Z')
  })

  it('handles space separator from backend', () => {
    const d = toUTCDate('2024-01-01 12:00:00')
    expect(d.toISOString()).toBe('2024-01-01T12:00:00.000Z')
  })
})

describe('relative time formatting', () => {
  const now = new Date('2024-06-15T12:00:00Z')

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('formatTimeAgo', () => {
    it('formats recent', () => {
      expect(formatTimeAgo(new Date(now.getTime() - 5000))).toBe('5s ago')
    })

    it('formats minutes and seconds', () => {
      expect(formatTimeAgo(new Date(now.getTime() - 75000))).toBe('1m 15s ago')
    })

    it('formats hours', () => {
      expect(formatTimeAgo(new Date(now.getTime() - 3600000 * 2 - 60000))).toBe('2h 1m 0s ago')
    })
  })
})

describe('formatLocalDateTime', () => {
  it('matches new Date(x).toLocaleString() for ISO strings', () => {
    const iso = '2026-07-01T12:34:56Z'
    expect(formatLocalDateTime(iso)).toBe(new Date(iso).toLocaleString())
  })

  it('matches for epoch-millis numbers', () => {
    const ms = 1_750_000_000_000
    expect(formatLocalDateTime(ms)).toBe(new Date(ms).toLocaleString())
  })

  it('accepts a Date instance directly', () => {
    const d = new Date('2026-01-15T00:00:00Z')
    expect(formatLocalDateTime(d)).toBe(d.toLocaleString())
  })

  it('surfaces Invalid Date like the native call', () => {
    expect(formatLocalDateTime('not-a-date')).toBe(new Date('not-a-date').toLocaleString())
  })

  it('forwards Intl.DateTimeFormatOptions', () => {
    const iso = '2026-07-01T12:34:56Z'
    const opts: Intl.DateTimeFormatOptions = { dateStyle: 'short' }
    expect(formatLocalDateTime(iso, opts)).toBe(new Date(iso).toLocaleString(undefined, opts))
  })
})

const ANCHOR = '2026-06-29T12:00:00Z'
const START = '2026-06-10T00:00:00Z'
const END = '2026-06-12T00:00:00Z'

describe('resolveRangeWire', () => {
  it('cold-load / auto default → "24h" token (server-reproducible, SSR-seedable)', () => {
    const w = resolveRangeWire({
      relativeRange: null,
      isAutoRange: true,
      startTime: START,
      endTime: END,
      anchor: ANCHOR,
    })
    expect(w.rangeToken).toBe(DEFAULT_RANGE_TOKEN)
    expect(w.rangeKey).toBe('24h')
    expect(w.rangeBody).toEqual({ range_token: '24h', anchor: ANCHOR, start_time: START, end_time: END })
  })

  it('explicit preset pill → forwards the label as the token + display bounds', () => {
    for (const preset of ['1h', '3h', '6h', '12h', '24h', '3d', '7d', '30d']) {
      const w = resolveRangeWire({
        relativeRange: preset,
        isAutoRange: false,
        startTime: START,
        endTime: END,
        anchor: ANCHOR,
      })
      expect(w.rangeToken).toBe(preset)
      expect(w.rangeKey).toBe(preset)
      expect(w.rangeBody).toEqual({ range_token: preset, anchor: ANCHOR, start_time: START, end_time: END })
    }
  })

  it('custom absolute range (relativeRange null, not auto) → sends bounds, no token', () => {
    const w = resolveRangeWire({
      relativeRange: null,
      isAutoRange: false,
      startTime: START,
      endTime: END,
      anchor: ANCHOR,
    })
    expect(w.rangeToken).toBeNull()
    expect(w.rangeBody).toEqual({ start_time: START, end_time: END })
    expect('range_token' in w.rangeBody).toBe(false)
  })

  it('distinct custom ranges produce distinct cache keys (no collision)', () => {
    const a = resolveRangeWire({
      relativeRange: null,
      isAutoRange: false,
      startTime: START,
      endTime: END,
      anchor: ANCHOR,
    })
    const b = resolveRangeWire({
      relativeRange: null,
      isAutoRange: false,
      startTime: '2026-05-01T00:00:00Z',
      endTime: '2026-05-02T00:00:00Z',
      anchor: ANCHOR,
    })
    expect(a.rangeKey).not.toBe(b.rangeKey)
  })

  it('a preset wins even when isAutoRange somehow lingers true', () => {
    const w = resolveRangeWire({
      relativeRange: '7d',
      isAutoRange: true,
      startTime: START,
      endTime: END,
      anchor: ANCHOR,
    })
    expect(w.rangeToken).toBe('7d')
  })
})

const NOW = new Date('2026-06-29T12:00:00Z')

describe('quantizeAnchor floors to the 60s grid', () => {
  test('floors a sub-minute anchor to :00', () => {
    expect(quantizeAnchor('2026-06-29T12:00:59Z', NOW)).toBe('2026-06-29T12:00:00Z')
  })
  test('leaves an exact-minute anchor unchanged', () => {
    expect(quantizeAnchor('2026-06-29T12:00:00Z', NOW)).toBe('2026-06-29T12:00:00Z')
  })
  test('two anchors within the same quantum collapse to one value (key stability)', () => {
    const a = quantizeAnchor('2026-06-29T12:00:01Z', NOW)
    const b = quantizeAnchor('2026-06-29T12:00:58Z', NOW)
    expect(a).toBe(b)
    expect(a).toBe('2026-06-29T12:00:00Z')
  })
  test('a missing anchor falls back to quantized now', () => {
    expect(quantizeAnchor(undefined, NOW)).toBe('2026-06-29T12:00:00Z')
  })
  test('an unparseable anchor falls back to quantized now', () => {
    expect(quantizeAnchor('not-a-date', NOW)).toBe('2026-06-29T12:00:00Z')
  })
  test('output carries NO milliseconds fraction (matches backend iso_z)', () => {
    expect(quantizeAnchor('2026-06-29T12:00:30.123Z', NOW)).toBe('2026-06-29T12:00:00Z')
  })
})

describe('token vocabulary + constants mirror the backend', () => {
  test('ANCHOR_QUANTUM_SECONDS is 60', () => {
    expect(ANCHOR_QUANTUM_SECONDS).toBe(60)
  })
})
