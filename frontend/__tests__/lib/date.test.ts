import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { toUTCDate, formatTimeAgo, formatLocalDateTime } from '@/lib/date'

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
