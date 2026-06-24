import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { toUTCDate, formatCompactRelative, formatTimeAgo } from '@/lib/date'

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

  describe('formatCompactRelative', () => {
    it('formats seconds', () => {
      expect(formatCompactRelative(new Date(now.getTime() - 30000))).toBe('(30s)')
    })

    it('formats minutes', () => {
      expect(formatCompactRelative(new Date(now.getTime() - 120000))).toBe('(2m)')
    })

    it('formats hours', () => {
      expect(formatCompactRelative(new Date(now.getTime() - 3600000 * 5))).toBe('(5h)')
    })
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
