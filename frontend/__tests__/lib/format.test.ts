import { describe, it, expect, vi } from 'vitest'
import { formatValue, formatBytes, calculateDelta, formatCount, formatCountMaybe, formatAbbreviatedCount } from '@/lib/format'

describe('formatCount', () => {
  it('matches Number.prototype.toLocaleString() byte-for-byte', () => {
    for (const n of [0, 5, 42, 1000, 1234567, -9876, 1234.5, 0.125]) {
      expect(formatCount(n)).toBe(n.toLocaleString())
    }
  })

  it('passes through Intl.NumberFormatOptions', () => {
    expect(formatCount(3.14159, { maximumFractionDigits: 1 })).toBe(
      (3.14159).toLocaleString(undefined, { maximumFractionDigits: 1 }),
    )
  })
})

describe('formatBytes', () => {
  it('formats bytes correctly', () => {
    expect(formatBytes(0)).toBe('0 B')
    expect(formatBytes(1024)).toBe('1 KB')
    expect(formatBytes(1024 * 1024)).toBe('1 MB')
    expect(formatBytes(1024 * 1024 * 1.5)).toBe('1.5 MB')
  })
})

describe('formatValue', () => {
  it('formats null/undefined as "null"', () => {
    expect(formatValue('any', null)).toBe('null')
    expect(formatValue('any', undefined)).toBe('null')
  })

  it('formats bytes fields using formatBytes', () => {
    expect(formatValue('resp_bytes', 1024)).toBe('1 KB')
    expect(formatValue('req_header_bytes', 500)).toBe('500 B')
  })

  it('formats numeric fields with toLocaleString', () => {
    expect(formatValue('requests', 1000)).toBe('1,000')
  })

  it('formats country codes to names', () => {
    // Note: Intl.DisplayNames might not be available in all test environments
    // but jsdom/node usually has it.
    expect(formatValue('country', 'US')).toBe('United States')
    expect(formatValue('country', 'GB')).toBe('United Kingdom')
  })

  it('title cases city/region/pop', () => {
    expect(formatValue('city', 'new york')).toBe('New York')
    expect(formatValue('pop', 'jfk')).toBe('JFK')
    expect(formatValue('region', 'ca')).toBe('CA')
  })

  it('formats proto/tls to 1 decimal place', () => {
    expect(formatValue('proto', 3)).toBe('3')
    expect(formatValue('tls', 1.3)).toBe('1.3')
  })

  // Branch coverage: proto/tls path for string inputs (line 44-49 in format.ts)
  it('parses proto string values: valid numeric string → trimmed decimal', () => {
    // "2.0" → Number("2.0") = 2 → parseFloat("2.0").toString() = "2"
    expect(formatValue('proto', '2.0')).toBe('2')
    // "1.3" stays "1.3"
    expect(formatValue('tls', '1.3')).toBe('1.3')
  })

  it('passes invalid proto/tls strings through unchanged', () => {
    // Non-numeric string: Number() yields NaN, branch falls through to return str
    expect(formatValue('proto', 'h2')).toBe('h2')
    expect(formatValue('tls', 'TLSv1.3')).toBe('TLSv1.3')
  })

  // Branch coverage: Intl.DisplayNames throw path (line 32 catch)
  it('falls back to raw country code when Intl.DisplayNames throws', () => {
    const displayNames = vi.spyOn(Intl, 'DisplayNames').mockImplementation(function () {
      throw new Error('boom')
    })
    try {
      expect(formatValue('country', 'US')).toBe('US')
    } finally {
      displayNames.mockRestore()
    }
  })
})

describe('calculateDelta', () => {
  it('calculates percentage change', () => {
    expect(calculateDelta(110, 100)).toBe(10)
    expect(calculateDelta(90, 100)).toBe(-10)
  })

  it('returns null for zero or undefined baseline', () => {
    expect(calculateDelta(100, 0)).toBe(null)
    expect(calculateDelta(100, undefined)).toBe(null)
  })
})

describe('formatCountMaybe', () => {
  it('returns undefined for null/undefined and formats numbers like formatCount', () => {
    expect(formatCountMaybe(undefined)).toBeUndefined()
    expect(formatCountMaybe(null)).toBeUndefined()
    expect(formatCountMaybe(1234567)).toBe(formatCount(1234567))
    expect(formatCountMaybe(0)).toBe('0')
  })
})

describe('formatAbbreviatedCount', () => {
  it('abbreviates with K/M/B and configurable M/B precision', () => {
    expect(formatAbbreviatedCount(999)).toBe('999')
    expect(formatAbbreviatedCount(1500)).toBe('1.5K')
    expect(formatAbbreviatedCount(2_345_678)).toBe('2.3M')
    expect(formatAbbreviatedCount(2_345_678, 2)).toBe('2.35M')
    expect(formatAbbreviatedCount(3_210_000_000, 2)).toBe('3.21B')
  })
})
