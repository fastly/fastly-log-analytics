import { describe, it, expect } from 'vitest'
import { formatValue, formatBytes, calculateDelta } from '@/lib/format'

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
    const originalDN = Intl.DisplayNames
    // @ts-expect-error — stubbing for test
    Intl.DisplayNames = class {
      constructor() {
        throw new Error('boom')
      }
    }
    try {
      expect(formatValue('country', 'US')).toBe('US')
    } finally {
      // @ts-expect-error — restoring stub
      Intl.DisplayNames = originalDN
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
