import { describe, it, expect, vi } from 'vitest'
import {
  formatValue,
  formatBytes,
  calculateDelta,
  formatCount,
  formatCountMaybe,
  formatAbbreviatedCount,
  ELAPSED_AGE_COMPACT,
  ELAPSED_FRESHNESS,
  ELAPSED_RELATIVE_AGO,
  ELAPSED_RELATIVE_AGO_JUST_NOW,
  formatBytesTiered,
  formatElapsed,
} from '@/lib/format'

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

type Row<I> = [I, string]

const METADATA_BYTES: Row<number | null>[] = [
  [0, "0 B"],
  [1, "1 B"],
  [1023, "1023 B"],
  [1024, "1.0 KB"],
  [1025, "1.0 KB"],
  [1536, "1.5 KB"],
  [1048575, "1024.0 KB"],
  [1048576, "1.0 MB"],
  [5366336, "5.1 MB"],
  [1073741823, "1024.0 MB"],
  [1073741824, "1.00 GB"],
  [3271225472, "3.05 GB"],
  [5497558138880, "5120.00 GB"],
  [null, "–"],
]

const QUARANTINE_BYTES: Row<number>[] = [
  [0, "0 B"],
  [1, "1 B"],
  [1023, "1023 B"],
  [1024, "1.0 KB"],
  [1025, "1.0 KB"],
  [1536, "1.5 KB"],
  [1048575, "1024.0 KB"],
  [1048576, "1.0 MB"],
  [5366336, "5.1 MB"],
  [1073741823, "1024.0 MB"],
  [1073741824, "1024.0 MB"],
  [3271225472, "3119.7 MB"],
  [5497558138880, "5242880.0 MB"],
]

const BOT_SOURCES_RELATIVE: Row<number>[] = [
  [-5000, "just now"],
  [0, "just now"],
  [1000, "just now"],
  [44400, "just now"],
  [44500, "just now"],
  [45000, "just now"],
  [59000, "just now"],
  [59500, "just now"],
  [60000, "just now"],
  [89000, "just now"],
  [89400, "just now"],
  [89500, "just now"],
  [90000, "just now"],
  [119000, "just now"],
  [120000, "2m ago"],
  [121000, "2m ago"],
  [179000, "2m ago"],
  [3599000, "59m ago"],
  [3599600, "59m ago"],
  [3600000, "1h ago"],
  [5399000, "1h ago"],
  [5400000, "1h ago"],
  [7200000, "2h ago"],
  [86399000, "23h ago"],
  [86400000, "1d ago"],
  [90000000, "1d ago"],
  [172799000, "1d ago"],
  [172800000, "2d ago"],
  [259200000, "3d ago"],
  [400000000, "4d ago"],
]

const RESUME_BANNER_RELATIVE: Row<number>[] = [
  [-5000, "-5s ago"],
  [0, "0s ago"],
  [1000, "1s ago"],
  [45000, "45s ago"],
  [59000, "59s ago"],
  [60000, "1m ago"],
  [89000, "1m ago"],
  [90000, "1m ago"],
  [119000, "1m ago"],
  [120000, "2m ago"],
  [121000, "2m ago"],
  [179000, "2m ago"],
  [3599000, "59m ago"],
  [3600000, "1h ago"],
  [5399000, "1h ago"],
  [5400000, "1h ago"],
  [7200000, "2h ago"],
  [86399000, "23h ago"],
  [86400000, "1d ago"],
  [90000000, "1d ago"],
  [172799000, "1d ago"],
  [172800000, "2d ago"],
  [259200000, "3d ago"],
  [400000000, "4d ago"],
]

const SYSTEM_HEALTH_AGE: Row<number>[] = [
  [-5, "-5s"],
  [0, "0s"],
  [1, "1s"],
  [44.4, "44s"],
  [44.5, "45s"],
  [45, "45s"],
  [59, "59s"],
  [59.5, "60s"],
  [60, "60s"],
  [89, "89s"],
  [89.4, "89s"],
  [89.5, "90s"],
  [90, "2m"],
  [119, "2m"],
  [120, "2m"],
  [121, "2m"],
  [179, "3m"],
  [3599, "60m"],
  [3599.6, "60m"],
  [3600, "60m"],
  [5399, "90m"],
  [5400, "1.5h"],
  [7200, "2.0h"],
  [86399, "24.0h"],
  [86400, "24.0h"],
  [90000, "25.0h"],
  [172799, "48.0h"],
  [172800, "2.0d"],
  [259200, "3.0d"],
  [400000, "4.6d"],
]

const HIGH_SCALE_FRESHNESS: Row<number>[] = [
  [-5, "-5s"],
  [0, "0s"],
  [1, "1s"],
  [44.4, "44s"],
  [44.5, "45s"],
  [45, "45s"],
  [59, "59s"],
  [59.5, "60s"],
  [60, "1m"],
  [89, "1m"],
  [89.4, "1m"],
  [89.5, "1m"],
  [90, "2m"],
  [119, "2m"],
  [120, "2m"],
  [121, "2m"],
  [179, "3m"],
  [3599, "60m"],
  [3599.6, "60m"],
  [3600, "1.0h"],
  [5399, "1.5h"],
  [5400, "1.5h"],
  [7200, "2.0h"],
  [86399, "24.0h"],
  [86400, "24.0h"],
  [90000, "25.0h"],
  [172799, "48.0h"],
  [172800, "48.0h"],
  [259200, "72.0h"],
  [400000, "111.1h"],
]

describe('formatBytesTiered', () => {
  it.each(METADATA_BYTES)('metadata card (GB cap) %s -> %s', (n, want) => {
    expect(formatBytesTiered(n, { maxUnit: 'GB' })).toBe(want)
  })
  it('renders the metadata card placeholder for undefined', () => {
    expect(formatBytesTiered(undefined, { maxUnit: 'GB' })).toBe('–')
  })
  it.each(QUARANTINE_BYTES)('quarantine (MB cap) %s -> %s', (n, want) => {
    expect(formatBytesTiered(n)).toBe(want)
  })
})

describe('formatElapsed presets', () => {
  it.each(BOT_SOURCES_RELATIVE)('bot sources %s ms -> %s', (ms, want) => {
    expect(formatElapsed(Math.floor(ms / 1000), ELAPSED_RELATIVE_AGO_JUST_NOW)).toBe(want)
  })
  it.each(RESUME_BANNER_RELATIVE)('resume banner %s ms -> %s', (ms, want) => {
    expect(formatElapsed(Math.floor(ms / 1000), ELAPSED_RELATIVE_AGO)).toBe(want)
  })
  it.each(SYSTEM_HEALTH_AGE)('system health age %s s -> %s', (s, want) => {
    expect(formatElapsed(s, ELAPSED_AGE_COMPACT)).toBe(want)
  })
  it.each(HIGH_SCALE_FRESHNESS)('high-scale freshness %s s -> %s', (s, want) => {
    expect(formatElapsed(s, ELAPSED_FRESHNESS)).toBe(want)
  })
})
