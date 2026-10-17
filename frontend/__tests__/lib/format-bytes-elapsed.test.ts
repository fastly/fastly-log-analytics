/**
 * Pins the exact outputs of the per-component byte / elapsed-time formatters
 * that were unified into lib/format.ts (tables captured from the old local
 * implementations before they were removed).
 */
import { describe, expect, it } from 'vitest'

import {
  ELAPSED_AGE_COMPACT,
  ELAPSED_FRESHNESS,
  ELAPSED_RELATIVE_AGO,
  ELAPSED_RELATIVE_AGO_JUST_NOW,
  formatBytesTiered,
  formatElapsed,
} from '@/lib/format'

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

// input is the elapsed milliseconds (Date.now() - then)
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

// input is an age in seconds
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
