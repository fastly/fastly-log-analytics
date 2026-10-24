import { describe, expect, test } from 'vitest'

import {
  ANCHOR_QUANTUM_SECONDS,
  quantizeAnchor,
} from '@/lib/time-window'

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
