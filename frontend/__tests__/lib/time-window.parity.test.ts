import { describe, expect, test } from 'vitest'

import {
  ANCHOR_QUANTUM_SECONDS,
  FIXED_TOKEN_DELTAS,
  VALID_RANGE_TOKENS,
  isValidRangeToken,
  pickAutoToken,
  quantizeAnchor,
  resolveWindow,
} from '@/lib/time-window'

// CLIENT ≡ SERVER PARITY for the relative-range wire token.
//
// frontend/lib/time-window.ts is the FE port of backend/utils/time_window.py.
// The keyed network path (routers/network.py) resolves the scan window from
// (range_token, quantize_anchor(anchor)) and keys the response memo on
// (token, quantized_anchor, invite-clamp fingerprint). For the SSR seed and the
// client first-paint to land on the SAME key — and for the legacy absolute
// bounds the FE sends as a fallback to match what the server resolves — the two
// resolvers MUST agree byte-for-byte.
//
// The EXPECTED values below are HARD-CODED from the Python resolve_window logic
// (we can't call Python here, so the contract is encoded). They were produced
// by running the exact time_window.py algorithm with the fixed NOW below; if the
// backend math changes (epoch floor, auto bands, ISO-Z formatting), this table
// must be regenerated in lockstep — that is the whole point of the gate.

// Fixed resolution base so the "auto" history bands are deterministic.
const NOW = new Date('2026-06-29T12:00:00Z')

interface ParityCase {
  token: string
  anchor: string
  earliest: string | null
  // The (start, end) the Python resolve_window produces for (token, anchor,
  // earliest) at NOW. ISO-Z, no milliseconds (matches backend iso_z).
  start: string
  end: string
  // The fixed token "auto" resolves to (documentation + pickAutoToken check).
  autoResolvesTo?: string
}

const PARITY_TABLE: ParityCase[] = [
  // ── Fixed tokens: [quantizedAnchor - delta, quantizedAnchor] ──
  { token: '24h', anchor: '2026-06-29T12:00:37Z', earliest: null, start: '2026-06-28T12:00:00Z', end: '2026-06-29T12:00:00Z' },
  { token: '7d', anchor: '2026-06-29T12:00:37Z', earliest: null, start: '2026-06-22T12:00:00Z', end: '2026-06-29T12:00:00Z' },
  { token: '30d', anchor: '2026-06-29T12:00:00Z', earliest: null, start: '2026-05-30T12:00:00Z', end: '2026-06-29T12:00:00Z' },
  // Anchor floors to the 60s grid: :59 → :00 (same key as :00..:59).
  { token: '24h', anchor: '2026-06-29T12:00:59Z', earliest: null, start: '2026-06-28T12:00:00Z', end: '2026-06-29T12:00:00Z' },
  // Exact minute boundary stays on the minute (no floor needed).
  { token: '24h', anchor: '2026-06-29T12:01:00Z', earliest: null, start: '2026-06-28T12:01:00Z', end: '2026-06-29T12:01:00Z' },

  // ── "auto" adaptive bands (earliest relative to NOW) ──
  // No extent → "7d".
  { token: 'auto', anchor: '2026-06-29T12:00:30Z', earliest: null, start: '2026-06-22T12:00:00Z', end: '2026-06-29T12:00:00Z', autoResolvesTo: '7d' },
  // 6h history (<7d) → "24h".
  { token: 'auto', anchor: '2026-06-29T12:00:30Z', earliest: '2026-06-29T06:00:00Z', start: '2026-06-28T12:00:00Z', end: '2026-06-29T12:00:00Z', autoResolvesTo: '24h' },
  // 4d history (<7d) → "24h".
  { token: 'auto', anchor: '2026-06-29T12:00:30Z', earliest: '2026-06-25T12:00:00Z', start: '2026-06-28T12:00:00Z', end: '2026-06-29T12:00:00Z', autoResolvesTo: '24h' },
  // Exactly 7d history → "7d" (half-open: >=7d band).
  { token: 'auto', anchor: '2026-06-29T12:00:30Z', earliest: '2026-06-22T12:00:00Z', start: '2026-06-22T12:00:00Z', end: '2026-06-29T12:00:00Z', autoResolvesTo: '7d' },
  // 19d history (<30d) → "7d".
  { token: 'auto', anchor: '2026-06-29T12:00:30Z', earliest: '2026-06-10T12:00:00Z', start: '2026-06-22T12:00:00Z', end: '2026-06-29T12:00:00Z', autoResolvesTo: '7d' },
  // Exactly 30d history → "30d" (half-open: >=30d band).
  { token: 'auto', anchor: '2026-06-29T12:00:30Z', earliest: '2026-05-30T12:00:00Z', start: '2026-05-30T12:00:00Z', end: '2026-06-29T12:00:00Z', autoResolvesTo: '30d' },
  // Mature service → "30d".
  { token: 'auto', anchor: '2026-06-29T12:00:30Z', earliest: '2026-01-01T00:00:00Z', start: '2026-05-30T12:00:00Z', end: '2026-06-29T12:00:00Z', autoResolvesTo: '30d' },
  // Date-only extent widened to UTC start-of-day: exactly 7d (180h) → "7d".
  { token: 'auto', anchor: '2026-06-29T12:00:30Z', earliest: '2026-06-22', start: '2026-06-22T12:00:00Z', end: '2026-06-29T12:00:00Z', autoResolvesTo: '7d' },
]

describe('time-window client≡server parity', () => {
  test.each(PARITY_TABLE)(
    'resolveWindow($token, $anchor, earliest=$earliest) matches backend',
    ({ token, anchor, earliest, start, end }) => {
      const got = resolveWindow(token, anchor, { earliestLogAt: earliest, now: NOW })
      expect(got).toEqual({ start, end })
    },
  )

  test.each(PARITY_TABLE.filter((c) => c.autoResolvesTo))(
    'pickAutoToken(earliest=$earliest) === $autoResolvesTo',
    ({ earliest, autoResolvesTo }) => {
      expect(pickAutoToken(earliest, NOW)).toBe(autoResolvesTo)
    },
  )
})

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

describe('pickAutoToken bands (half-open, higher bucket on the boundary)', () => {
  test('no extent → 7d', () => {
    expect(pickAutoToken(null, NOW)).toBe('7d')
    expect(pickAutoToken(undefined, NOW)).toBe('7d')
  })
  test('just under 7d → 24h; exactly 7d → 7d', () => {
    expect(pickAutoToken('2026-06-22T12:00:01Z', NOW)).toBe('24h') // 6d23h59m59s
    expect(pickAutoToken('2026-06-22T12:00:00Z', NOW)).toBe('7d') // exactly 7d
  })
  test('just under 30d → 7d; exactly 30d → 30d', () => {
    expect(pickAutoToken('2026-05-30T12:00:01Z', NOW)).toBe('7d') // 29d23h59m59s
    expect(pickAutoToken('2026-05-30T12:00:00Z', NOW)).toBe('30d') // exactly 30d
  })
})

describe('token vocabulary + constants mirror the backend', () => {
  test('ANCHOR_QUANTUM_SECONDS is 60', () => {
    expect(ANCHOR_QUANTUM_SECONDS).toBe(60)
  })
  test('FIXED_TOKEN_DELTAS covers 24h/7d/30d', () => {
    expect(Object.keys(FIXED_TOKEN_DELTAS).sort()).toEqual(['24h', '30d', '7d'])
    expect(FIXED_TOKEN_DELTAS['24h']).toBe(24 * 60 * 60 * 1000)
    expect(FIXED_TOKEN_DELTAS['7d']).toBe(7 * 24 * 60 * 60 * 1000)
    expect(FIXED_TOKEN_DELTAS['30d']).toBe(30 * 24 * 60 * 60 * 1000)
  })
  test('VALID_RANGE_TOKENS = fixed tokens + auto', () => {
    expect([...VALID_RANGE_TOKENS].sort()).toEqual(['24h', '30d', '7d', 'auto'])
  })
  test('isValidRangeToken gates known tokens, rejects unknown/null', () => {
    expect(isValidRangeToken('24h')).toBe(true)
    expect(isValidRangeToken('auto')).toBe(true)
    expect(isValidRangeToken('90d')).toBe(false)
    expect(isValidRangeToken(null)).toBe(false)
    expect(isValidRangeToken(undefined)).toBe(false)
    expect(isValidRangeToken('')).toBe(false)
  })
  test('resolveWindow throws on an unrecognized token (Python ValueError parity)', () => {
    expect(() => resolveWindow('90d', '2026-06-29T12:00:00Z', { now: NOW })).toThrow()
  })
})
