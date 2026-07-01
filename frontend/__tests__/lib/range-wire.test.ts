import { describe, expect, it } from 'vitest'

import { resolveRangeWire, DEFAULT_RANGE_TOKEN } from '@/lib/range-wire'

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
    expect(w.rangeBody).toEqual({ range_token: '24h', anchor: ANCHOR })
  })

  it('explicit preset pill → forwards the label as the token', () => {
    for (const preset of ['24h', '7d', '30d']) {
      const w = resolveRangeWire({
        relativeRange: preset,
        isAutoRange: false,
        startTime: START,
        endTime: END,
        anchor: ANCHOR,
      })
      expect(w.rangeToken).toBe(preset)
      expect(w.rangeKey).toBe(preset)
      expect(w.rangeBody).toEqual({ range_token: preset, anchor: ANCHOR })
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
    // No range_token in the body → backend _clamp_window uses the absolute bounds.
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
