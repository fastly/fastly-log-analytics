/**
 * MapLayer hosts two pure helpers (`formatBucket`, `getScoreColor`) and two
 * MapLibre-bound hooks (`useMapInit`, `useMapData`) consumed by NetworkMap.
 * The pure helpers were 0%-covered; the hooks are likewise unverified
 * because they require a WebGL context jsdom can't provide.
 *
 * Strategy:
 *
 * (A) Direct unit tests for the helpers — formatBucket covers the ISO /
 *     invalid / already-zoned / empty branches, and getScoreColor walks the
 *     5 metric × 4 tier matrix (20 assertions).
 *
 * (B) `useMapInit` + `useMapData` smoke tests via @testing-library/react
 *     `renderHook`, driving the shared maplibre mock from
 *     `__tests__/helpers/maplibre-mock`. We verify the constructor was
 *     called with the container element, that `addSource` registered the
 *     two scatter/fill sources, and that unmount calls `remove()` on the
 *     map (the cleanup path that was previously untested).
 *
 * @vitest-environment jsdom
 */
import { renderHook, act } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import React, { useRef } from 'react'

import {
  mapInstances,
  maplibreMockFactory,
  installMaplibreSideEffects,
  type MockMapInstance,
} from '../../../helpers/maplibre-mock'

vi.mock('maplibre-gl', () => maplibreMockFactory())
vi.mock('maplibre-gl/dist/maplibre-gl.css', () => ({}))
installMaplibreSideEffects()

// ---------------------------------------------------------------------------
// (A) Pure helpers

import { formatBucket, getScoreColor, useMapInit, useMapData } from '@/components/Map/NetworkMap/MapLayer'

describe('formatBucket', () => {
  it('returns an empty string for empty input', () => {
    expect(formatBucket('', 'America/New_York')).toBe('')
  })

  it('returns the original input when the ISO string is unparseable', () => {
    // Bogus date string — `new Date('not-a-date').getTime()` is NaN, the
    // helper returns the raw input so the UI shows something rather than
    // 'Invalid Date'.
    expect(formatBucket('not-a-date', 'UTC')).toBe('not-a-date')
  })

  it('formats a Z-suffixed ISO string in the requested timezone', () => {
    // 2026-06-15T15:30:45Z in UTC → "Jun 15, 3:30:45 PM" in en-US.
    const out = formatBucket('2026-06-15T15:30:45Z', 'UTC')
    expect(out).toMatch(/Jun 15/)
    expect(out).toMatch(/3:30:45/)
    // hour12 → "PM" tail expected for 15:30.
    expect(out).toMatch(/PM/)
  })

  it('treats an unsuffixed ISO string as UTC by appending Z', () => {
    // The helper checks /[Z+\-]\d*$/ and appends 'Z' if the suffix is
    // missing. Both inputs should resolve to the same instant.
    const withZ = formatBucket('2026-06-15T15:30:45Z', 'UTC')
    const withoutZ = formatBucket('2026-06-15T15:30:45', 'UTC')
    expect(withoutZ).toBe(withZ)
  })

  it('respects a numeric +/- offset suffix without re-appending Z', () => {
    // The suffix regex is /[Z+\-]\d*$/ — `+0000` (no colon) satisfies it,
    // so the helper passes the string through to `new Date(...)` as-is.
    // +0000 == Z → identical formatted output.
    const offset = formatBucket('2026-06-15T15:30:45+0000', 'UTC')
    const zulu = formatBucket('2026-06-15T15:30:45Z', 'UTC')
    expect(offset).toBe(zulu)
  })
})

describe('getScoreColor', () => {
  // Universal: null payload → transparent regardless of metric.
  it('returns transparent when val is null', () => {
    expect(getScoreColor(null, 'health_score')).toBe('transparent')
    expect(getScoreColor(null, 'throughput_bps')).toBe('transparent')
  })

  it('returns the unknown-metric fallback color for unrecognised metrics', () => {
    expect(getScoreColor(50, 'something_else')).toBe('#3b82f6')
  })

  // health_score: high-is-good — ladder 90 / 70 / 50 / else.
  describe('health_score (high-is-good)', () => {
    it('>= 90 → green', () => {
      expect(getScoreColor(95, 'health_score')).toBe('#22c55e')
    })
    it('>= 70 → yellow', () => {
      expect(getScoreColor(75, 'health_score')).toBe('#eab308')
    })
    it('>= 50 → orange', () => {
      expect(getScoreColor(55, 'health_score')).toBe('#f97316')
    })
    it('< 50 → red', () => {
      expect(getScoreColor(10, 'health_score')).toBe('#ef4444')
    })
  })

  // throughput_bps: high-is-good — ladder 100M / 10M / 1M / else.
  describe('throughput_bps (high-is-good)', () => {
    it('>= 100Mbps → green', () => {
      expect(getScoreColor(150_000_000, 'throughput_bps')).toBe('#22c55e')
    })
    it('>= 10Mbps → yellow', () => {
      expect(getScoreColor(50_000_000, 'throughput_bps')).toBe('#eab308')
    })
    it('>= 1Mbps → orange', () => {
      expect(getScoreColor(5_000_000, 'throughput_bps')).toBe('#f97316')
    })
    it('< 1Mbps → red', () => {
      expect(getScoreColor(500_000, 'throughput_bps')).toBe('#ef4444')
    })
  })

  // rtt_med_us: low-is-good — ladder 50_000 / 150_000 / 300_000 / else.
  describe('rtt_med_us (low-is-good)', () => {
    it('<= 50ms → green', () => {
      expect(getScoreColor(40_000, 'rtt_med_us')).toBe('#22c55e')
    })
    it('<= 150ms → yellow', () => {
      expect(getScoreColor(120_000, 'rtt_med_us')).toBe('#eab308')
    })
    it('<= 300ms → orange', () => {
      expect(getScoreColor(250_000, 'rtt_med_us')).toBe('#f97316')
    })
    it('> 300ms → red', () => {
      expect(getScoreColor(500_000, 'rtt_med_us')).toBe('#ef4444')
    })
  })

  // avg_ploss: low-is-good — ladder 0.01 / 0.05 / 0.10 / else.
  describe('avg_ploss (low-is-good)', () => {
    it('<= 1% → green', () => {
      expect(getScoreColor(0.005, 'avg_ploss')).toBe('#22c55e')
    })
    it('<= 5% → yellow', () => {
      expect(getScoreColor(0.03, 'avg_ploss')).toBe('#eab308')
    })
    it('<= 10% → orange', () => {
      expect(getScoreColor(0.08, 'avg_ploss')).toBe('#f97316')
    })
    it('> 10% → red', () => {
      expect(getScoreColor(0.25, 'avg_ploss')).toBe('#ef4444')
    })
  })

  // error_pct: low-is-good — ladder 1 / 5 / 10 / else.
  describe('error_pct (low-is-good)', () => {
    it('<= 1% → green', () => {
      expect(getScoreColor(0.5, 'error_pct')).toBe('#22c55e')
    })
    it('<= 5% → yellow', () => {
      expect(getScoreColor(3, 'error_pct')).toBe('#eab308')
    })
    it('<= 10% → orange', () => {
      expect(getScoreColor(8, 'error_pct')).toBe('#f97316')
    })
    it('> 10% → red', () => {
      expect(getScoreColor(25, 'error_pct')).toBe('#ef4444')
    })
  })
})

// (B) useMapInit + useMapData hook smoke

/**
 * Tiny harness that mirrors the ref shape used by NetworkMap. We pre-attach
 * a real HTMLDivElement to `mapContainer` so useMapInit can pass it to the
 * maplibregl.Map constructor.
 */
function useMapInitHarness(opts: {
  isDark: boolean
  setTooltip?: (t: any) => void
}) {
  const containerEl = document.createElement('div')
  const mapContainer = useRef<HTMLDivElement | null>(containerEl)
  const map = useRef<any>(null)
  const isDarkRef = useRef(opts.isDark)
  const dmaDataRef = useRef<Record<number, any>>({})
  const setTooltip = opts.setTooltip ?? (() => {})

  useMapInit({
    mapContainer,
    map,
    isDark: opts.isDark,
    isDarkRef,
    dmaDataRef,
    setTooltip,
  })

  return { mapContainer, map, dmaDataRef }
}

describe('useMapInit', () => {
  beforeEach(() => {
    mapInstances.length = 0
  })

  it('constructs a maplibregl.Map with the container element', () => {
    renderHook(() => useMapInitHarness({ isDark: false }))
    expect(mapInstances.length).toBe(1)
    const inst = mapInstances[0]
    expect(inst.container).toBeInstanceOf(HTMLDivElement)
    expect(inst.options).toMatchObject({
      renderWorldCopies: false,
      zoom: 1,
      interactive: true,
    })
  })

  it('registers dma / heatmap sources and the three core layers on load', async () => {
    renderHook(() => useMapInitHarness({ isDark: false }))
    // The mock fires `load` on a queued microtask — flush it.
    await act(async () => {
      await Promise.resolve()
    })
    const inst = mapInstances[0]
    // The 'world' source was dropped from MapLayer.tsx in favour of the
    // base style's countries layer (no explicit GeoJSON source needed);
    // only the two app-specific overlay sources remain.
    expect(Object.keys(inst.sources).sort()).toEqual(['dma', 'heatmap'])
    const layerIds = inst.layers.map((l: any) => l.id)
    expect(layerIds).toEqual(expect.arrayContaining(['countries', 'dma-fill', 'city-scatter']))
  })

  it('calls remove() on the map when the hook unmounts', () => {
    const { unmount } = renderHook(() => useMapInitHarness({ isDark: false }))
    expect(mapInstances.length).toBe(1)
    const inst = mapInstances[0]
    expect(inst.removed).toBe(false)
    unmount()
    expect(inst.removed).toBe(true)
  })

  it('rebuilds the map when isDark flips (cleanup + reconstruct)', () => {
    const { rerender } = renderHook(
      ({ isDark }: { isDark: boolean }) => useMapInitHarness({ isDark }),
      { initialProps: { isDark: false } },
    )
    expect(mapInstances.length).toBe(1)
    const first = mapInstances[0]

    rerender({ isDark: true })

    // The dependency-array on the useEffect is `[isDark]`, so flipping it
    // must tear down the prior map and spin up a new one.
    expect(first.removed).toBe(true)
    expect(mapInstances.length).toBe(2)
  })
})

/**
 * useMapData harness: assumes the map is already loaded (the mock returns
 * `isStyleLoaded() === true`). Stuffs a single city payload through the
 * hook so we can confirm it doesn't throw on the happy path.
 */
function useMapDataHarness(opts: {
  data: any
  bucketIdx?: number
  metric?: string
  isDark?: boolean
}) {
  const map = useRef<any>(null)
  const dmaDataRef = useRef<Record<number, any>>({})
  const setTooltip = vi.fn()

  // Synthesize a fully-loaded MockMap so useMapData has something to drive.
  // We don't go through useMapInit here because useMapData runs in its own
  // useEffect with `[bucketIdx, data, metric, isDark]` deps.
  if (!map.current) {
    // Pull the mocked Map constructor out of the mocked module by going
    // through maplibreMockFactory directly — avoids the async load timing
    // dance for these data-only assertions.
    const mod = maplibreMockFactory()
    map.current = new (mod.default.Map as any)({ container: document.createElement('div') })
    map.current.addSource('heatmap', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } })
    map.current.addLayer({ id: 'countries', type: 'fill', source: 'heatmap' })
  }

  useMapData({
    map,
    dmaDataRef,
    data: opts.data,
    bucketIdx: opts.bucketIdx ?? 0,
    metric: opts.metric ?? 'health_score',
    isDark: opts.isDark ?? false,
    setTooltip,
  })

  return { map, dmaDataRef, setTooltip }
}

describe('useMapData', () => {
  beforeEach(() => {
    mapInstances.length = 0
  })

  it('is a no-op when data is null / missing map_buckets', () => {
    const { setTooltip } = renderHook(() => useMapDataHarness({ data: null })).result.current
    // Should not have cleared tooltip because the early-return runs first.
    expect(setTooltip).not.toHaveBeenCalled()
  })

  it('populates dmaDataRef for DMA cities and leaves it empty for scatter-only payloads', () => {
    const data = {
      cities: [
        { name: 'Boston', lat: 42.36, lon: -71.06 },
        { name: 'Munich', lat: 48.1, lon: 11.6 },
      ],
      map_buckets: [
        {
          cities: [
            // DMA city — has metro_code, should land in dmaDataRef.
            {
              city_idx: 0,
              country: 'US',
              metro_code: 506,
              health_score: 95,
              rtt_med_us: 40_000,
              avg_ploss: 0.001,
              error_pct: 0.5,
              throughput_bps: 200_000_000,
              reqs: 100,
            },
            // Non-DMA city — lat/lon come from the cities[city_idx] record now.
            {
              city_idx: 1,
              country: 'DE',
              health_score: 60,
              rtt_med_us: 200_000,
              avg_ploss: 0.04,
              error_pct: 3,
              throughput_bps: 5_000_000,
              reqs: 50,
            },
          ],
        },
      ],
    }
    const { result } = renderHook(() => useMapDataHarness({ data, metric: 'health_score' }))
    expect(result.current.dmaDataRef.current[506]).toMatchObject({
      city: 'Boston',
      country: 'US',
      health_score: 95,
    })
    // Munich has no metro_code and so should not appear in the DMA ref.
    expect(Object.keys(result.current.dmaDataRef.current)).toEqual(['506'])
  })

  it('clears the tooltip on every data tick', () => {
    const data = {
      cities: [{ name: 'Boston', lat: 42.36, lon: -71.06 }],
      map_buckets: [
        {
          cities: [
            { city_idx: 0, country: 'US', metro_code: 506, health_score: 95, reqs: 1 },
          ],
        },
      ],
    }
    const { result } = renderHook(() => useMapDataHarness({ data }))
    expect(result.current.setTooltip).toHaveBeenCalledWith(null)
  })
})
