/**
 * Consolidated tests for NetworkMap (index.tsx, MapLayer.ts, OverlayLayer.tsx, controls.tsx).
 *
 * @vitest-environment jsdom
 */
import React, { useRef, useState } from 'react'
import { act, render, renderHook, screen, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, test, expect, vi, beforeEach, afterEach } from 'vitest'
import type * as maplibregl from 'maplibre-gl'
import { stubResizeObserver } from '@/__tests__/helpers/browser-stubs'
import {
  mapInstances,
  maplibreMockFactory,
  installMaplibreSideEffects,
} from '@/__tests__/helpers/maplibre-mock'
import { selectMockFactory } from '@/__tests__/helpers/ui-mocks'
import { formatBucket } from '@/lib/date'
import type { CityTooltipData, TooltipInfo } from '@/components/Map/NetworkMap/OverlayLayer'
import type { PlaybackControls as PlaybackControlsComponent } from '@/components/Map/NetworkMap/controls'
import type { NetworkMapData } from '@/components/Map/NetworkMap/MapLayer'

vi.mock('next-themes', () => ({
  useTheme: () => ({ theme: 'light' }),
}))

vi.mock('@/stores/timezoneStore', () => ({
  useTimezoneStore: () => ({ timezone: 'UTC' }),
}))

vi.mock('maplibre-gl', () => maplibreMockFactory())
vi.mock('maplibre-gl/dist/maplibre-gl.css', () => ({}))
installMaplibreSideEffects()

vi.mock('@/components/ui/select', () => selectMockFactory())

vi.mock('@/components/ui/slider', () => {
  const Slider = ({
    value,
    min,
    max,
    step,
    onValueChange,
  }: {
    value: number[]
    min: number
    max: number
    step?: number
    onValueChange?: (v: number[]) => void
  }) => (
    <input
      type="range"
      data-testid="mock-slider"
      min={min}
      max={max}
      step={step}
      value={value[0]}
      onChange={(e) => onValueChange?.([Number(e.target.value)])}
    />
  )
  return { Slider }
})

const mapDataCalls: Array<{ setTooltip: (t: TooltipInfo | null) => void }> = []

vi.mock('@/components/Map/NetworkMap/MapLayer', () => ({
  formatBucket: (iso: string) => (iso ? `LBL(${iso})` : ''),
  useMapInit: () => {},
  useMapData: ({ setTooltip }: { setTooltip: (t: TooltipInfo | null) => void }) => {
    mapDataCalls.push({ setTooltip })
  },
}))

const playbackProps: { current: React.ComponentProps<typeof PlaybackControlsComponent> | null } = { current: null }

vi.mock('@/components/Map/NetworkMap/controls', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/Map/NetworkMap/controls')>()
  return {
    ...actual,
    PlaybackControls: (props: React.ComponentProps<typeof PlaybackControlsComponent>) => {
      playbackProps.current = props
      return (
        <div
          data-testid="playback-controls"
          data-playing={String(props.playing)}
          data-bucket-idx={String(props.bucketIdx)}
          data-buckets-length={String(props.bucketsLength)}
          data-metric={String(props.metric)}
          data-current-label={String(props.currentBucketLabel)}
        />
      )
    },
  }
})

vi.mock('@/components/Map/NetworkMap/OverlayLayer', () => ({
  MapTooltip: ({ info, metric }: { info: TooltipInfo | null; metric: string }) => (
    <div data-testid="map-tooltip" data-metric={metric}>
      {info?.city ?? ''}
    </div>
  ),
}))

import { NetworkMap } from '@/components/Map/NetworkMap'

const actualMapLayer = await vi.importActual<typeof import('@/components/Map/NetworkMap/MapLayer')>(
  '@/components/Map/NetworkMap/MapLayer',
)
const { getScoreColor, useMapInit, useMapData } = actualMapLayer

const actualOverlayLayer = await vi.importActual<typeof import('@/components/Map/NetworkMap/OverlayLayer')>(
  '@/components/Map/NetworkMap/OverlayLayer',
)
const { formatMetricValue, MapTooltip } = actualOverlayLayer

const actualControls = await vi.importActual<typeof import('@/components/Map/NetworkMap/controls')>(
  '@/components/Map/NetworkMap/controls',
)
const { PlaybackControls, METRIC_OPTIONS, SPEED_OPTIONS, STEP_OPTIONS } = actualControls

function baseProps(overrides: Partial<React.ComponentProps<typeof NetworkMap>> = {}) {
  return {
    data: null,
    isLoading: false,
    className: 'test-cls',
    metric: 'health_score',
    onMetricChange: vi.fn(),
    bucketSeconds: 60,
    onBucketChange: vi.fn(),
    mapAsn: 'all',
    onAsnChange: vi.fn(),
    asnOptions: [],
    ...overrides,
  }
}

beforeEach(() => {
  stubResizeObserver()
  mapDataCalls.length = 0
  playbackProps.current = null
})

afterEach(() => {
  vi.useRealTimers()
})

describe('NetworkMap empty-state', () => {
  it('renders "Loading map data..." when isLoading and no buckets', () => {
    render(<NetworkMap {...baseProps({ isLoading: true, data: null })} />)
    expect(screen.getByText('Loading map data...')).toBeInTheDocument()
    expect(screen.queryByTestId('playback-controls')).not.toBeInTheDocument()
  })

  it('renders "No map data available" when not loading and no buckets', () => {
    render(<NetworkMap {...baseProps({ isLoading: false, data: { buckets: [] } })} />)
    expect(screen.getByText('No map data available')).toBeInTheDocument()
    expect(screen.queryByTestId('playback-controls')).not.toBeInTheDocument()
  })

  it('UX-3: shows a distinct error state, not "No map data available", when error is set', () => {
    render(<NetworkMap {...baseProps({ data: undefined, error: new Error('map boom') })} />)
    expect(screen.getByRole('alert')).toHaveTextContent(/failed to load map data/i)
    expect(screen.queryByText('No map data available')).toBeNull()
  })

  it('UX-8: renders the empty state without throwing when buckets are absent', () => {
    expect(() =>
      render(<NetworkMap {...baseProps({ data: {}, error: null })} />),
    ).not.toThrow()
    expect(screen.getByText('No map data available')).toBeInTheDocument()
  })
})

describe('NetworkMap with data', () => {
  it('renders PlaybackControls and forwards key props when buckets exist', () => {
    const data = { buckets: ['2026-01-01T00:00:00', '2026-01-01T00:01:00', '2026-01-01T00:02:00'] }
    render(<NetworkMap {...baseProps({ data })} />)

    const ctrl = screen.getByTestId('playback-controls')
    expect(ctrl).toBeInTheDocument()
    expect(ctrl.getAttribute('data-bucket-idx')).toBe('2')
    expect(ctrl.getAttribute('data-buckets-length')).toBe('3')
    expect(ctrl.getAttribute('data-playing')).toBe('false')
    expect(ctrl.getAttribute('data-metric')).toBe('health_score')
    expect(ctrl.getAttribute('data-current-label')).toBe(formatBucket('2026-01-01T00:02:00', 'UTC'))

    expect(screen.queryByText('Loading map data...')).not.toBeInTheDocument()
    expect(screen.queryByText('No map data available')).not.toBeInTheDocument()
  })
})

describe('NetworkMap auto-play', () => {
  it('increments bucketIdx on the playInterval when playing flips to true', () => {
    const data = { buckets: ['t0', 't1'] }

    vi.useFakeTimers()
    render(<NetworkMap {...baseProps({ data })} />)

    expect(screen.getByTestId('playback-controls').getAttribute('data-bucket-idx')).toBe('1')
    expect(playbackProps.current).not.toBeNull()

    act(() => {
      playbackProps.current!.setPlaying(true)
    })

    act(() => {
      vi.advanceTimersByTime(100)
    })
    expect(screen.getByTestId('playback-controls').getAttribute('data-bucket-idx')).toBe('0')

    act(() => {
      vi.advanceTimersByTime(100)
    })
    expect(screen.getByTestId('playback-controls').getAttribute('data-bucket-idx')).toBe('1')
  })
})

describe('NetworkMap data-change reset', () => {
  it('resets bucketIdx to the last index whenever data.buckets identity changes', () => {
    const data1 = { buckets: ['a', 'b', 'c', 'd', 'e'] }
    const { rerender } = render(<NetworkMap {...baseProps({ data: data1 })} />)
    expect(screen.getByTestId('playback-controls').getAttribute('data-bucket-idx')).toBe('4')

    const data2 = { buckets: ['x', 'y'] }
    rerender(<NetworkMap {...baseProps({ data: data2 })} />)
    expect(screen.getByTestId('playback-controls').getAttribute('data-bucket-idx')).toBe('1')

    const data3 = { buckets: [] as string[] }
    rerender(<NetworkMap {...baseProps({ data: data3 })} />)
    expect(screen.queryByTestId('playback-controls')).not.toBeInTheDocument()
    expect(screen.getByText('No map data available')).toBeInTheDocument()
  })
})

describe('NetworkMap tooltip portal', () => {
  it('renders MapTooltip into document.body when setTooltip receives an info object', () => {
    const data = { buckets: ['t0', 't1', 't2'] }
    render(<NetworkMap {...baseProps({ data, metric: 'rtt_med_us' })} />)

    expect(mapDataCalls.length).toBeGreaterThan(0)
    const lastCall = mapDataCalls[mapDataCalls.length - 1]

    act(() => {
      lastCall.setTooltip({
        clientX: 50,
        clientY: 60,
        city: 'PortalCity',
        country: 'PortalLand',
        cityData: { reqs: 99, rtt_med_us: 12345 },
      })
    })

    const tooltip = screen.getByTestId('map-tooltip')
    expect(tooltip).toBeInTheDocument()
    expect(tooltip.getAttribute('data-metric')).toBe('rtt_med_us')
    expect(tooltip.textContent).toBe('PortalCity')
    expect(tooltip.parentElement).toBe(document.body)

    act(() => {
      lastCall.setTooltip(null)
    })
    expect(screen.queryByTestId('map-tooltip')).not.toBeInTheDocument()
  })
})

describe('formatBucket', () => {
  it('returns an empty string for empty input', () => {
    expect(formatBucket('', 'America/New_York')).toBe('')
  })

  it('returns the original input when the ISO string is unparseable', () => {
    expect(formatBucket('not-a-date', 'UTC')).toBe('not-a-date')
  })

  it('formats a Z-suffixed ISO string in the requested timezone', () => {
    const out = formatBucket('2026-06-15T15:30:45Z', 'UTC')
    expect(out).toMatch(/Jun 15/)
    expect(out).toMatch(/3:30:45/)
    expect(out).toMatch(/PM/)
  })

  it('treats an unsuffixed ISO string as UTC by appending Z', () => {
    const withZ = formatBucket('2026-06-15T15:30:45Z', 'UTC')
    const withoutZ = formatBucket('2026-06-15T15:30:45', 'UTC')
    expect(withoutZ).toBe(withZ)
  })

  it('respects a numeric +/- offset suffix without re-appending Z', () => {
    const offset = formatBucket('2026-06-15T15:30:45+0000', 'UTC')
    const zulu = formatBucket('2026-06-15T15:30:45Z', 'UTC')
    expect(offset).toBe(zulu)
  })
})

describe('getScoreColor', () => {
  it('returns transparent when val is null', () => {
    expect(getScoreColor(null, 'health_score')).toBe('transparent')
    expect(getScoreColor(null, 'throughput_bps')).toBe('transparent')
  })

  it('returns the unknown-metric fallback color for unrecognised metrics', () => {
    expect(getScoreColor(50, 'something_else')).toBe('#3b82f6')
  })

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

function useMapInitHarness(opts: {
  isDark: boolean
  setTooltip?: (t: TooltipInfo | null) => void
}) {
  const containerEl = document.createElement('div')
  const mapContainer = useRef<HTMLDivElement | null>(containerEl)
  const isDarkRef = useRef(opts.isDark)
  const dmaDataRef = useRef<Record<number, CityTooltipData>>({})
  const setTooltip = opts.setTooltip ?? (() => {})

  const map = useMapInit({
    mapContainer,
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
      cooperativeGestures: true,
    })
  })

  it('registers dma / heatmap sources and the three core layers on load', async () => {
    renderHook(() => useMapInitHarness({ isDark: false }))
    await act(async () => {
      await Promise.resolve()
    })
    const inst = mapInstances[0]
    expect(Object.keys(inst.sources).sort()).toEqual(['dma', 'heatmap'])
    const layerIds = inst.layers.map((l: { id?: string }) => l.id)
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

    expect(first.removed).toBe(true)
    expect(mapInstances.length).toBe(2)
  })
})

function asMaplibreMap(candidate: object): maplibregl.Map {
  if (!('getLayer' in candidate && 'getSource' in candidate && 'addSource' in candidate)) {
    throw new Error('maplibre mock is missing the Map surface useMapData relies on')
  }
  return candidate as maplibregl.Map
}

function useMapDataHarness(opts: {
  data: NetworkMapData | null
  bucketIdx?: number
  metric?: string
  isDark?: boolean
}) {
  const [loadedMap] = useState(() => {
    const mod = maplibreMockFactory()
    const mock = new mod.default.Map({ container: document.createElement('div') })
    mock.addSource('heatmap', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } })
    mock.addLayer({ id: 'countries', type: 'fill', source: 'heatmap' })
    return asMaplibreMap(mock)
  })
  const map = useRef<maplibregl.Map | null>(loadedMap)
  const dmaDataRef = useRef<Record<number, CityTooltipData>>({})
  const setTooltip = vi.fn<(t: TooltipInfo | null) => void>()

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
          bucket_idx: 0,
          bucket: '2026-06-15T12:00:00Z',
          cities: [
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
    expect(Object.keys(result.current.dmaDataRef.current)).toEqual(['506'])
  })

  it('clears the tooltip on every data tick', () => {
    const data = {
      cities: [{ name: 'Boston', lat: 42.36, lon: -71.06 }],
      map_buckets: [
        {
          bucket_idx: 0,
          bucket: '2026-06-15T12:00:00Z',
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

describe('formatMetricValue', () => {
  it('returns em dash for null/undefined values', () => {
    expect(formatMetricValue(null, 'health_score')).toBe('—')
    expect(formatMetricValue(undefined, 'rtt_med_us')).toBe('—')
  })

  it('formats health_score as NN/100', () => {
    expect(formatMetricValue(87.4, 'health_score')).toBe('87/100')
    expect(formatMetricValue(100, 'health_score')).toBe('100/100')
  })

  it('converts rtt_med_us microseconds to milliseconds with one decimal', () => {
    expect(formatMetricValue(12345, 'rtt_med_us')).toBe('12.3 ms')
    expect(formatMetricValue(0, 'rtt_med_us')).toBe('0.0 ms')
  })

  it('formats avg_ploss as a percent with two decimals', () => {
    expect(formatMetricValue(0.0123, 'avg_ploss')).toBe('1.23%')
    expect(formatMetricValue(0, 'avg_ploss')).toBe('0.00%')
  })

  it('formats error_pct directly with two decimals', () => {
    expect(formatMetricValue(4.5678, 'error_pct')).toBe('4.57%')
  })

  it('scales throughput_bps across the Gbps/Mbps/Kbps/bps ladder', () => {
    expect(formatMetricValue(2.5e9, 'throughput_bps')).toBe('2.5 Gbps')
    expect(formatMetricValue(3.4e6, 'throughput_bps')).toBe('3.4 Mbps')
    expect(formatMetricValue(7.8e3, 'throughput_bps')).toBe('7.8 Kbps')
    expect(formatMetricValue(500, 'throughput_bps')).toBe('500 bps')
  })

  it('falls back to String(val) for unknown metric ids', () => {
    expect(formatMetricValue(42, 'unknown_metric')).toBe('42')
  })
})

describe('MapTooltip', () => {
  const baseInfo: TooltipInfo = {
    clientX: 100,
    clientY: 200,
    city: 'Sydney',
    country: 'Australia',
    cityData: { reqs: 12345, health_score: 92, rtt_med_us: 18000 },
  }

  it('renders city, country, and a localized requests count', () => {
    render(<MapTooltip info={baseInfo} metric="rtt_med_us" />)

    expect(screen.getByText('Sydney')).toBeInTheDocument()
    expect(screen.getByText('Australia')).toBeInTheDocument()
    expect(screen.getByText('Median RTT')).toBeInTheDocument()
    expect(screen.getByText('18.0 ms')).toBeInTheDocument()
    expect(screen.getByText('12,345')).toBeInTheDocument()
    expect(screen.getByText('Health Score')).toBeInTheDocument()
    expect(screen.getByText('92/100')).toBeInTheDocument()
  })

  it('flips the tooltip to the left when cursor is in the right 30% of viewport', () => {
    const info: TooltipInfo = { ...baseInfo, clientX: 900 }
    const { container } = render(<MapTooltip info={info} metric="error_pct" />)

    const tooltip = container.firstChild as HTMLElement
    expect(tooltip).not.toBeNull()
    expect(tooltip.style.left).toBe('886px')
    expect(tooltip.style.transform).toBe('translate(-100%, -100%)')

    const { container: c2 } = render(<MapTooltip info={{ ...info, clientX: 100 }} metric="error_pct" />)
    const tooltip2 = c2.firstChild as HTMLElement
    expect(tooltip2.style.left).toBe('114px')
    expect(tooltip2.style.transform).toBe('translateY(-100%)')
  })

  it('hides the duplicate health-score row when the selected metric IS health_score', () => {
    render(<MapTooltip info={baseInfo} metric="health_score" />)

    const labels = screen.getAllByText('Health Score')
    expect(labels).toHaveLength(1)
    expect(screen.getByText('92/100')).toBeInTheDocument()
  })
})

function defaultControlsProps(overrides: Partial<React.ComponentProps<typeof PlaybackControls>> = {}) {
  return {
    playing: false,
    setPlaying: vi.fn(),
    bucketIdx: 2,
    setBucketIdx: vi.fn(),
    bucketsLength: 10,
    firstBucketLabel: '10:00',
    currentBucketLabel: '10:02',
    lastBucketLabel: '10:09',
    metric: 'health_score',
    onMetricChange: vi.fn(),
    bucketSeconds: 60,
    onBucketChange: vi.fn(),
    playInterval: 1000,
    setPlayInterval: vi.fn(),
    mapAsn: 'all',
    onAsnChange: vi.fn(),
    asnOptions: [
      { value: '7922', label: 'AS7922 Comcast' },
      { value: '15169', label: 'AS15169 Google' },
    ],
    ...overrides,
  }
}

describe('PlaybackControls', () => {
  test('renders Play when !playing, Pause when playing, and click toggles setPlaying', async () => {
    const user = userEvent.setup()

    const setPlayingPaused = vi.fn()
    const { rerender, container } = render(
      <PlaybackControls {...defaultControlsProps({ playing: false, setPlaying: setPlayingPaused })} />
    )

    const playBtn = screen.getByRole('button', { name: /play map playback/i })
    expect(playBtn).toBeInTheDocument()

    await user.click(playBtn)
    expect(setPlayingPaused).toHaveBeenCalledTimes(1)
    expect(setPlayingPaused).toHaveBeenCalledWith(true)

    const setPlayingPlaying = vi.fn()
    rerender(
      <PlaybackControls {...defaultControlsProps({ playing: true, setPlaying: setPlayingPlaying })} />
    )

    const pauseBtn = screen.getByRole('button', { name: /pause map playback/i })
    expect(pauseBtn).toBeInTheDocument()

    await user.click(pauseBtn)
    expect(setPlayingPlaying).toHaveBeenCalledTimes(1)
    expect(setPlayingPlaying).toHaveBeenCalledWith(false)

    expect(container.querySelector('.absolute')).toBeInTheDocument()
  })

  test('Slider onValueChange fires setBucketIdx + setPlaying(false)', () => {
    const setBucketIdx = vi.fn()
    const setPlaying = vi.fn()
    render(
      <PlaybackControls
        {...defaultControlsProps({ setBucketIdx, setPlaying, playing: true, bucketIdx: 2 })}
      />
    )

    const slider = screen.getByTestId('mock-slider') as HTMLInputElement
    expect(slider).toHaveValue('2')
    expect(slider.min).toBe('0')
    expect(slider.max).toBe('9')

    fireEvent.change(slider, { target: { value: '5' } })

    expect(setBucketIdx).toHaveBeenCalledWith(5)
    expect(setPlaying).toHaveBeenCalledWith(false)
  })

  test('bucket label triple renders firstBucketLabel / currentBucketLabel / lastBucketLabel', () => {
    render(
      <PlaybackControls
        {...defaultControlsProps({
          firstBucketLabel: 'FIRST_LBL',
          currentBucketLabel: 'CURRENT_LBL',
          lastBucketLabel: 'LAST_LBL',
        })}
      />
    )
    expect(screen.getByText('FIRST_LBL')).toBeInTheDocument()
    expect(screen.getByText('CURRENT_LBL')).toBeInTheDocument()
    expect(screen.getByText('LAST_LBL')).toBeInTheDocument()
  })

  test('metric Select onValueChange calls onMetricChange and ignores empty value', async () => {
    const user = userEvent.setup()
    const onMetricChange = vi.fn()
    render(<PlaybackControls {...defaultControlsProps({ onMetricChange, metric: 'health_score' })} />)

    const metricItem = screen
      .getAllByRole('button')
      .find((b) => b.getAttribute('data-select-item-value') === 'rtt_med_us')
    expect(metricItem).toBeTruthy()

    await user.click(metricItem!)
    expect(onMetricChange).toHaveBeenCalledTimes(1)
    expect(onMetricChange).toHaveBeenCalledWith('rtt_med_us')
  })

  test('bucketSeconds Select converts string value back to a number on change', async () => {
    const user = userEvent.setup()
    const onBucketChange = vi.fn()
    render(<PlaybackControls {...defaultControlsProps({ onBucketChange, bucketSeconds: 60 })} />)

    const item = screen
      .getAllByRole('button')
      .find((b) => b.getAttribute('data-select-item-value') === '300')
    expect(item).toBeTruthy()

    await user.click(item!)

    expect(onBucketChange).toHaveBeenCalledTimes(1)
    expect(onBucketChange).toHaveBeenCalledWith(300)
    expect(typeof onBucketChange.mock.calls[0][0]).toBe('number')
  })

  test("asn Select renders 'all' + dynamic asnOptions and fires onAsnChange", async () => {
    const user = userEvent.setup()
    const onAsnChange = vi.fn()
    const asnOptions = [
      { value: '7922', label: 'AS7922 Comcast' },
      { value: '15169', label: 'AS15169 Google' },
    ]
    render(<PlaybackControls {...defaultControlsProps({ onAsnChange, asnOptions })} />)

    expect(
      screen
        .getAllByRole('button')
        .filter((b) => b.getAttribute('data-select-item-value') === 'all')
    ).toHaveLength(1)
    expect(screen.getByText('All ASNs')).toBeInTheDocument()
    expect(screen.getByText('AS7922 Comcast')).toBeInTheDocument()
    expect(screen.getByText('AS15169 Google')).toBeInTheDocument()

    const target = screen
      .getAllByRole('button')
      .find((b) => b.getAttribute('data-select-item-value') === '15169')
    expect(target).toBeTruthy()

    await user.click(target!)
    expect(onAsnChange).toHaveBeenCalledWith('15169')
  })
})

describe('exported option arrays', () => {
  test('METRIC_OPTIONS shape: 5 entries, each {value, label}', () => {
    expect(METRIC_OPTIONS).toHaveLength(5)
    for (const o of METRIC_OPTIONS) {
      expect(typeof o.value).toBe('string')
      expect(typeof o.label).toBe('string')
    }
    expect(METRIC_OPTIONS.map((o) => o.value)).toEqual([
      'health_score',
      'rtt_med_us',
      'avg_ploss',
      'error_pct',
      'throughput_bps',
    ])
  })

  test('SPEED_OPTIONS shape: numeric values, label strings', () => {
    expect(SPEED_OPTIONS).toHaveLength(4)
    for (const o of SPEED_OPTIONS) {
      expect(typeof o.value).toBe('number')
      expect(typeof o.label).toBe('string')
    }
    const values = SPEED_OPTIONS.map((o) => o.value)
    expect(values).toEqual([...values].sort((a, b) => b - a))
  })

  test('STEP_OPTIONS shape: 11 entries, monotonically increasing seconds', () => {
    expect(STEP_OPTIONS).toHaveLength(11)
    const values = STEP_OPTIONS.map((o) => o.value)
    for (let i = 1; i < values.length; i++) {
      expect(values[i]).toBeGreaterThan(values[i - 1])
    }
    expect(values[0]).toBe(1)
    expect(values[values.length - 1]).toBe(14400)
  })
})
