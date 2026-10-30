/**
 * Consolidated tests for NetworkMap (index.tsx, MapLayer.ts, OverlayLayer.tsx, controls.tsx).
 *
 * @vitest-environment jsdom
 */
import React, { useRef, useState } from 'react'
import { renderToString } from 'react-dom/server'
import { act, render, renderHook, screen, fireEvent, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, test, expect, vi, beforeEach, afterEach } from 'vitest'
import type * as maplibregl from 'maplibre-gl'
import {
  stubResizeObserver,
  selectMockFactory,
  mapInstances,
  maplibreMockFactory,
  installMaplibreSideEffects,
} from '@/__tests__/helpers/page-smoke'
import { formatBucket } from '@/lib/utils'
import {
  NetworkMap,
  PlaybackControls,
  METRIC_OPTIONS,
  SPEED_OPTIONS,
  STEP_OPTIONS,
  formatMetricValue,
  MapTooltip,
  MapLayer,
  getScoreColor,
  useMapInit,
  useMapData,
  type CityTooltipData,
  type TooltipInfo,
  type NetworkMapData,
} from '@/components/Map/NetworkMap'
import {
  ShieldingMap,
  greatCirclePoints,
  efficiencyColor,
  lineWidth,
  buildArcFeatures,
  buildDotFeatures,
} from '@/components/Map/ShieldingMap'
import { rafThrottle, ChoroplethMap, MapPrewarm, useMapResize } from '@/components/Map/ChoroplethMap'
import { ImpossibleDistanceModal } from '@/components/Insights/InsightDetailModal'
import { InsightHelpModal } from '@/components/Insights/InsightHelpModal'

const { setWorkerUrl, constructedWithWorker, failMapConstruction } = vi.hoisted(() => ({
  setWorkerUrl: vi.fn(),
  constructedWithWorker: vi.fn(),
  failMapConstruction: vi.fn(() => false),
}))

vi.mock('next-themes', () => ({ useTheme: vi.fn(() => ({ theme: 'light' })) }))

vi.mock('@/stores/filterStore', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/stores/filterStore')>()),
  useTimezoneStore: () => ({ timezone: 'UTC' }),
}))

let useStrictSources = false

vi.mock('maplibre-gl', () => {
  const mapMock = maplibreMockFactory({ strictSources: false })
  class PrewarmMap extends mapMock.Map {
    constructor(options: ConstructorParameters<typeof mapMock.Map>[0]) {
      if (failMapConstruction()) throw new Error('WebGL unavailable')
      constructedWithWorker(setWorkerUrl.mock.calls.at(-1)?.[0])
      super(options)
    }
    override getSource(id: string) {
      const src = this.sources[id] as ReturnType<InstanceType<typeof mapMock.Map>['getSource']>
      return useStrictSources ? src : super.getSource(id)
    }
  }
  return { ...mapMock, Map: PrewarmMap, setWorkerUrl }
})
vi.mock('maplibre-gl/dist/maplibre-gl.css', () => ({}))
installMaplibreSideEffects()

vi.mock('@/components/ui/select', () => selectMockFactory())

vi.mock('@/components/ui/slider', () => ({
  Slider: ({
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
  ),
}))

const mapDataCalls: Array<{ setTooltip: (t: TooltipInfo | null) => void }> = []

MapLayer.useMapInit = (() => ({ current: null })) as typeof useMapInit
MapLayer.useMapData = (({ setTooltip }: { setTooltip: (t: TooltipInfo | null) => void }) => {
  mapDataCalls.push({ setTooltip })
}) as typeof useMapData

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

const stubWorldTopologyFetch = () =>
  vi.stubGlobal(
    'fetch',
    vi.fn(async () => ({
      json: async () => ({
        type: 'Topology',
        objects: { world: { type: 'GeometryCollection', geometries: [] } },
        arcs: [],
      }),
    })),
  )

beforeEach(() => {
  stubResizeObserver()
  mapDataCalls.length = 0
  stubWorldTopologyFetch()
})

afterEach(() => {
  vi.useRealTimers()
})

describe('NetworkMap empty-state & data rendering', () => {
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
    expect(() => render(<NetworkMap {...baseProps({ data: {}, error: null })} />)).not.toThrow()
    expect(screen.getByText('No map data available')).toBeInTheDocument()
  })

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

  it('increments bucketIdx on the playInterval when playing flips to true', () => {
    vi.useFakeTimers()
    render(<NetworkMap {...baseProps({ data: { buckets: ['t0', 't1'] } })} />)
    expect(screen.getByTestId('playback-controls').getAttribute('data-bucket-idx')).toBe('1')
    act(() => { fireEvent.click(screen.getByRole('button', { name: /play map playback/i })) })
    act(() => { vi.advanceTimersByTime(500) })
    expect(screen.getByTestId('playback-controls').getAttribute('data-bucket-idx')).toBe('0')
    act(() => { vi.advanceTimersByTime(500) })
    expect(screen.getByTestId('playback-controls').getAttribute('data-bucket-idx')).toBe('1')
  })

  it('resets bucketIdx to the last index whenever data.buckets identity changes', () => {
    const { rerender } = render(<NetworkMap {...baseProps({ data: { buckets: ['a', 'b', 'c', 'd', 'e'] } })} />)
    expect(screen.getByTestId('playback-controls').getAttribute('data-bucket-idx')).toBe('4')
    rerender(<NetworkMap {...baseProps({ data: { buckets: ['x', 'y'] } })} />)
    expect(screen.getByTestId('playback-controls').getAttribute('data-bucket-idx')).toBe('1')
    rerender(<NetworkMap {...baseProps({ data: { buckets: [] as string[] } })} />)
    expect(screen.queryByTestId('playback-controls')).not.toBeInTheDocument()
    expect(screen.getByText('No map data available')).toBeInTheDocument()
  })

  it('selects the last non-empty bucket index when trailing map_buckets have no cities', () => {
    const data = {
      buckets: ['t0', 't1', 't2'],
      map_buckets: [
        { bucket_idx: 0, bucket: 't0', cities: [{ city_idx: 0, country: 'US', health_score: 95, reqs: 10 }] },
        { bucket_idx: 1, bucket: 't1', cities: [{ city_idx: 0, country: 'US', health_score: 90, reqs: 20 }] },
        { bucket_idx: 2, bucket: 't2', cities: [] },
      ],
    }
    render(<NetworkMap {...baseProps({ data })} />)
    expect(screen.getByTestId('playback-controls').getAttribute('data-bucket-idx')).toBe('1')
  })

  it('renders MapTooltip into document.body when setTooltip receives an info object', () => {
    render(<NetworkMap {...baseProps({ data: { buckets: ['t0', 't1', 't2'] }, metric: 'rtt_med_us' })} />)
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
    expect(tooltip.textContent).toContain('PortalCity')
    expect(tooltip.parentElement).toBe(document.body)
    act(() => { lastCall.setTooltip(null) })
    expect(screen.queryByTestId('map-tooltip')).not.toBeInTheDocument()
  })
})

describe('formatBucket', () => {
  it('handles empty, unparseable, Z-suffixed, unsuffixed, and numeric-offset ISO strings', () => {
    expect(formatBucket('', 'America/New_York')).toBe('')
    expect(formatBucket('not-a-date', 'UTC')).toBe('not-a-date')
    const withZ = formatBucket('2026-06-15T15:30:45Z', 'UTC')
    expect(withZ).toMatch(/Jun 15/)
    expect(withZ).toMatch(/3:30:45/)
    expect(withZ).toMatch(/PM/)
    expect(formatBucket('2026-06-15T15:30:45', 'UTC')).toBe(withZ)
    expect(formatBucket('2026-06-15T15:30:45+0000', 'UTC')).toBe(withZ)
  })
})

describe('getScoreColor', () => {
  it('returns transparent for null and fallback blue for unknown metric', () => {
    expect(getScoreColor(null, 'health_score')).toBe('transparent')
    expect(getScoreColor(null, 'throughput_bps')).toBe('transparent')
    expect(getScoreColor(50, 'something_else')).toBe('#3b82f6')
  })

  it.each([
    ['health_score', [95, 75, 55, 10]],
    ['throughput_bps', [150_000_000, 50_000_000, 5_000_000, 500_000]],
    ['rtt_med_us', [40_000, 120_000, 250_000, 500_000]],
    ['avg_ploss', [0.005, 0.03, 0.08, 0.25]],
    ['error_pct', [0.5, 3, 8, 25]],
  ] as const)('maps %s across green/yellow/orange/red thresholds', (metric, [green, yellow, orange, red]) => {
    expect(getScoreColor(green, metric)).toBe('#22c55e')
    expect(getScoreColor(yellow, metric)).toBe('#eab308')
    expect(getScoreColor(orange, metric)).toBe('#f97316')
    expect(getScoreColor(red, metric)).toBe('#ef4444')
  })
})

function useMapInitHarness(opts: { isDark: boolean; setTooltip?: (t: TooltipInfo | null) => void }) {
  const containerEl = document.createElement('div')
  const mapContainer = useRef<HTMLDivElement | null>(containerEl)
  const isDarkRef = useRef(opts.isDark)
  const dmaDataRef = useRef<Record<number, CityTooltipData>>({})
  const setTooltip = opts.setTooltip ?? (() => {})
  const map = useMapInit({ mapContainer, isDark: opts.isDark, isDarkRef, dmaDataRef, setTooltip })
  return { mapContainer, map, dmaDataRef }
}

describe('useMapInit', () => {
  beforeEach(() => { mapInstances.length = 0 })

  it('keeps the same map instance when the caller passes a new onMapError every render', () => {
    const setTooltip = vi.fn()
    const { rerender } = renderHook(() => {
      const mapContainer = useRef<HTMLDivElement | null>(document.createElement('div'))
      const isDarkRef = useRef(false)
      const dmaDataRef = useRef<Record<number, CityTooltipData>>({})
      return useMapInit({ mapContainer, isDark: false, isDarkRef, dmaDataRef, setTooltip, onMapError: () => {} })
    })
    rerender()
    rerender()
    expect(mapInstances.length).toBe(1)
    expect(mapInstances[0].removed).toBe(false)
  })

  it('constructs a maplibregl.Map with the container element', () => {
    renderHook(() => useMapInitHarness({ isDark: false }))
    expect(mapInstances.length).toBe(1)
    const inst = mapInstances[0]
    expect(inst.container).toBeInstanceOf(HTMLDivElement)
    expect(inst.options).toMatchObject({ renderWorldCopies: false, zoom: 1, cooperativeGestures: true })
  })

  it('registers dma / heatmap sources and the three core layers on load', async () => {
    renderHook(() => useMapInitHarness({ isDark: false }))
    await act(async () => { await Promise.resolve() })
    const inst = mapInstances[0]
    expect(Object.keys(inst.sources).sort()).toEqual(['dma', 'heatmap'])
    const layerIds = inst.layers.map((l: { id?: string }) => l.id)
    expect(layerIds).toEqual(expect.arrayContaining(['countries', 'dma-fill', 'city-scatter']))
  })

  it('calls remove() on the map when the hook unmounts', () => {
    const { unmount } = renderHook(() => useMapInitHarness({ isDark: false }))
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
  beforeEach(() => { mapInstances.length = 0 })

  it('is a no-op when data is null / missing map_buckets', () => {
    const { setTooltip } = renderHook(() => useMapDataHarness({ data: null })).result.current
    expect(setTooltip).not.toHaveBeenCalled()
  })

  it('populates dmaDataRef for DMA cities, leaves scatter-only empty, and clears tooltip', () => {
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
              city_idx: 0, country: 'US', metro_code: 506, health_score: 95,
              rtt_med_us: 40_000, avg_ploss: 0.001, error_pct: 0.5, throughput_bps: 200_000_000, reqs: 100,
            },
            {
              city_idx: 1, country: 'DE', health_score: 60,
              rtt_med_us: 200_000, avg_ploss: 0.04, error_pct: 3, throughput_bps: 5_000_000, reqs: 50,
            },
          ],
        },
      ],
    }
    const { result } = renderHook(() => useMapDataHarness({ data, metric: 'health_score' }))
    expect(result.current.dmaDataRef.current[506]).toMatchObject({ city: 'Boston', country: 'US', health_score: 95 })
    expect(Object.keys(result.current.dmaDataRef.current)).toEqual(['506'])
    expect(result.current.setTooltip).toHaveBeenCalledWith(null)
  })

  it('computes fallback throughput_bps coloring when throughput_bps is absent from wire city objects', () => {
    const data = {
      bucket_seconds: 3600,
      cities: [{ name: 'Boston', lat: 42.36, lon: -71.06 }],
      map_buckets: [
        {
          bucket_idx: 0,
          bucket: '2026-06-15T12:00:00Z',
          cities: [
            {
              city_idx: 0, country: 'US', metro_code: 506, health_score: 95,
              rtt_med_us: 40_000, avg_ploss: 0.001, error_pct: 0.5, reqs: 10000,
            },
          ],
        },
      ],
    }
    const { result } = renderHook(() => useMapDataHarness({ data, metric: 'throughput_bps' }))
    expect(result.current.dmaDataRef.current[506]).toMatchObject({ city: 'Boston', country: 'US', reqs: 10000 })
  })
})

describe('formatMetricValue', () => {
  it.each([
    [null, 'health_score', '—'],
    [undefined, 'rtt_med_us', '—'],
    [87.4, 'health_score', '87/100'],
    [100, 'health_score', '100/100'],
    [12345, 'rtt_med_us', '12.3 ms'],
    [0, 'rtt_med_us', '0.0 ms'],
    [0.0123, 'avg_ploss', '1.23%'],
    [0, 'avg_ploss', '0.00%'],
    [4.5678, 'error_pct', '4.57%'],
    [2.5e9, 'throughput_bps', '2.5 Gbps'],
    [3.4e6, 'throughput_bps', '3.4 Mbps'],
    [7.8e3, 'throughput_bps', '7.8 Kbps'],
    [500, 'throughput_bps', '500 bps'],
    [42, 'unknown_metric', '42'],
  ] as const)('formats (%s, %s) → %s', (val, metric, expected) => {
    expect(formatMetricValue(val, metric)).toBe(expected)
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
    for (const txt of ['Sydney', 'Australia', 'Median RTT', '18.0 ms', '12,345', 'Health Score', '92/100']) {
      expect(screen.getByText(txt)).toBeInTheDocument()
    }
  })

  it('flips the tooltip to the left when cursor is in the right 30% of viewport', () => {
    const { container } = render(<MapTooltip info={{ ...baseInfo, clientX: 900 }} metric="error_pct" />)
    const tooltip = container.firstChild as HTMLElement
    expect(tooltip.style.left).toBe('886px')
    expect(tooltip.style.transform).toBe('translate(-100%, -100%)')

    const { container: c2 } = render(<MapTooltip info={{ ...baseInfo, clientX: 100 }} metric="error_pct" />)
    const tooltip2 = c2.firstChild as HTMLElement
    expect(tooltip2.style.left).toBe('114px')
    expect(tooltip2.style.transform).toBe('translateY(-100%)')
  })

  it('hides the duplicate health-score row when the selected metric IS health_score', () => {
    render(<MapTooltip info={baseInfo} metric="health_score" />)
    expect(screen.getAllByText('Health Score')).toHaveLength(1)
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

describe('PlaybackControls & exported option arrays', () => {
  test('renders Play when !playing, Pause when playing, and click toggles setPlaying', async () => {
    const user = userEvent.setup()
    const setPlayingPaused = vi.fn()
    const { rerender, container } = render(
      <PlaybackControls {...defaultControlsProps({ playing: false, setPlaying: setPlayingPaused })} />,
    )
    await user.click(screen.getByRole('button', { name: /play map playback/i }))
    expect(setPlayingPaused).toHaveBeenCalledWith(true)

    const setPlayingPlaying = vi.fn()
    rerender(<PlaybackControls {...defaultControlsProps({ playing: true, setPlaying: setPlayingPlaying })} />)
    await user.click(screen.getByRole('button', { name: /pause map playback/i }))
    expect(setPlayingPlaying).toHaveBeenCalledWith(false)
    expect(container.querySelector('.absolute')).toBeInTheDocument()
  })

  test('Slider onValueChange fires setBucketIdx + setPlaying(false) and bucket labels render', () => {
    const setBucketIdx = vi.fn()
    const setPlaying = vi.fn()
    render(
      <PlaybackControls
        {...defaultControlsProps({
          setBucketIdx,
          setPlaying,
          playing: true,
          bucketIdx: 2,
          firstBucketLabel: 'FIRST_LBL',
          currentBucketLabel: 'CURRENT_LBL',
          lastBucketLabel: 'LAST_LBL',
        })}
      />,
    )
    for (const lbl of ['FIRST_LBL', 'CURRENT_LBL', 'LAST_LBL']) {
      expect(screen.getByText(lbl)).toBeInTheDocument()
    }
    const slider = screen.getByTestId('mock-slider') as HTMLInputElement
    expect(slider).toHaveValue('2')
    expect(slider.min).toBe('0')
    expect(slider.max).toBe('9')
    fireEvent.change(slider, { target: { value: '5' } })
    expect(setBucketIdx).toHaveBeenCalledWith(5)
    expect(setPlaying).toHaveBeenCalledWith(false)
  })

  test('metric, bucketSeconds, and asn Selects fire their change callbacks', async () => {
    const user = userEvent.setup()
    const onMetricChange = vi.fn()
    const onBucketChange = vi.fn()
    const onAsnChange = vi.fn()
    render(<PlaybackControls {...defaultControlsProps({ onMetricChange, onBucketChange, onAsnChange })} />)

    expect(screen.getByText('All ASNs')).toBeInTheDocument()
    expect(screen.getByText('AS7922 Comcast')).toBeInTheDocument()
    expect(screen.getByText('AS15169 Google')).toBeInTheDocument()

    const findSelectBtn = (val: string) =>
      screen.getAllByRole('button').find((b) => b.getAttribute('data-select-item-value') === val)!

    await user.click(findSelectBtn('rtt_med_us'))
    expect(onMetricChange).toHaveBeenCalledWith('rtt_med_us')

    await user.click(findSelectBtn('300'))
    expect(onBucketChange).toHaveBeenCalledWith(300)
    expect(typeof onBucketChange.mock.calls[0][0]).toBe('number')

    await user.click(findSelectBtn('15169'))
    expect(onAsnChange).toHaveBeenCalledWith('15169')
  })

  test('METRIC_OPTIONS, SPEED_OPTIONS, and STEP_OPTIONS shapes', () => {
    expect(METRIC_OPTIONS.map((o) => o.value)).toEqual([
      'health_score', 'rtt_med_us', 'avg_ploss', 'error_pct', 'throughput_bps',
    ])
    expect(SPEED_OPTIONS).toHaveLength(4)
    const speedValues = SPEED_OPTIONS.map((o) => o.value)
    expect(speedValues).toEqual([...speedValues].sort((a, b) => b - a))

    expect(STEP_OPTIONS).toHaveLength(11)
    const stepValues = STEP_OPTIONS.map((o) => o.value)
    for (let i = 1; i < stepValues.length; i++) {
      expect(stepValues[i]).toBeGreaterThan(stepValues[i - 1])
    }
    expect(stepValues[0]).toBe(1)
    expect(stepValues[stepValues.length - 1]).toBe(14400)
  })
})

describe('ShieldingMap helpers', () => {
  it('greatCirclePoints handles identical endpoints and antimeridian wrap', () => {
    const same = greatCirclePoints(40, -74, 40, -74)
    expect(same).toEqual([[-74, 40], [-74, 40]])

    const pts = greatCirclePoints(0, -179, 0, 179)
    for (let i = 1; i < pts.length; i++) {
      expect(Math.abs(pts[i][0] - pts[i - 1][0])).toBeLessThanOrEqual(180)
    }
    expect(pts.length).toBeGreaterThan(2)
  })

  it.each([
    [null, '#6366f1'],
    [-0.5, '#6366f1'],
    [-10, '#6366f1'],
    [1.0, '#22c55e'],
    [1.49, '#22c55e'],
    [1.5, '#eab308'],
    [2.9, '#eab308'],
    [3.0, '#ef4444'],
    [10, '#ef4444'],
  ] as const)('efficiencyColor(%s) → %s', (ratio, expected) => {
    expect(efficiencyColor(ratio)).toBe(expected)
  })

  it('lineWidth clamps at [1.5, 6] and scales logarithmically between bounds', () => {
    expect(lineWidth(0)).toBe(1.5)
    expect(lineWidth(1)).toBe(1.5)
    expect(lineWidth(1_000_000_000)).toBe(6)
    expect(lineWidth(Number.MAX_SAFE_INTEGER)).toBe(6)
    const w10 = lineWidth(10)
    const w100 = lineWidth(100)
    expect(w10).toBeGreaterThan(1.5)
    expect(w10).toBeLessThan(6)
    expect(w100).toBeGreaterThan(w10)
  })

  describe('rafThrottle', () => {
    let rafCallbacks: FrameRequestCallback[]
    let originalRAF: typeof globalThis.requestAnimationFrame

    beforeEach(() => {
      rafCallbacks = []
      originalRAF = globalThis.requestAnimationFrame
      globalThis.requestAnimationFrame = ((cb: FrameRequestCallback) => {
        rafCallbacks.push(cb)
        return rafCallbacks.length
      }) as typeof globalThis.requestAnimationFrame
    })

    afterEach(() => { globalThis.requestAnimationFrame = originalRAF })

    it('coalesces multiple calls within a frame and re-queues on subsequent frames', () => {
      const fn = vi.fn()
      const throttled = rafThrottle(fn)
      throttled(1)
      throttled(2)
      throttled(3)
      expect(fn).not.toHaveBeenCalled()
      expect(rafCallbacks).toHaveLength(1)
      rafCallbacks[0](performance.now())
      expect(fn).toHaveBeenCalledTimes(1)
      expect(fn).toHaveBeenCalledWith(3)

      throttled(4)
      expect(rafCallbacks).toHaveLength(2)
      rafCallbacks[1](performance.now())
      expect(fn).toHaveBeenCalledTimes(2)
      expect(fn).toHaveBeenNthCalledWith(2, 4)
    })
  })

  describe('buildArcFeatures & buildDotFeatures', () => {
    it('builds valid LineString features, carries anomaly_static, gates low_sample, and skips invalid rows', () => {
      const fc = buildArcFeatures([
        {
          edge_lat: 40, edge_lon: -74, shield_lat: 51, shield_lon: 0,
          edge_pop: 'JFK', shield_pop: 'LON', requests: 100, p50_ms: 10, p95_ms: 50, p99_ms: 100,
          distance_km: 5000, light_speed_rtt_ms: 33, efficiency_ratio: 2.5, anomaly_static: true,
        },
        {
          edge_lat: 35, edge_lon: 139, shield_lat: 22, shield_lon: 114,
          edge_pop: 'NRT', shield_pop: 'HKG', requests: 3, p50_ms: 400, p95_ms: 800, p99_ms: 1200,
          distance_km: 2900, light_speed_rtt_ms: 19, efficiency_ratio: 12, anomaly_static: false, low_sample: true,
        },
        { edge_lat: 40, edge_lon: -74, shield_lat: null, shield_lon: null, requests: 10 },
        { edge_lat: null, edge_lon: null, shield_lat: 51, shield_lon: 0, requests: 10 },
        { edge_lat: 40, edge_lon: -74, shield_lat: 40, shield_lon: -74, requests: 10 },
      ])
      expect(fc.type).toBe('FeatureCollection')
      expect(fc.features).toHaveLength(2)
      expect(fc.features[0].geometry.type).toBe('LineString')
      expect(fc.features[0].properties).toMatchObject({ edge_pop: 'JFK', anomaly_static: true })
      expect(fc.features[0].properties?.line_width).toBeGreaterThan(1.5)
      expect(fc.features[1].properties).toMatchObject({ color: '#94a3b8', low_sample: true })
    })

    it('buildDotFeatures deduplicates POPs, omits "Direct to Origin", and handles missing coords', () => {
      const fc = buildDotFeatures([
        { edge_lat: 40, edge_lon: -74, edge_pop: 'JFK', shield_lat: 51, shield_lon: 0, shield_pop: 'LON' },
        { edge_lat: 40, edge_lon: -74, edge_pop: 'JFK', shield_lat: 35, shield_lon: 139, shield_pop: 'NRT' },
        { edge_lat: 40, edge_lon: -74, edge_pop: 'JFK', shield_lat: 0, shield_lon: 0, shield_pop: 'Direct to Origin' },
      ])
      expect(fc.features).toHaveLength(3)
      expect(fc.features.map((f) => f.properties?.role).sort()).toEqual(['edge', 'shield', 'shield'])
      expect(
        buildDotFeatures([{ edge_lat: null, edge_lon: null, shield_lat: null, shield_lon: null }]).features,
      ).toHaveLength(0)
    })
  })
})

describe('ShieldingMap component', () => {
  const sampleRows = [
    {
      edge_lat: 40, edge_lon: -74, shield_lat: 51, shield_lon: 0,
      edge_pop: 'JFK', shield_pop: 'LON', requests: 100, p50_ms: 10, p95_ms: 50, p99_ms: 100,
    },
  ]

  beforeEach(() => {
    useStrictSources = true
    mapInstances.length = 0
    stubWorldTopologyFetch()
  })

  afterEach(() => {
    useStrictSources = false
    vi.unstubAllGlobals()
  })

  it('mounts a map instance, registers sources, and calls map.remove() on unmount', async () => {
    const { unmount, queryByLabelText } = render(<ShieldingMap rows={sampleRows} />)
    await act(async () => { await Promise.resolve(); await Promise.resolve() })
    expect(mapInstances.length).toBe(1)
    expect(queryByLabelText(/expand map to fullscreen/i)).not.toBeInTheDocument()
    const instance = mapInstances[0]
    await waitFor(() => {
      expect(Object.keys(instance.sources)).toEqual(expect.arrayContaining(['world', 'arcs', 'dots']))
    })
    expect(instance.removed).toBe(false)
    unmount()
    expect(instance.removed).toBe(true)
  })

  it.each([
    [{ rows: [], edgeOnly: true }, /edge-only logging detected/i],
    [{ rows: [], isLoading: false }, /no shielding path data/i],
    [{ rows: [], isLoading: false, errored: true }, /shielding analysis unavailable/i],
    [
      {
        rows: [{ edge_pop: 'XYZ', shield_pop: 'ABC', edge_lat: null, edge_lon: null, shield_lat: null, shield_lon: null }],
        isLoading: false,
      },
      /POP coordinates unavailable/i,
    ],
  ] as const)('renders fallback state for %o', (props, pattern) => {
    const { getByText } = render(<ShieldingMap {...props} rows={[...props.rows]} />)
    expect(getByText(pattern)).toBeInTheDocument()
  })

  it('does not crash transitioning from loading to the empty state (Rules-of-Hooks regression)', async () => {
    const { rerender, getByText } = render(<ShieldingMap rows={[]} isLoading />)
    await act(async () => { await Promise.resolve(); await Promise.resolve() })
    rerender(<ShieldingMap rows={[]} isLoading={false} />)
    expect(getByText(/no shielding path data/i)).toBeInTheDocument()
  })

  it('opens a fullscreen dialog with a second map when Expand is clicked', async () => {
    const { getByLabelText, findByRole } = render(<ShieldingMap rows={sampleRows} expandable />)
    await act(async () => { await Promise.resolve(); await Promise.resolve() })
    expect(mapInstances.length).toBe(1)
    await act(async () => { fireEvent.click(getByLabelText(/expand map to fullscreen/i)) })
    const dialog = await findByRole('dialog')
    expect(dialog).toHaveTextContent(/Edge → Shield Transit Map/)
    await act(async () => { await Promise.resolve(); await Promise.resolve() })
    expect(mapInstances.length).toBe(2)
  })
})

describe('ChoroplethMap, MapPrewarm & useMapResize', () => {
  beforeEach(() => {
    mapInstances.length = 0
    failMapConstruction.mockReset().mockReturnValue(false)
  })

  it('mounts a map instance, handles empty/updated data, and does not invoke onCountryClick on mount', async () => {
    const handler = vi.fn()
    const { rerender, container } = render(<ChoroplethMap data={[]} onCountryClick={handler} />)
    expect(container.firstChild).not.toBeNull()
    rerender(<ChoroplethMap data={[{ country: 'US', count: 50 }, { country: 'GB', count: 5 }]} onCountryClick={handler} />)
    await Promise.resolve()
    expect(mapInstances.length).toBe(1)
    expect(handler).not.toHaveBeenCalled()
  })

  it('server-renders accessible country data without constructing a browser map', () => {
    const html = renderToString(<ChoroplethMap data={[{ country: 'US', count: 100 }]} />)
    expect(html).toContain('United States')
    expect(html).toContain('Browse countries')
    expect(mapInstances).toHaveLength(0)
  })

  it('keeps the country picker usable when WebGL initialization fails', () => {
    failMapConstruction.mockReturnValueOnce(true)
    const onCountryClick = vi.fn()
    render(<ChoroplethMap data={[{ country: 'US', count: 100 }]} onCountryClick={onCountryClick} />)
    expect(screen.getByText(/Interactive map unavailable/)).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Browse countries' }))
    const list = screen.getByRole('listbox')
    expect(list).toHaveFocus()
    fireEvent.keyDown(list, { key: 'Enter' })
    expect(onCountryClick).toHaveBeenCalledWith('US')
    expect(screen.getByRole('button', { name: 'Browse countries' })).toBeInTheDocument()
  })

  it('resets keyboard selection after row-count changes while keeping the open picker focused', () => {
    const onCountryClick = vi.fn()
    const data = [{ country: 'US', count: 100 }, { country: 'GB', count: 50 }, { country: 'CA', count: 5 }]
    const { rerender } = render(<ChoroplethMap data={data} onCountryClick={onCountryClick} />)
    fireEvent.click(screen.getByRole('button', { name: 'Browse countries' }))
    fireEvent.keyDown(screen.getByRole('listbox'), { key: 'End' })
    expect(screen.getByRole('option', { name: /Canada/ })).toHaveAttribute('aria-selected', 'true')
    rerender(<ChoroplethMap data={data.slice(0, 2)} onCountryClick={onCountryClick} />)
    expect(screen.getByRole('listbox')).toHaveFocus()
    expect(screen.getByRole('option', { name: /United States/ })).toHaveAttribute('aria-selected', 'true')
    fireEvent.keyDown(screen.getByRole('listbox'), { key: 'Enter' })
    expect(onCountryClick).toHaveBeenCalledWith('US')
  })

  it('bounds hover tooltip coordinates using the current map dimensions', async () => {
    render(<ChoroplethMap data={[{ country: 'US', count: 100 }]} />)
    await act(async () => { await Promise.resolve() })
    const instance = mapInstances[0]
    Object.defineProperties(instance.container as HTMLElement, {
      clientWidth: { value: 400 },
      clientHeight: { value: 200 },
    })
    act(() => instance.emit('mousemove', {
      features: [{ properties: { name: 'United States of America' } }],
      point: { x: 390, y: 190 },
    }))
    expect(screen.getByText('100 requests').parentElement).toHaveStyle({ left: '240px', top: '120px' })
  })

  it('MapPrewarm configures the worker before prewarming', async () => {
    constructedWithWorker.mockClear()
    const { unmount } = render(<MapPrewarm />)
    await waitFor(() => expect(constructedWithWorker).toHaveBeenCalledTimes(1))
    expect(constructedWithWorker).toHaveBeenCalledWith('/maplibre-gl-worker.mjs')
    unmount()
  })

  it('useMapResize calls map.resize() on resize, disconnects on unmount, and tolerates missing refs', () => {
    const ro = stubResizeObserver()
    const el = document.createElement('div')
    const resize = vi.fn()
    const { unmount } = renderHook(() => useMapResize({ current: { resize } }, { current: el }))
    expect(ro.observe).toHaveBeenCalledWith(el)
    ro.fire()
    expect(resize).toHaveBeenCalledTimes(1)
    unmount()
    expect(ro.disconnect).toHaveBeenCalledTimes(1)

    const ro2 = stubResizeObserver()
    renderHook(() => useMapResize({ current: null }, { current: document.createElement('div') }))
    expect(() => ro2.fire()).not.toThrow()

    const ro3 = stubResizeObserver()
    renderHook(() => useMapResize({ current: null }, { current: null }))
    expect(ro3.observe).not.toHaveBeenCalled()
  })
})

const VALID_DATA = {
  label: '1.2.3.4',
  client_lat: 48.8566,
  client_lon: 2.3522,
  pop_lat: 37.7749,
  pop_lon: -122.4194,
  pop: 'SFO',
  tcp_rtt: 20000,
  distance_km: 8960,
  max_km: 2000,
  city: 'Paris',
  country: 'FR',
}

describe('ImpossibleDistanceModal & InsightHelpModal', () => {
  test('renders modal with valid data, city/country overlay, fallback on missing coords, and null when data is null', () => {
    const { rerender, container } = render(<ImpossibleDistanceModal isOpen data={VALID_DATA} onOpenChange={vi.fn()} />)
    expect(screen.getByText(/Physics Violation/)).toBeInTheDocument()
    expect(screen.getAllByText(/8,960/).length).toBeGreaterThan(0)
    expect(screen.getAllByText(/Paris, FR/).length).toBeGreaterThan(0)

    rerender(<ImpossibleDistanceModal isOpen data={{ ...VALID_DATA, pop_lat: NaN, pop_lon: NaN }} onOpenChange={vi.fn()} />)
    expect(screen.getByText('Location data unavailable.')).toBeInTheDocument()

    rerender(<ImpossibleDistanceModal isOpen={false} data={null} onOpenChange={vi.fn()} />)
    expect(container).toBeEmptyDOMElement()
  })

  test('InsightHelpModal renders impossible_distance content, default fallback, and Escape triggers onOpenChange', async () => {
    const onChange = vi.fn()
    const { rerender } = render(<InsightHelpModal insightId="impossible_distance" isOpen onOpenChange={onChange} />)
    expect(screen.getByText(/The Physics of/)).toBeInTheDocument()
    expect(screen.getByText(/Speed of Light Violation Detected/)).toBeInTheDocument()
    fireEvent.keyDown(document, { key: 'Escape' })
    await waitFor(() => expect(onChange).toHaveBeenCalled())

    rerender(<InsightHelpModal insightId="unknown_insight_xyz" isOpen onOpenChange={vi.fn()} />)
    expect(screen.getByText('Insight Analysis')).toBeInTheDocument()
  })
})
