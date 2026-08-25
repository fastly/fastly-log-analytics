/**
 * NetworkMap is the top-level container that composes the
 * MapLibre canvas (via useMapInit/useMapData), the PlaybackControls
 * overlay, and a portal-rendered MapTooltip. It owns the bucket-index
 * state machine: auto-advance on a setInterval when ``playing`` is
 * true, and a reset-to-last-bucket effect any time ``data.buckets``
 * changes by reference.
 *
 * The component was 0%-covered. Jsdom cannot host real maplibre-gl,
 * so we stub the maplibre module, the MapLayer hooks, and the
 * PlaybackControls + MapTooltip subcomponents — leaving only the
 * NetworkMap orchestration logic under test (empty-state branches,
 * playback interval, idx-reset effect, and the createPortal tooltip).
 *
 * @vitest-environment jsdom
 */
import React from 'react'
import { act, render, screen } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

// ---------------------------------------------------------------------------
// Mocks must be declared before importing the component under test.

vi.mock('next-themes', () => ({
  useTheme: () => ({ theme: 'light' }),
}))

vi.mock('@/stores/timezoneStore', () => ({
  useTimezoneStore: () => ({ timezone: 'UTC' }),
}))

// maplibre-gl is a heavy module that touches WebGL / canvas; jsdom can't run
// it. Stub the Map constructor so ``useRef<maplibregl.Map>`` keeps typing
// happy and any reachable methods (the real ones are only invoked from
// useMapInit/useMapData, both mocked below) are no-ops.
vi.mock('maplibre-gl', () => {
  class MockMap {
    on() {}
    off() {}
    remove() {}
    addSource() {}
    addLayer() {}
    removeLayer() {}
    removeSource() {}
    getSource() {}
    getLayer() {}
    setFeatureState() {}
    setPaintProperty() {}
    setLayoutProperty() {}
    setFilter() {}
    setStyle() {}
    isStyleLoaded() {
      return true
    }
    queryRenderedFeatures() {
      return []
    }
    getCanvas() {
      return { style: {} }
    }
    project() {
      return { x: 0, y: 0 }
    }
    fitBounds() {}
    setMaxBounds() {}
    addControl() {}
  }
  return {
    default: { Map: MockMap, NavigationControl: class {}, LngLatBounds: class {}, setWorkerUrl: () => {} },
    Map: MockMap,
    NavigationControl: class {},
    LngLatBounds: class {},
    setWorkerUrl: () => {},
  }
})

// CSS side-effect import — vite throws if unmocked under vitest.
vi.mock('maplibre-gl/dist/maplibre-gl.css', () => ({}))

// ---------------------------------------------------------------------------
// Stub the sibling MapLayer module. We keep ``formatBucket`` callable so the
// component's "first/current/last bucket label" computation doesn't crash,
// but useMapInit / useMapData become no-op hooks. The test for tooltip
// rendering captures the setTooltip callback the component passes to
// useMapData so it can drive the portal branch directly.
const mapDataCalls: Array<{ setTooltip: (t: any) => void }> = []

vi.mock('@/components/Map/NetworkMap/MapLayer', () => ({
  formatBucket: (iso: string) => (iso ? `LBL(${iso})` : ''),
  useMapInit: () => {},
  useMapData: ({ setTooltip }: { setTooltip: (t: any) => void }) => {
    mapDataCalls.push({ setTooltip })
  },
}))

// PlaybackControls becomes a tiny probe that surfaces the props the
// container passed in so we can assert against them without depending on the
// real Select/Slider implementation. The latest props object is stashed in a
// module-scoped ref so tests can drive setPlaying / inspect bucketIdx.
const playbackProps: { current: Record<string, any> | null } = { current: null }

vi.mock('@/components/Map/NetworkMap/controls', () => ({
  PlaybackControls: (props: Record<string, any>) => {
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
}))

// MapTooltip stub: assert it lands in document.body via createPortal by
// rendering a marker with the metric prop.
vi.mock('@/components/Map/NetworkMap/OverlayLayer', () => ({
  MapTooltip: ({ info, metric }: { info: any; metric: string }) => (
    <div data-testid="map-tooltip" data-metric={metric}>
      {info?.city ?? ''}
    </div>
  ),
}))

// jsdom lacks ResizeObserver; some downstream deps assume it exists.
class MockResizeObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
}
;(globalThis as any).ResizeObserver = (globalThis as any).ResizeObserver || MockResizeObserver

// Import AFTER mocks are registered.
import { NetworkMap } from '@/components/Map/NetworkMap'

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
})

describe('NetworkMap with data', () => {
  it('renders PlaybackControls and forwards key props when buckets exist', () => {
    const data = { buckets: ['2026-01-01T00:00:00', '2026-01-01T00:01:00', '2026-01-01T00:02:00'] }
    render(<NetworkMap {...baseProps({ data })} />)

    const ctrl = screen.getByTestId('playback-controls')
    expect(ctrl).toBeInTheDocument()
    // The reset-on-new-data effect synchronously moves bucketIdx to the last
    // index (length-1 = 2). PlaybackControls receives that as bucketIdx.
    expect(ctrl.getAttribute('data-bucket-idx')).toBe('2')
    expect(ctrl.getAttribute('data-buckets-length')).toBe('3')
    expect(ctrl.getAttribute('data-playing')).toBe('false')
    expect(ctrl.getAttribute('data-metric')).toBe('health_score')
    // formatBucket stub wraps the iso string — current label = LBL(<bucket[2]>).
    expect(ctrl.getAttribute('data-current-label')).toBe('LBL(2026-01-01T00:02:00)')

    // No empty-state copy.
    expect(screen.queryByText('Loading map data...')).not.toBeInTheDocument()
    expect(screen.queryByText('No map data available')).not.toBeInTheDocument()
  })
})

describe('NetworkMap auto-play', () => {
  it('increments bucketIdx on the playInterval when playing flips to true', () => {
    // Two buckets so the index toggles 1 → 0 → 1 → 0 ... predictably.
    const data = { buckets: ['t0', 't1'] }

    vi.useFakeTimers()
    render(<NetworkMap {...baseProps({ data })} />)

    // Initial: bucketIdx = last index = 1.
    expect(screen.getByTestId('playback-controls').getAttribute('data-bucket-idx')).toBe('1')
    expect(playbackProps.current).not.toBeNull()

    // Flip playing → true via the captured setPlaying prop. The interval
    // (default 100ms) will then advance bucketIdx on every tick:
    // 1 → (1+1)%2 = 0 → (0+1)%2 = 1 ...
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
    const data1 = { buckets: ['a', 'b', 'c', 'd', 'e'] } // length 5 → idx 4
    const { rerender } = render(<NetworkMap {...baseProps({ data: data1 })} />)
    expect(screen.getByTestId('playback-controls').getAttribute('data-bucket-idx')).toBe('4')

    // New buckets array (different reference + different length) triggers the
    // reset-effect, which pins idx to (length-1).
    const data2 = { buckets: ['x', 'y'] } // length 2 → idx 1
    rerender(<NetworkMap {...baseProps({ data: data2 })} />)
    expect(screen.getByTestId('playback-controls').getAttribute('data-bucket-idx')).toBe('1')

    // Empty-but-defined buckets array: effect runs (truthy), idx = 0.
    const data3 = { buckets: [] as string[] }
    rerender(<NetworkMap {...baseProps({ data: data3 })} />)
    // length === 0 falls through to the !data?.buckets?.length branch
    // (length 0 is falsy), so PlaybackControls is unmounted and the
    // empty-state overlay takes its place.
    expect(screen.queryByTestId('playback-controls')).not.toBeInTheDocument()
    expect(screen.getByText('No map data available')).toBeInTheDocument()
  })
})

describe('NetworkMap tooltip portal', () => {
  it('renders MapTooltip into document.body when setTooltip receives an info object', () => {
    const data = { buckets: ['t0', 't1', 't2'] }
    render(<NetworkMap {...baseProps({ data, metric: 'rtt_med_us' })} />)

    // useMapData was invoked with the component's setTooltip. Drive it.
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
    // Portal target is document.body — verify by walking up the DOM.
    expect(tooltip.parentElement).toBe(document.body)

    // Setting tooltip back to null removes it.
    act(() => {
      lastCall.setTooltip(null)
    })
    expect(screen.queryByTestId('map-tooltip')).not.toBeInTheDocument()
  })
})
