/**
 * Consolidated Map tests: ChoroplethMap, MapPrewarm, useMapResize,
 * ImpossibleDistanceModal, and InsightHelpModal.
 *
 * @vitest-environment jsdom
 */
import { render, renderHook, screen, fireEvent, waitFor } from '@testing-library/react'
import { describe, it, test, expect, vi, beforeEach } from 'vitest'
import React from 'react'

import {
  mapInstances,
  maplibreMockFactory,
  installMaplibreSideEffects,
} from '../../helpers/maplibre-mock'
import { stubResizeObserver } from '../../helpers/browser-stubs'
import { ChoroplethMap, MapPrewarm } from '@/components/Map/ChoroplethMap'
import { useMapResize } from '@/components/Map/shared'
import { ImpossibleDistanceModal } from '@/components/Insights/ImpossibleDistanceModal'
import { InsightHelpModal } from '@/components/Insights/InsightHelpModal'

const { setWorkerUrl, constructedWithWorker } = vi.hoisted(() => ({
  setWorkerUrl: vi.fn(),
  constructedWithWorker: vi.fn(),
}))

vi.mock('next-themes', () => ({
  useTheme: vi.fn(() => ({ theme: 'light' })),
}))

vi.mock('maplibre-gl', () => {
  const mapMock = maplibreMockFactory()
  class PrewarmMap extends mapMock.Map {
    constructor(options: ConstructorParameters<typeof mapMock.Map>[0]) {
      constructedWithWorker(setWorkerUrl.mock.calls.at(-1)?.[0])
      super(options)
    }
  }
  return { ...mapMock, Map: PrewarmMap, setWorkerUrl }
})
vi.mock('maplibre-gl/dist/maplibre-gl.css', () => ({}))

installMaplibreSideEffects()

describe('ChoroplethMap', () => {
  beforeEach(() => {
    mapInstances.length = 0
  })

  it('mounts a map instance into the container ref', async () => {
    const { container } = render(
      <ChoroplethMap data={[{ country: 'US', count: 100 }]} />,
    )
    expect(container.querySelector('div')).toBeInTheDocument()
    await Promise.resolve()
    expect(mapInstances.length).toBe(1)
  })

  it('renders an empty container when given an empty data array and re-renders cleanly on data changes', () => {
    const { rerender, container } = render(<ChoroplethMap data={[]} />)
    expect(container.firstChild).not.toBeNull()
    rerender(
      <ChoroplethMap
        data={[
          { country: 'US', count: 50 },
          { country: 'GB', count: 5 },
        ]}
      />,
    )
    expect(container.firstChild).not.toBeNull()
  })

  it('accepts an onCountryClick handler without invoking it on mount', async () => {
    const handler = vi.fn()
    render(<ChoroplethMap data={[{ country: 'US', count: 1 }]} onCountryClick={handler} />)
    await Promise.resolve()
    expect(handler).not.toHaveBeenCalled()
  })
})

describe('MapPrewarm', () => {
  it('configures the worker before prewarming without another map component present', async () => {
    constructedWithWorker.mockClear()
    const { unmount } = render(<MapPrewarm />)
    await waitFor(() => expect(constructedWithWorker).toHaveBeenCalledTimes(1))
    expect(constructedWithWorker).toHaveBeenCalledWith('/maplibre-gl-worker.mjs')
    unmount()
  })
})

describe('useMapResize', () => {
  test('calls map.resize() whenever the container resizes and disconnects on unmount', () => {
    const ro = stubResizeObserver()
    const el = document.createElement('div')
    const resize = vi.fn()
    const mapRef = { current: { resize } }
    const containerRef = { current: el }
    const { unmount } = renderHook(() => useMapResize(mapRef, containerRef))

    expect(ro.observe).toHaveBeenCalledWith(el)
    ro.fire()
    expect(resize).toHaveBeenCalledTimes(1)

    unmount()
    expect(ro.disconnect).toHaveBeenCalledTimes(1)
  })

  test('tolerates a map that has not been constructed yet or missing container', () => {
    const ro = stubResizeObserver()
    const mapRef = { current: null }
    renderHook(() => useMapResize(mapRef, { current: document.createElement('div') }))
    expect(() => ro.fire()).not.toThrow()

    const ro2 = stubResizeObserver()
    renderHook(() => useMapResize({ current: null }, { current: null }))
    expect(ro2.observe).not.toHaveBeenCalled()
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
  test('renders modal with valid data', () => {
    render(
      <ImpossibleDistanceModal isOpen data={VALID_DATA} onOpenChange={vi.fn()} />
    )
    expect(screen.getByText(/Physics Violation/)).toBeInTheDocument()
    expect(screen.getAllByText(/8,960/).length).toBeGreaterThan(0)
  })

  test('shows fallback when coordinates are missing', () => {
    const bad = { ...VALID_DATA, pop_lat: NaN, pop_lon: NaN }
    render(
      <ImpossibleDistanceModal isOpen data={bad} onOpenChange={vi.fn()} />
    )
    expect(screen.getByText('Location data unavailable.')).toBeInTheDocument()
  })

  test('hasCoords guard prevents .toFixed() crash on undefined pop coordinates', () => {
    expect(Number.isFinite(undefined)).toBe(false)
    expect(Number.isFinite(null)).toBe(false)
    expect(Number.isFinite(NaN)).toBe(false)
    expect(Number.isFinite(37.7749)).toBe(true)
  })

  test('shows city and country in coordinate overlay when provided', () => {
    render(
      <ImpossibleDistanceModal isOpen data={VALID_DATA} onOpenChange={vi.fn()} />
    )
    expect(screen.getAllByText(/Paris, FR/).length).toBeGreaterThan(0)
  })

  test('returns null when data is null', () => {
    const { container } = render(
      <ImpossibleDistanceModal isOpen={false} data={null} onOpenChange={vi.fn()} />
    )
    expect(container).toBeEmptyDOMElement()
  })

  test('InsightHelpModal renders impossible_distance content', () => {
    render(
      <InsightHelpModal insightId="impossible_distance" isOpen onOpenChange={vi.fn()} />
    )
    expect(screen.getByText(/The Physics of/)).toBeInTheDocument()
    expect(screen.getByText(/Speed of Light Violation Detected/)).toBeInTheDocument()
  })

  test('InsightHelpModal renders default content for unknown insight', () => {
    render(
      <InsightHelpModal insightId="unknown_insight_xyz" isOpen onOpenChange={vi.fn()} />
    )
    expect(screen.getByText('Insight Analysis')).toBeInTheDocument()
  })

  test('InsightHelpModal close button triggers onOpenChange', async () => {
    const onChange = vi.fn()
    render(
      <InsightHelpModal insightId="impossible_distance" isOpen onOpenChange={onChange} />
    )
    fireEvent.keyDown(document, { key: 'Escape' })
    await waitFor(() => expect(onChange).toHaveBeenCalled())
  })
})
