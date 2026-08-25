import { render, screen } from '@testing-library/react'
import { describe, test, expect, vi } from 'vitest'
import React from 'react'

// Isolate the presentational overlay (loading / error / empty) from the
// MapLibre canvas: useMapInit/useMapData touch WebGL + browser APIs jsdom
// can't provide, so stub the GL layer and render only the React shell.
vi.mock('maplibre-gl', () => ({
  default: { setWorkerUrl: () => {} },
  setWorkerUrl: () => {},
}))
vi.mock('@/components/Map/NetworkMap/MapLayer', () => ({
  formatBucket: (b: string) => String(b),
  useMapInit: () => {},
  useMapData: () => {},
}))
vi.mock('@/components/Map/NetworkMap/controls', () => ({
  PlaybackControls: () => <div data-testid="playback-controls" />,
}))
vi.mock('@/components/Map/NetworkMap/OverlayLayer', () => ({
  MapTooltip: () => null,
}))
vi.mock('next-themes', () => ({ useTheme: () => ({ theme: 'light' }) }))
vi.mock('@/stores/timezoneStore', () => ({ useTimezoneStore: () => ({ timezone: 'UTC' }) }))

import { NetworkMap } from '@/components/Map/NetworkMap'

const baseProps = {
  metric: 'health_score',
  onMetricChange: () => {},
  bucketSeconds: 5,
  onBucketChange: () => {},
  mapAsn: 'all',
  onAsnChange: () => {},
  asnOptions: [],
}

describe('NetworkMap failure + empty states', () => {
  // UX-3: a cold /network-health map 5xx leaves heatmapData null, so the
  // heatmap card (the only other error-carrying surface) unmounts. The map
  // must therefore surface its OWN error rather than the misleading
  // "No map data available" empty copy.
  test('UX-3: shows a distinct error state, not "No map data available", when error is set', () => {
    render(<NetworkMap data={undefined} error={new Error('map boom')} {...baseProps} />)
    expect(screen.getByRole('alert')).toHaveTextContent(/failed to load map data/i)
    expect(screen.queryByText('No map data available')).toBeNull()
  })

  // UX-8: data?.buckets.length (no optional chain on buckets) threw when the
  // response had no `buckets`. The empty state must render without throwing.
  test('UX-8: renders the empty state without throwing when buckets are absent', () => {
    expect(() =>
      render(<NetworkMap data={{ available: true }} error={null} {...baseProps} />),
    ).not.toThrow()
    expect(screen.getByText('No map data available')).toBeInTheDocument()
  })
})
