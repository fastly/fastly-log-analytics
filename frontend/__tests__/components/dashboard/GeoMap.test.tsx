/**
 * GeoMap is a thin presentational wrapper around a dynamic-imported
 * ChoroplethMap. The component branches between four states:
 *   - loading (no aggregates yet)
 *   - empty (aggregates loaded, but map_data is missing/empty)
 *   - populated (delegates to ChoroplethMap)
 *   - "fetching but stale data" (passthrough)
 *
 * These tests pin: (1) the loading skeleton renders when aggregates are
 * undefined, (2) the empty branch shows a helpful message tied to the
 * Geolocation field group, (3) the ChoroplethMap is mounted with the
 * map_data + click handler when data is present, and (4) clicking
 * the country forwards to `onCountryClick`.
 *
 * @vitest-environment jsdom
 */
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import React from 'react'

// next/dynamic returns the loaded module after the promise resolves.
// In tests we collapse it to a synchronous passthrough so the
// ChoroplethMap mock below renders on first paint.
vi.mock('next/dynamic', () => ({
  default: (loader: any) => {
    const Comp = (props: any) => {
      const [C, setC] = React.useState<any>(null)
      React.useEffect(() => {
        let alive = true
        Promise.resolve(loader()).then((mod: any) => {
          if (!alive) return
          setC(() => (mod?.default ?? mod))
        })
        return () => {
          alive = false
        }
      }, [])
      if (!C) return null
      return React.createElement(C, props)
    }
    Comp.displayName = 'DynamicMock'
    return Comp
  },
}))

// useActiveLogFields chains useBootstrap + useLogFieldsCatalog (both react-query),
// which would need a QueryClientProvider. Mock it instead — GeoMap's contract
// here is "show the catalog/'Requires Geolocation' hint when `country` is NOT in
// the active log format, and a neutral 'No country data in this time range.' when
// it IS". Default to no active fields so the existing not-enabled assertions
// exercise the hint path; the neutral-branch test adds 'country' to `activeFields`.
const { activeFields } = vi.hoisted(() => ({ activeFields: new Set<string>() }))
vi.mock('@/hooks/useActiveLogFields', () => ({
  useActiveLogFields: () => ({
    ready: true,
    isFieldActive: (id: string) => activeFields.has(id),
    isGroupActive: () => false,
  }),
}))

vi.mock('@/components/Map/ChoroplethMap', () => ({
  ChoroplethMap: ({ data, onCountryClick }: { data: any[]; onCountryClick: (c: string) => void }) => (
    <div data-testid="choropleth-map">
      <span data-testid="marker-count">{data?.length ?? 0}</span>
      {(data ?? []).map((row: any) => (
        <button
          key={row.country ?? row.name}
          type="button"
          data-testid={`country-${row.country ?? row.name}`}
          onClick={() => onCountryClick(row.country ?? row.name)}
        >
          {row.country ?? row.name}
        </button>
      ))}
    </div>
  ),
}))

import { GeoMap } from '@/app/dashboard/_sections/GeoMap'

const catalog = {
  fields: [{ id: 'country', label: 'Country', group: 'GEO' }],
  groups: [{ id: 'GEO', label: 'Geolocation' }],
}

describe('GeoMap', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    activeFields.clear()
  })

  it('renders the loading skeleton when aggregates are undefined', () => {
    render(
      <GeoMap
        isReady={false}
        isLoadingAggs={true}
        isFetchingAggs={false}
        aggregates={undefined}
        catalog={catalog}
        onCountryClick={vi.fn()}
      />,
    )
    expect(screen.getByText('Requests by Country')).toBeInTheDocument()
    // !isReady branch wins → "Initializing..." not "Mapping traffic..."
    expect(screen.getByText('Initializing...')).toBeInTheDocument()
    expect(screen.queryByTestId('choropleth-map')).toBeNull()
  })

  it('shows the empty state with the catalog group label when map_data is empty', () => {
    render(
      <GeoMap
        isReady={true}
        isLoadingAggs={false}
        isFetchingAggs={false}
        aggregates={{ map_data: [] }}
        catalog={catalog}
        onCountryClick={vi.fn()}
      />,
    )
    expect(screen.getByText('No data available')).toBeInTheDocument()
    // The dynamic group lookup resolves to "Geolocation" via the catalog.
    expect(
      screen.getByText(/Requires Geolocation fields to be enabled in Fastly logging\./),
    ).toBeInTheDocument()
    expect(screen.queryByTestId('choropleth-map')).toBeNull()
  })

  it('shows a neutral "no data in this range" state when the country field IS active', () => {
    // Enabled-but-empty: `country` is in the active log format, so an empty
    // map_data means "no data in this window", NOT "Geolocation misconfigured".
    activeFields.add('country')
    render(
      <GeoMap
        isReady={true}
        isLoadingAggs={false}
        isFetchingAggs={false}
        aggregates={{ map_data: [] }}
        catalog={catalog}
        onCountryClick={vi.fn()}
      />,
    )
    expect(screen.getByText('No data available')).toBeInTheDocument()
    expect(screen.getByText('No country data in this time range.')).toBeInTheDocument()
    expect(screen.queryByText(/Requires Geolocation/)).toBeNull()
    expect(screen.queryByTestId('choropleth-map')).toBeNull()
  })

  it('renders ChoroplethMap with one marker per map_data row when populated', async () => {
    const map_data = [
      { country: 'US', name: 'United States', requests: 100 },
      { country: 'DE', name: 'Germany', requests: 40 },
      { country: 'JP', name: 'Japan', requests: 25 },
    ]
    render(
      <GeoMap
        isReady={true}
        isLoadingAggs={false}
        isFetchingAggs={false}
        aggregates={{ map_data }}
        catalog={catalog}
        onCountryClick={vi.fn()}
      />,
    )
    // Dynamic-imported via next/dynamic — wait for the inner module
    // to resolve and render.
    await waitFor(() => expect(screen.getByTestId('choropleth-map')).toBeInTheDocument())
    expect(screen.getByTestId('marker-count').textContent).toBe('3')
  })

  it('forwards country clicks to onCountryClick', async () => {
    const onCountryClick = vi.fn()
    render(
      <GeoMap
        isReady={true}
        isLoadingAggs={false}
        isFetchingAggs={false}
        aggregates={{ map_data: [{ country: 'US', name: 'United States', requests: 1 }] }}
        catalog={catalog}
        onCountryClick={onCountryClick}
      />,
    )
    await waitFor(() => expect(screen.getByTestId('country-US')).toBeInTheDocument())
    fireEvent.click(screen.getByTestId('country-US'))
    expect(onCountryClick).toHaveBeenCalledWith('US')
  })

  it('falls back to the generic Geolocation hint when catalog is missing', () => {
    render(
      <GeoMap
        isReady={true}
        isLoadingAggs={false}
        isFetchingAggs={false}
        aggregates={{ map_data: [] }}
        catalog={undefined}
        onCountryClick={vi.fn()}
      />,
    )
    expect(
      screen.getByText('Requires Geolocation fields to be enabled in Fastly logging.'),
    ).toBeInTheDocument()
  })
})
