import React from 'react'
import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { GeographyCard, type GeographyGroup } from '@/app/usage/_sections/GeographyCard'
import { renderWithQuery } from '../helpers/page-smoke'
import type { components } from '@/types/api.generated'

const plotly = vi.hoisted(() => vi.fn())

vi.mock('@/components/PlotlyChart', () => ({
  PlotlyChart: (props: {
    data: Array<{ x: number[]; y: string[] }>
    a11yTitle?: string
  }) => {
    plotly(props)
    return <div data-testid="geography-chart">{props.a11yTitle}</div>
  },
}))

type GeographyResponse = components['schemas']['PlatformGeographyResponse']

const response = (
  group_by: GeographyGroup,
  data: GeographyResponse['data'] = [],
  source: GeographyResponse['source'] = 'fastly',
): GeographyResponse => ({
  source,
  group_by,
  data,
  requests: data.reduce((sum, row) => sum + row.requests, 0),
  bandwidth_bytes: data.reduce((sum, row) => sum + row.bandwidth_bytes, 0),
  _is_cached: false,
})

const startTime = '2026-11-01T00:00:00.000Z'
const endTime = '2026-11-02T00:00:00.000Z'

function renderCard(fetcher: (groupBy: GeographyGroup) => Promise<GeographyResponse>) {
  return renderWithQuery(
    <GeographyCard
      activeServiceId="svc-1"
      startTime={startTime}
      endTime={endTime}
      fetchGeography={fetcher}
    />,
  )
}

describe('Usage geography', () => {
  beforeEach(() => {
    plotly.mockClear()
  })

  it('defaults to region and fetches datacenter only when selected', async () => {
    const fetcher = vi.fn(async (groupBy: GeographyGroup) => response(groupBy, [
      {
        location: groupBy === 'region' ? 'Europe' : 'AMS',
        requests: 7,
        bandwidth_bytes: 700,
        request_share: 0.7,
        bandwidth_share: 0.7,
      },
      {
        location: groupBy === 'region' ? 'USA' : 'IAD',
        requests: 3,
        bandwidth_bytes: 300,
        request_share: 0.3,
        bandwidth_share: 0.3,
      },
    ]))

    renderCard(fetcher)

    await waitFor(() => expect(fetcher).toHaveBeenCalledWith('region'))
    expect(fetcher).not.toHaveBeenCalledWith('datacenter')
    expect(await screen.findByText('10 total requests')).toBeInTheDocument()

    await userEvent.click(screen.getByRole('button', { name: 'Datacenter' }))

    await waitFor(() => expect(fetcher).toHaveBeenCalledWith('datacenter'))
    expect(fetcher.mock.calls.map(([group]) => group)).toEqual(['region', 'datacenter'])
  })

  it('sorts the chart by the selected measure and exposes every row, share and total', async () => {
    const fetcher = vi.fn(async () => response('region', [
      {
        location: 'Alpha',
        requests: 9,
        bandwidth_bytes: 100,
        request_share: 0.9,
        bandwidth_share: 0.1,
      },
      {
        location: 'Beta',
        requests: 1,
        bandwidth_bytes: 900,
        request_share: 0.1,
        bandwidth_share: 0.9,
      },
    ]))

    renderCard(fetcher)

    await screen.findByRole('row', { name: /Alpha 9/ })
    expect(plotly.mock.calls.at(-1)?.[0].data[0]).toMatchObject({
      x: [9, 1],
      y: ['Alpha', 'Beta'],
      orientation: 'h',
    })
    expect(screen.getByRole('row', { name: /Alpha 9 90\.0% 100 B 10\.0%/ })).toBeInTheDocument()
    expect(screen.getByRole('row', { name: /Total 10 100\.0% 0\.98 KB 100\.0%/ })).toBeInTheDocument()

    await userEvent.click(screen.getByRole('button', { name: 'Bandwidth' }))

    await waitFor(() => expect(plotly.mock.calls.at(-1)?.[0].data[0]).toMatchObject({
      x: [900, 100],
      y: ['Beta', 'Alpha'],
    }))
  })

  it('keeps loading, empty and unavailable states independent from billing cards', async () => {
    const pending = new Promise<GeographyResponse>(() => {})
    const { unmount } = renderCard(() => pending)
    expect(screen.getByText('Loading traffic geography...')).toBeInTheDocument()
    unmount()

    const empty = renderCard(async (group) => response(group))
    expect(await screen.findByText('No data for this filter')).toBeInTheDocument()
    empty.unmount()

    renderCard(async (group) => response(group, [], 'unavailable'))
    expect(await screen.findByText('Geography unavailable')).toBeInTheDocument()
    expect(screen.queryByText(/cost/i)).not.toBeInTheDocument()
  })
})
