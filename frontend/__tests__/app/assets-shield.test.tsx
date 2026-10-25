import { screen, waitFor, within } from '@testing-library/react'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
import AssetsClient, { AssetsReportContent } from '@/app/assets-shield/_sections/AssetsClient'
import { createTestQueryClient, renderWithQuery } from '../helpers/query'
import React from 'react'
import { spyOnConsoleError } from '../helpers/page-smoke'

vi.mock('@/stores/serviceStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())

vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).filterStoreModuleMock())

vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/browser-stubs')).reactVirtualModuleMock())

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/assets-shield'))

const mockPost = vi.fn()
const mockGet = vi.fn()

vi.mock('@/lib/api', () => ({
  client: {
    GET: (...args: unknown[]) => mockGet(...args),
    POST: (...args: unknown[]) => mockPost(...args),
    use: vi.fn(),
  },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/components/ReportLayout', async () =>
  (await import('../helpers/page-smoke')).reportLayoutModuleMock({
    startTime: '2026-08-19T12:00:00Z',
    endTime: '2026-08-19T13:00:00Z',
    activeServiceId: 'test-service-id',
    filterPayload: {},
    intervalButtons: null,
    bucketSeconds: 3600,
  }),
)

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })

let errorSpy: ReturnType<typeof spyOnConsoleError>

beforeEach(() => {
  queryClient.clear()
  errorSpy = spyOnConsoleError()
  mockPost.mockReset()
  mockGet.mockReset()
})

afterEach(() => {
  errorSpy.mockRestore()
})

const MOCK_ASSETS_DATA = {
  has_data: true,
  missing_fields: [],
  asset_type_breakdown: [
    {
      asset_type: 'Images',
      requests: 50000,
      egress_bytes: 250000000,
      cache_hit_ratio: 0.945,
      compression_rate: 0.85,
    },
    {
      asset_type: 'JavaScript/CSS',
      requests: 30000,
      egress_bytes: 60000000,
      cache_hit_ratio: 0.982,
      compression_rate: 0.92,
    },
  ],
  cache_performance: [
    {
      asset_type: 'Images',
      cache_status: 'HIT',
      requests: 47250,
      bytes: 236250000,
    },
    {
      asset_type: 'Images',
      cache_status: 'MISS',
      requests: 2750,
      bytes: 13750000,
    },
  ],
  compression_performance: [
    {
      asset_type: 'JavaScript/CSS',
      content_encoding: 'gzip',
      requests: 20000,
      bytes: 40000000,
    },
    {
      asset_type: 'JavaScript/CSS',
      content_encoding: 'br',
      requests: 7600,
      bytes: 15200000,
    },
  ],
  large_uncompressed_assets: [
    {
      url: '/static/bundle-huge.js',
      requests: 1200,
      avg_bytes: 3750.0,
      total_bytes: 4500000,
      status: 200,
    },
  ],
  low_ttl_assets: [
    {
      url: '/feed.json',
      requests: 3500,
      avg_ttl: 60.0,
      asset_type: 'API/Dynamic',
    },
  ],
}

test('assets-shield page mounts and renders title and layout shell', async () => {
  mockPost.mockResolvedValue({ data: MOCK_ASSETS_DATA })

  renderWithQuery(<AssetsClient />, { queryClient })

  expect(screen.getByText('Assets & Shield')).toBeInTheDocument()
  expect(errorSpy).not.toHaveBeenCalled()
})

test('assets-shield renders zero-CLS layout reservations and contextual skeletons during loading', async () => {
  mockPost.mockReturnValue(new Promise(() => {})) // Never resolves to keep loading state

  renderWithQuery(<AssetsClient />, { queryClient })

  // Verify zero-CLS card containers exist
  const overviewGrid = screen.getByTestId('assets-overview-grid')
  expect(overviewGrid).toBeInTheDocument()
  expect(overviewGrid.className).toContain('[contain-intrinsic-size:160px]')
  expect(overviewGrid.className).toContain('[content-visibility:auto]')

  const cacheCard = screen.getByTestId('assets-cache-efficiency-card')
  expect(cacheCard).toBeInTheDocument()
  expect(cacheCard.className).toContain('[contain-intrinsic-size:420px]')
  expect(cacheCard.className).toContain('[content-visibility:auto]')

  const compressionCard = screen.getByTestId('assets-compression-card')
  expect(compressionCard).toBeInTheDocument()
  expect(compressionCard.className).toContain('[contain-intrinsic-size:420px]')
  expect(compressionCard.className).toContain('[content-visibility:auto]')

  // Verify contextual in-place loading messages
  expect(screen.getByText('Evaluating cache absorption efficiency...')).toBeInTheDocument()
  expect(screen.getByText('Evaluating payload compression efficiency...')).toBeInTheDocument()
})

test('assets-shield renders full data: breakdown, cache performance, compression, and hotspots', async () => {
  mockPost.mockResolvedValue({ data: MOCK_ASSETS_DATA })

  renderWithQuery(<AssetsClient />, { queryClient })

  expect(await screen.findByText('/static/bundle-huge.js')).toBeInTheDocument()
  expect(screen.getByText('/feed.json')).toBeInTheDocument()
  expect(screen.getByTestId('assets-cache-efficiency-card')).toBeInTheDocument()
  expect(screen.getByTestId('assets-compression-card')).toBeInTheDocument()
  expect(screen.getByTestId('assets-uncompressed-card')).toBeInTheDocument()
  const overviewGrid = screen.getByTestId('assets-overview-grid')
  expect(within(overviewGrid).getByText('Images')).toBeInTheDocument()
  expect(within(overviewGrid).getByText('JavaScript/CSS')).toBeInTheDocument()
  expect(errorSpy).not.toHaveBeenCalled()
})

test('assets-shield renders missing fields banner when optional fields are missing', async () => {
  mockPost.mockResolvedValue({
    data: {
      ...MOCK_ASSETS_DATA,
      missing_fields: ['resp_header_content_encoding', 'ttl'],
    },
  })

  renderWithQuery(<AssetsClient />, { queryClient })

  const banner = await screen.findByTestId('missing-asset-fields-banner')
  expect(banner).toBeInTheDocument()
  expect(within(banner).getByText(/Optional Log Fields Missing/i)).toBeInTheDocument()
  expect(within(banner).getByText(/resp_header_content_encoding, ttl/i)).toBeInTheDocument()

  const configLink = within(banner).getByRole('link', { name: /Configure Service Log Fields/i })
  expect(configLink).toBeInTheDocument()
  expect(configLink.getAttribute('href')).toContain('/service-config')
  expect(errorSpy).not.toHaveBeenCalled()
})

test('assets-shield renders empty state when asset breakdown has no data', async () => {
  mockPost.mockResolvedValue({
    data: {
      has_data: false,
      missing_fields: [],
      asset_type_breakdown: [],
      cache_performance: [],
      compression_performance: [],
      large_uncompressed_assets: [],
      low_ttl_assets: [],
    },
  })

  renderWithQuery(<AssetsClient />, { queryClient })

  await waitFor(() => {
    expect(screen.getByText('No Asset Metrics Yet')).toBeInTheDocument()
  })
  expect(
    screen.getByText(/No static assets or dynamic pages were requested in this time window/i),
  ).toBeInTheDocument()
  expect(errorSpy).not.toHaveBeenCalled()
})

test('assets-shield refetches when only the custom range start changes', async () => {
  mockPost.mockResolvedValue({ data: MOCK_ASSETS_DATA })
  const base = {
    endTime: '2026-08-19T13:00:00Z',
    timezone: 'UTC',
    activeServiceId: 'test-service-id',
    filterPayload: {},
    config: {},
    relativeRange: null,
    isAutoRange: false,
    anchor: '2026-08-19T13:00:00Z',
  }
  const ui = (startTime: string) => <AssetsReportContent {...base} startTime={startTime} />

  const { rerender } = renderWithQuery(ui('2026-08-19T12:00:00Z'), { queryClient })
  await waitFor(() => expect(mockPost).toHaveBeenCalledTimes(1))

  rerender(ui('2026-08-19T11:00:00Z'))
  await waitFor(() => expect(mockPost).toHaveBeenCalledTimes(2))
  expect(mockPost.mock.calls[1][1].body.start_time).toBe('2026-08-19T11:00:00Z')
})

test('assets-shield first paint hits the SSR-seeded cache entry (no refetch)', async () => {
  const { resolveRangeWire, quantizeAnchor } = await import('@/lib/range-wire')
  const anchor = quantizeAnchor('2026-01-01T01:00:00Z')
  const { rangeKey } = resolveRangeWire({
    relativeRange: null,
    isAutoRange: false,
    startTime: '2026-08-19T12:00:00Z',
    endTime: '2026-08-19T13:00:00Z',
    anchor,
  })
  const seedClient = createTestQueryClient({ queries: { staleTime: 60_000 } })
  seedClient.setQueryData(['assets', 'aggregates', 'test-service-id', rangeKey, anchor, {}], MOCK_ASSETS_DATA)
  mockPost.mockReturnValue(new Promise(() => {}))

  renderWithQuery(
    <AssetsReportContent
      startTime="2026-08-19T12:00:00Z"
      endTime="2026-08-19T13:00:00Z"
      activeServiceId="test-service-id"
      filterPayload={{}}
      relativeRange={null}
      isAutoRange={false}
      anchor={anchor}
    />,
    { queryClient: seedClient },
  )

  expect(await screen.findByText('/static/bundle-huge.js')).toBeInTheDocument()
  expect(mockPost).not.toHaveBeenCalled()
})

test('assets-shield renders the uncompressed and low-TTL tables with encoded drill-down links', async () => {
  mockPost.mockResolvedValue({ data: MOCK_ASSETS_DATA })

  renderWithQuery(<AssetsClient />, { queryClient })

  const link = await screen.findByRole('link', { name: '/static/bundle-huge.js' })
  expect(link.getAttribute('href')).toContain('filter_url=%2Fstatic%2Fbundle-huge.js')
  for (const name of ['Static Asset URL', 'Requests', 'Avg Size', 'Bandwidth', 'Type', 'Cache Misses', 'Avg TTL']) {
    expect(screen.getAllByRole('columnheader', { name }).length).toBeGreaterThan(0)
  }
  expect(screen.getByText('60.0s')).toBeInTheDocument()
})
