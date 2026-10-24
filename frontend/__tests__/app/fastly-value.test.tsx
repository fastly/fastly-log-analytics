import { render, screen } from '@testing-library/react'
import { beforeAll, beforeEach, afterEach, describe, expect, it, test, vi } from 'vitest'
import FastlyValueClient from '@/app/fastly-value/page'
import IOValueTab from '@/app/fastly-value/_sections/tabs/IOValueTab'
import type { components } from '@/types/api.generated'
import { stubResizeObserver } from '@/__tests__/helpers/browser-stubs'
import { createTestQueryClient, renderWithQuery } from '../helpers/query'
import React from 'react'
import { spyOnConsoleError } from '../helpers/page-smoke'

// Render-smoke for the Fastly Value / Service Summary page + IOValueTab tables.

vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/virtual-mock')).reactVirtualModuleMock())
vi.mock('@/stores/serviceStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())
vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).filterStoreModuleMock())
vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/fastly-value'))

vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(), POST: vi.fn().mockResolvedValue({ data: {} }), use: vi.fn() },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/components/PlotlyChart', async () => (await import('../helpers/page-smoke')).plotlyChartModuleMock())
vi.mock('@/components/FilterValueCell', () => ({
  FilterValueCell: ({ display }: { display: string }) => <span>{display}</span>,
}))

vi.mock('@/components/ReportLayout', async () =>
  (await import('../helpers/page-smoke')).reportLayoutModuleMock({
    startTime: '2026-01-01T00:00:00Z',
    endTime: '2026-01-01T01:00:00Z',
    activeServiceId: 'test-svc',
    filterPayload: {},
    intervalButtons: null,
    bucketSeconds: 86400,
    config: { effectiveInterval: '1 day', interval: '1 day' },
    setChartInterval: vi.fn(),
    timezone: 'UTC',
  }),
)

beforeAll(() => {
  stubResizeObserver()
})

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })

let errorSpy: ReturnType<typeof spyOnConsoleError>

beforeEach(() => {
  queryClient.clear()
  errorSpy = spyOnConsoleError()
})

afterEach(() => {
  errorSpy.mockRestore()
})

test('fastly-value page mounts and renders title and tab navigation', () => {
  renderWithQuery(<FastlyValueClient />, { queryClient })
  expect(screen.getByText('Service Summary')).toBeInTheDocument()
  expect(screen.getByRole('tab', { name: /summary/i })).toBeInTheDocument()
  expect(screen.getByRole('tab', { name: /cdn & caching/i })).toBeInTheDocument()
  expect(screen.getByRole('tab', { name: /assets & shield/i })).toBeInTheDocument()
  expect(screen.getByRole('tab', { name: /security/i })).toBeInTheDocument()
  expect(screen.getByRole('tab', { name: /bot management/i })).toBeInTheDocument()
  expect(screen.getByRole('tab', { name: /performance/i })).toBeInTheDocument()
  expect(screen.getByRole('tab', { name: /network/i })).toBeInTheDocument()
  expect(screen.getByRole('tab', { name: /image optimizer/i })).toBeInTheDocument()
  expect(errorSpy).not.toHaveBeenCalled()
})

test('fastly-value summary query key keeps its pre-migration shape', async () => {
  const { resolveRangeWire } = await import('@/lib/range-wire')
  const { quantizeAnchor } = await import('@/lib/time-window')
  renderWithQuery(<FastlyValueClient />, { queryClient })
  const anchor = quantizeAnchor('2026-01-01T01:00:00Z')
  const { rangeKey } = resolveRangeWire({
    relativeRange: null,
    isAutoRange: false,
    startTime: '2026-01-01T00:00:00Z',
    endTime: '2026-01-01T01:00:00Z',
    anchor,
  })
  const keys = queryClient.getQueryCache().getAll().map((q) => q.queryKey)
  expect(keys).toContainEqual(['value', 'summary', 'test-svc', rangeKey, anchor, {}, '1 day', 'summary'])
})

type IOData = components['schemas']['IOMetrics']

const baseIOData = {
  io_time_series: [],
  format_distribution: [],
  io_bandwidth_time_series: [],
  io_compression_time_series: [],
  optimization_opportunities: [
    { url: '/img/hero.png', request_count: 1500, avg_kb: 812.34, total_bytes: 2_000_000 },
    { url: '/img/nokb.png', request_count: 3, avg_kb: null, total_bytes: 100 },
  ],
  io_format_conversion_pairs: [],
} as unknown as IOData

describe('IOValueTab tables', () => {
  it('upsell view lists the top image URLs', () => {
    render(<IOValueTab data={baseIOData} />)
    for (const name of ['URL', 'Requests', 'Avg KB', 'Total Bandwidth']) {
      expect(screen.getByRole('columnheader', { name })).toBeTruthy()
    }
    expect(screen.getByText('/img/hero.png')).toBeTruthy()
    expect(screen.getByText('812.3')).toBeTruthy()
    expect(screen.getAllByText('—').length).toBeGreaterThan(0)
  })

  it('enabled view lists format conversions', () => {
    const data = {
      ...baseIOData,
      io_transforms: 10,
      io_format_conversion_pairs: [{ input_format: 'png', output_format: 'webp', count: 2500, avg_ratio: 3.2 }],
    } as unknown as IOData
    render(<IOValueTab data={data} />)
    for (const name of ['From', 'To', 'Count', 'Avg Ratio']) {
      expect(screen.getByRole('columnheader', { name })).toBeTruthy()
    }
    expect(screen.getByText('png')).toBeTruthy()
    expect(screen.getByText('webp')).toBeTruthy()
    expect(screen.getByText('3.2x')).toBeTruthy()
  })
})
