import { render, screen } from '@testing-library/react'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
import FastlyValueClient from '@/app/fastly-value/_sections/FastlyValueClient'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import React from 'react'
import { spyOnConsoleError } from '../helpers/page-smoke'

// Render-smoke for the Fastly Value / Service Summary page.

vi.mock('@/stores/serviceStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())

vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).filterStoreModuleMock())

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/fastly-value'))

vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(), POST: vi.fn().mockResolvedValue({ data: {} }), use: vi.fn() },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/components/PlotlyChart', async () => (await import('../helpers/page-smoke')).plotlyChartModuleMock())

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
  render(
    <QueryClientProvider client={queryClient}>
      <FastlyValueClient />
    </QueryClientProvider>,
  )
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
  render(
    <QueryClientProvider client={queryClient}>
      <FastlyValueClient />
    </QueryClientProvider>,
  )
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
