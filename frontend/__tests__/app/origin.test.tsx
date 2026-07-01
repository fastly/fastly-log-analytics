import { render, screen } from '@testing-library/react'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
// app/origin/page.tsx is now an async RSC shell that SSR-prefetches and
// dehydrates into <OriginClient />; RTL can't render an async component, so
// the smoke test targets the client component that owns the title + sections.
import OriginClient from '@/app/origin/_sections/OriginClient'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import React from 'react'
import { spyOnConsoleError } from '../helpers/page-smoke'

// R-6: render-smoke for the origin analytics page.

vi.mock('@/stores/serviceStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())

vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).filterStoreModuleMock())

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/origin'))

vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(), POST: vi.fn().mockResolvedValue({ data: {} }), use: vi.fn() },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/app/origin/_sections/Aggregates', () => ({
  Aggregates: () => <div data-testid="aggregates" />,
}))
vi.mock('@/app/origin/_sections/Timeseries', () => ({
  Timeseries: () => <div data-testid="timeseries" />,
}))
vi.mock('@/app/origin/_sections/LatencyHeatmap', () => ({
  LatencyHeatmap: () => <div data-testid="latency-heatmap" />,
}))

vi.mock('@/components/ReportLayout', async () =>
  (await import('../helpers/page-smoke')).reportLayoutModuleMock({
    startTime: '2026-01-01T00:00:00Z',
    endTime: '2026-01-01T01:00:00Z',
    activeServiceId: 'test-svc',
    filterPayload: {},
    intervalButtons: null,
    bucketSeconds: 3600,
    config: { effectiveInterval: '5 minutes', interval: '5 minutes' },
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

test('origin page mounts and renders title', () => {
  render(
    <QueryClientProvider client={queryClient}>
      <OriginClient />
    </QueryClientProvider>,
  )
  expect(screen.getByText('Origin Performance')).toBeInTheDocument()
  expect(errorSpy).not.toHaveBeenCalled()
})
