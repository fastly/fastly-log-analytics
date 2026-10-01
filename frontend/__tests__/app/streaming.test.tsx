import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
import StreamingClient from '@/app/streaming/_sections/StreamingClient'
import { QueryClientProvider } from '@tanstack/react-query'
import { NuqsTestingAdapter, type UrlUpdateEvent } from 'nuqs/adapters/testing'
import { createTestQueryClient } from '../helpers/query'
import React from 'react'
import { spyOnConsoleError } from '../helpers/page-smoke'

// Render-smoke for the streaming page's Quality / Security tabs.

vi.mock('@/stores/serviceStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())

vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).filterStoreModuleMock())

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/streaming'))

vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(), POST: vi.fn().mockResolvedValue({ data: {} }), use: vi.fn() },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/app/streaming/_sections/tabs/QualityTab', () => ({
  default: () => <div data-testid="quality-tab" />,
}))

vi.mock('@/components/ReportLayout', async () =>
  (await import('../helpers/page-smoke')).reportLayoutModuleMock({
    startTime: '2026-01-01T00:00:00Z',
    endTime: '2026-01-01T01:00:00Z',
    activeServiceId: 'test-svc',
    filterPayload: {},
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

function renderPage(searchParams = '', onUrlUpdate?: (e: UrlUpdateEvent) => void) {
  return render(
    <NuqsTestingAdapter searchParams={searchParams} onUrlUpdate={onUrlUpdate}>
      <QueryClientProvider client={queryClient}>
        <StreamingClient />
      </QueryClientProvider>
    </NuqsTestingAdapter>,
  )
}

test('defaults to the Quality tab', () => {
  renderPage()
  expect(screen.getByRole('tab', { name: /quality/i })).toHaveAttribute('aria-selected', 'true')
  expect(screen.getByRole('tab', { name: /security/i })).toHaveAttribute('aria-selected', 'false')
  expect(screen.getByTestId('quality-tab')).toBeInTheDocument()
  expect(screen.queryByText(/content security metrics/i)).not.toBeInTheDocument()
})

test('switching to Security shows the placeholder and writes ?tab=security', async () => {
  const onUrlUpdate = vi.fn<(e: UrlUpdateEvent) => void>()
  renderPage('', onUrlUpdate)
  await userEvent.click(screen.getByRole('tab', { name: /security/i }))
  expect(screen.getByRole('tab', { name: /security/i })).toHaveAttribute('aria-selected', 'true')
  expect(screen.getByText(/content security metrics/i)).toBeInTheDocument()
  expect(screen.queryByTestId('quality-tab')).not.toBeInTheDocument()
  expect(onUrlUpdate).toHaveBeenLastCalledWith(expect.objectContaining({ queryString: '?tab=security' }))
})

test('?tab=security deep-links to the Security tab', () => {
  renderPage('?tab=security')
  expect(screen.getByRole('tab', { name: /security/i })).toHaveAttribute('aria-selected', 'true')
  expect(screen.getByText(/content security metrics/i)).toBeInTheDocument()
})

test('switching back to Quality clears ?tab from the URL', async () => {
  const onUrlUpdate = vi.fn<(e: UrlUpdateEvent) => void>()
  renderPage('?tab=security', onUrlUpdate)
  await userEvent.click(screen.getByRole('tab', { name: /quality/i }))
  expect(screen.getByTestId('quality-tab')).toBeInTheDocument()
  expect(onUrlUpdate).toHaveBeenLastCalledWith(expect.objectContaining({ queryString: '' }))
})

test('an unknown ?tab value falls back to Quality', () => {
  renderPage('?tab=bogus')
  expect(screen.getByRole('tab', { name: /quality/i })).toHaveAttribute('aria-selected', 'true')
  expect(screen.getByTestId('quality-tab')).toBeInTheDocument()
})

test('the description under the tabs follows the active tab', async () => {
  renderPage()
  expect(screen.getByText(/buffer health, bitrate, throughput/i)).toBeInTheDocument()
  await userEvent.click(screen.getByRole('tab', { name: /security/i }))
  expect(screen.getByText(/anti-piracy metrics/i)).toBeInTheDocument()
  expect(screen.queryByText(/buffer health, bitrate, throughput/i)).not.toBeInTheDocument()
})
