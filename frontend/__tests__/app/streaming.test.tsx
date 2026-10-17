import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
import StreamingClient from '@/app/streaming/_sections/StreamingClient'
import { QueryClientProvider } from '@tanstack/react-query'
import { NuqsTestingAdapter, type UrlUpdateEvent } from 'nuqs/adapters/testing'
import { createTestQueryClient } from '../helpers/query'
import React from 'react'
import { spyOnConsoleError } from '../helpers/page-smoke'
import { useIsAnalyst } from '@/hooks/useIsAnalyst'

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

vi.mock('@/app/streaming/_sections/tabs/SecurityTab', () => ({
  default: () => <div data-testid="security-tab" />,
}))

vi.mock('@/app/streaming/_sections/tabs/PoliciesView', () => ({
  default: () => <div data-testid="policies-view" />,
}))

vi.mock('@/hooks/useIsAnalyst', () => ({ useIsAnalyst: vi.fn(() => false) }))

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
  vi.mocked(useIsAnalyst).mockReturnValue(false)
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
  expect(screen.queryByTestId('security-tab')).not.toBeInTheDocument()
})

test('switching to Security shows the Security tab and writes ?tab=security', async () => {
  const onUrlUpdate = vi.fn<(e: UrlUpdateEvent) => void>()
  renderPage('', onUrlUpdate)
  await userEvent.click(screen.getByRole('tab', { name: /security/i }))
  expect(screen.getByRole('tab', { name: /security/i })).toHaveAttribute('aria-selected', 'true')
  expect(screen.getByTestId('security-tab')).toBeInTheDocument()
  expect(screen.queryByTestId('quality-tab')).not.toBeInTheDocument()
  expect(onUrlUpdate).toHaveBeenLastCalledWith(expect.objectContaining({ queryString: '?tab=security' }))
})

test('?tab=security deep-links to the Security tab', () => {
  renderPage('?tab=security')
  expect(screen.getByRole('tab', { name: /security/i })).toHaveAttribute('aria-selected', 'true')
  expect(screen.getByTestId('security-tab')).toBeInTheDocument()
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

test('Content Security opens on Monitor, with a Policies view beside it', () => {
  renderPage('?tab=security')
  expect(screen.getByRole('tab', { name: 'Monitor' })).toHaveAttribute('aria-selected', 'true')
  expect(screen.getByRole('tab', { name: 'Policies' })).toHaveAttribute('aria-selected', 'false')
  expect(screen.getByTestId('security-tab')).toBeInTheDocument()
  expect(screen.queryByTestId('policies-view')).not.toBeInTheDocument()
})

test('choosing Policies shows the Policies view and writes ?csview=policies', async () => {
  const onUrlUpdate = vi.fn<(e: UrlUpdateEvent) => void>()
  renderPage('?tab=security', onUrlUpdate)
  await userEvent.click(screen.getByRole('tab', { name: 'Policies' }))
  expect(screen.getByTestId('policies-view')).toBeInTheDocument()
  expect(screen.queryByTestId('security-tab')).not.toBeInTheDocument()
  expect(onUrlUpdate).toHaveBeenLastCalledWith(expect.objectContaining({ queryString: '?tab=security&csview=policies' }))
})

test('?csview=policies deep-links to the Policies view', () => {
  renderPage('?tab=security&csview=policies')
  expect(screen.getByRole('tab', { name: 'Policies' })).toHaveAttribute('aria-selected', 'true')
  expect(screen.getByTestId('policies-view')).toBeInTheDocument()
})

test('analysts only get Monitor: no Policies view', () => {
  vi.mocked(useIsAnalyst).mockReturnValue(true)
  renderPage('?tab=security&csview=policies')
  expect(screen.queryByRole('tab', { name: 'Policies' })).not.toBeInTheDocument()
  expect(screen.getByTestId('security-tab')).toBeInTheDocument()
})
