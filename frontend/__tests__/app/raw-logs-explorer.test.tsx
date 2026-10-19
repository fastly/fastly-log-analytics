import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClientProvider } from '@tanstack/react-query'
import { beforeEach, expect, test, vi } from 'vitest'
import React from 'react'
import { createTestQueryClient } from '../helpers/query'

vi.mock('@/stores/serviceStore', async () => (await import('../helpers/page-smoke')).serviceStoreModuleMock())
vi.mock('@/stores/filterStore', async () => (await import('../helpers/page-smoke')).filterStoreModuleMock())
vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/virtual-mock')).reactVirtualModuleMock())
vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/logs'))

const mockPost = vi.fn()
let mockFilterPayload: Record<string, unknown> = {}
vi.mock('@/hooks/useFilterPayload', () => ({ useDebouncedFilterPayload: () => mockFilterPayload }))
vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(), POST: (...a: unknown[]) => mockPost(...a), use: vi.fn() },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

import { RawLogsExplorer } from '@/app/logs/_sections/RawLogsExplorer'

const page = (offset: number) => ({
  data: {
    rows: [{ timestamp: `2026-08-19T12:00:0${offset / 50}Z`, status: 200, url: '/x' }],
    total_count: 500,
    has_more: true,
    limit: 50,
    offset,
    columns: ['timestamp', 'status', 'url'],
    schema_columns: [],
    missing_fields: [],
  },
})

beforeEach(() => {
  mockPost.mockReset()
  mockPost.mockImplementation(async (_path: string, opts: { body: { offset: number } }) => page(opts.body.offset))
})

test('changing filters while on page 2 never fetches with the stale offset', async () => {
  mockFilterPayload = {}
  const qc = createTestQueryClient()
  const ui = () => (
    <QueryClientProvider client={qc}>
      <RawLogsExplorer activeServiceId="svc" />
    </QueryClientProvider>
  )
  const { rerender } = render(ui())
  await waitFor(() => expect(mockPost).toHaveBeenCalledTimes(1))

  fireEvent.click(await screen.findByRole('button', { name: /next/i }))
  await waitFor(() => expect(mockPost.mock.calls.some((c) => c[1].body.offset === 50)).toBe(true))

  mockFilterPayload = { status: { mode: 'include', values: ['500'] } }
  // A fresh element each time: re-rendering the identical element reference is a React no-op.
  rerender(ui())
  await waitFor(() => expect(mockPost.mock.calls.some((c) => c[1].body.filters.status)).toBe(true))

  const filtered = mockPost.mock.calls.filter((c) => c[1].body.filters.status)
  expect(filtered.every((c) => c[1].body.offset === 0)).toBe(true)
})

test('switching services never renders the previous service rows while loading', async () => {
  mockFilterPayload = {}
  let release: () => void = () => {}
  const gate = new Promise<void>((r) => {
    release = r
  })
  const rowsFor = (url: string) => ({ data: { ...page(0).data, rows: [{ timestamp: '2026-08-19T12:00:00Z', status: 200, url }] } })
  mockPost.mockReset()
  mockPost.mockResolvedValueOnce(rowsFor('/from-a'))
  mockPost.mockImplementationOnce(async () => {
    await gate
    return rowsFor('/from-b')
  })
  const qc = createTestQueryClient()
  const ui = (svc: string) => (
    <QueryClientProvider client={qc}>
      <RawLogsExplorer activeServiceId={svc} />
    </QueryClientProvider>
  )
  const { rerender } = render(ui('svc-a'))
  expect(await screen.findByText('/from-a')).toBeTruthy()
  rerender(ui('svc-b'))
  await waitFor(() => expect(mockPost).toHaveBeenCalledTimes(2))
  expect(screen.queryByText('/from-a')).toBeNull()
  release()
  expect(await screen.findByText('/from-b')).toBeTruthy()
})

test('renders selected columns through the shared table and opens the row detail on click', async () => {
  mockFilterPayload = {}
  const qc = createTestQueryClient()
  render(
    <QueryClientProvider client={qc}>
      <RawLogsExplorer activeServiceId="svc" />
    </QueryClientProvider>,
  )
  const table = await screen.findByTestId('raw-logs-table')
  for (const name of ['Timestamp', 'Status', 'Method', 'URL Path', 'Client IP', 'POP', 'Origin', 'Duration']) {
    expect(screen.getByRole('columnheader', { name })).toBeInTheDocument()
  }
  const dataRow = table.querySelector('tbody tr') as HTMLElement
  expect(dataRow.textContent).toContain('/x')
  expect(dataRow.textContent).toContain('2026-08-19 12:00:00')
  fireEvent.click(dataRow)
  expect(await screen.findByTestId('raw-logs-row-detail-modal')).toBeInTheDocument()
})
