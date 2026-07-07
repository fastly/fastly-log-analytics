import { act, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi, beforeEach } from 'vitest'
import QueryPage from '@/app/query/page'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import { useServiceStore } from '@/stores/serviceStore'
import { useFilterStore } from '@/stores/filterStore'
import {
  _resetUrlHydrationFlag,
  hydrateFilterStoreFromUrl,
} from '@/lib/urlFilterHydration'
import { client } from '@/lib/api'
import React from 'react'

// Mock next/navigation
vi.mock('next/navigation', () => ({
  useRouter: () => ({
    push: vi.fn(),
    replace: vi.fn(),
    prefetch: vi.fn(),
  }),
  useSearchParams: () => new URLSearchParams(),
  usePathname: () => '/query',
}))

// Mock complicated components. RawSqlMode dynamic-imports the editor by
// file path (so /query's initial bundle stays slim), while other consumers
// may still hit the barrel — stub both so the dynamic loader resolves to
// the textarea regardless of entry point.
const MockCodeEditor = ({ value, onChange }: any) => (
  <textarea data-testid="mock-editor" value={value} onChange={(e) => onChange(e.target.value)} />
)
vi.mock('@/components/CodeEditor', () => ({ CodeEditor: MockCodeEditor }))
vi.mock('@/components/CodeEditor/CodeEditor', () => ({ CodeEditor: MockCodeEditor }))

vi.mock('@tanstack/react-virtual', () => ({
  useVirtualizer: (options: any) => ({
    getVirtualItems: () => {
      const count = options.count || 0
      return Array.from({ length: count }).map((_, i) => ({
        index: i,
        start: i * 40,
        size: 40,
      }))
    },
    getTotalSize: () => (options.count || 0) * 40,
  }),
}))

// Mock the API client
vi.mock('@/lib/api', () => ({
  client: {
    GET: vi.fn(),
    POST: vi.fn(),
    use: vi.fn()
  },
  extractApiError: vi.fn(e => String(e)),
  getApiBase: vi.fn(() => 'http://test')
}))

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })

beforeEach(() => {
  vi.clearAllMocks()
  useServiceStore.setState({ activeServiceId: 'test-svc', isInitialized: true })
  useFilterStore.getState().resetAll()
  _resetUrlHydrationFlag()
  window.history.replaceState({}, '', '/query')
  queryClient.clear()
})

test('renders query page and executes a query', async () => {
  const user = userEvent.setup()
  // Mock API responses
  vi.mocked(client.GET).mockImplementation(async (url: any) => {
    if (url.includes('/api/log-fields/catalog')) {
      return { data: { fields: [] } } as any
    }
    if (url.includes('/api/schema')) {
      return { data: { schema: [] } } as any
    }
    if (url.includes('/api/presets')) {
      return { data: [] } as any
    }
    return { data: {} } as any
  })

  vi.mocked(client.POST).mockResolvedValue({
    data: {
      columns: ['timestamp', 'client_ip'],
      data: [{ timestamp: '2026-01-01T00:00:00Z', client_ip: '1.1.1.1' }],
      total_rows: 1,
      elapsed_ms: 10
    }
  } as any)

  render(
    <QueryClientProvider client={queryClient}>
      <QueryPage />
    </QueryClientProvider>
  )

  // Verify header
  expect(screen.getByText('Query Explorer')).toBeInTheDocument()

  // Click the Raw SQL tab to switch modes before interacting with the editor
  await user.click(screen.getByRole('tab', { name: /edit raw sql/i }))

  // Type a query. The mocked CodeEditor is a controlled <textarea>; user.clear()
  // + user.type() exercises the focus + per-character input chain. SQL string
  // contains no userEvent special chars ({, [, etc.) so we can pass it raw.
  const editor = screen.getByTestId('mock-editor')
  await user.clear(editor)
  await user.type(editor, 'SELECT * FROM logs')

  // Click Run Query — role lookup is sturdier than text-match (the page may
  // localize the label, or wrap the text in an icon span).
  await user.click(screen.getByRole('button', { name: /run query/i }))

  // Wait for results
  await waitFor(() => {
    expect(screen.getByText('1.1.1.1')).toBeInTheDocument()
  })
})

// Regression for the share-ability gap: /query used to strip ?filters= on
// hydration and never re-emit, so a teammate who pasted the URL got bare
// /query. useFilterUrlWriteback is now mounted here, mirroring the
// ReportLayout pages — every filter mutation rewrites ?filters= via
// history.replaceState so the visible URL stays shareable.
test('/query keeps ?filters= in the URL after a filter mutation', async () => {
  vi.mocked(client.GET).mockResolvedValue({ data: { fields: [] } } as any)
  vi.mocked(client.POST).mockResolvedValue({
    data: { columns: [], data: [], total_rows: 0, elapsed_ms: 1 },
  } as any)

  const inbound = { status: { values: ['200'], mode: 'include' } }
  window.history.replaceState(
    {},
    '',
    `/query?filters=${encodeURIComponent(JSON.stringify(inbound))}`,
  )

  // Module-load hydration normally runs from QueryProvider; replay it
  // here so the filterStore reflects the URL before QueryPage mounts.
  _resetUrlHydrationFlag()
  hydrateFilterStoreFromUrl()

  expect(useFilterStore.getState().filters.map(f => [f.column, f.value, f.mode]))
    .toEqual([['status', '200', 'include']])

  render(
    <QueryClientProvider client={queryClient}>
      <QueryPage />
    </QueryClientProvider>,
  )

  // useFilterUrlWriteback's effect should re-emit the current store state
  // (one include filter on status=200) back into the URL.
  await waitFor(() => {
    const params = new URLSearchParams(window.location.search)
    expect(params.get('filters')).not.toBeNull()
    const parsed = JSON.parse(params.get('filters')!)
    expect(parsed).toMatchObject({ status: { values: ['200'], mode: 'include' } })
  })

  // Mutate the store — adding a country filter — and verify the URL
  // updates to include the new pill (proves the write-back loop stays
  // live, not just a one-shot first-render emit).
  act(() => {
    useFilterStore.getState().addFilter('country', 'US', 'include')
  })

  await waitFor(() => {
    const params = new URLSearchParams(window.location.search)
    const parsed = JSON.parse(params.get('filters')!)
    expect(parsed).toMatchObject({
      status: { values: ['200'], mode: 'include' },
      country: { values: ['US'], mode: 'include' },
    })
  })
})
