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
  useSearchParams: () => new URLSearchParams(window.location.search),
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

test('renders engine initializing skeleton when service is not yet initialized', () => {
  useServiceStore.setState({ activeServiceId: null, isInitialized: false })
  vi.mocked(client.GET).mockImplementation(() => new Promise(() => {}) as any)

  render(
    <QueryClientProvider client={queryClient}>
      <QueryPage />
    </QueryClientProvider>,
  )

  expect(screen.getByTestId('query-engine-initializing')).toBeInTheDocument()
  expect(screen.getByText(/Initializing DuckDB analytical engine/i)).toBeInTheDocument()
})

test('enforces zero-CLS layout reservations and renders schema sidebar in Raw SQL mode', async () => {
  const user = userEvent.setup()
  vi.mocked(client.GET).mockImplementation(async (url: any) => {
    if (url.includes('/api/schema')) {
      return {
        data: {
          schema: [
            { name: 'timestamp', type: 'TIMESTAMP' },
            { name: 'client_ip', type: 'VARCHAR' },
          ],
        },
      } as any
    }
    return { data: { fields: [] } } as any
  })

  render(
    <QueryClientProvider client={queryClient}>
      <QueryPage />
    </QueryClientProvider>,
  )

  const pageRoot = screen.getByTestId('query-page-root')
  expect(pageRoot.className).toContain('contain-intrinsic-size')

  const editorContainer = screen.getByTestId('query-editor-container')
  expect(editorContainer.className).toContain('contain-intrinsic-size')

  await user.click(screen.getByRole('tab', { name: /edit raw sql/i }))

  const sidebar = await screen.findByTestId('query-schema-sidebar')
  expect(sidebar).toBeInTheDocument()
  expect(screen.getByText('timestamp')).toBeInTheDocument()
  expect(screen.getByText('client_ip')).toBeInTheDocument()
})

test('displays user-friendly error alert when mutating SQL is rejected by AST validator', async () => {
  const user = userEvent.setup()
  vi.mocked(client.GET).mockResolvedValue({ data: { fields: [], schema: [] } } as any)
  vi.mocked(client.POST).mockResolvedValue({
    error: JSON.stringify({
      detail: {
        error: 'sql_not_permitted',
        message: 'only SELECT / WITH / UNION statements allowed (got DROP)',
      },
    }),
  } as any)

  render(
    <QueryClientProvider client={queryClient}>
      <QueryPage />
    </QueryClientProvider>,
  )

  await user.click(screen.getByRole('tab', { name: /edit raw sql/i }))
  const editor = screen.getByTestId('mock-editor')
  await user.clear(editor)
  await user.type(editor, 'DROP TABLE logs')
  await user.click(screen.getByRole('button', { name: /run query/i }))

  const alert = await screen.findByTestId('query-error-alert')
  expect(alert).toBeInTheDocument()
  expect(alert.textContent).toMatch(/only SELECT \/ WITH \/ UNION statements allowed/i)
})

test('renders CSV export button and virtualized results table after query execution', async () => {
  const user = userEvent.setup()
  vi.mocked(client.GET).mockResolvedValue({ data: { fields: [], schema: [] } } as any)
  vi.mocked(client.POST).mockResolvedValue({
    data: {
      columns: ['url', 'requests'],
      data: [
        { url: '/index.html', requests: 42 },
        { url: '/api/v1/items', requests: 19 },
      ],
      total_rows: 2,
      elapsed_ms: 8,
    },
  } as any)

  render(
    <QueryClientProvider client={queryClient}>
      <QueryPage />
    </QueryClientProvider>,
  )

  await user.click(screen.getByRole('button', { name: /run query/i }))

  await waitFor(() => {
    expect(screen.getByTestId('query-results-table')).toBeInTheDocument()
  })
  expect(screen.getByTestId('query-csv-export-btn')).toBeInTheDocument()
  expect(screen.getByText('/index.html')).toBeInTheDocument()
})

test('hydrates Raw SQL mode when ?q= URL parameter is provided', async () => {
  vi.mocked(client.GET).mockResolvedValue({ data: { fields: [], schema: [] } } as any)
  window.history.replaceState({}, '', '/query?q=' + encodeURIComponent('SELECT count(*) FROM logs'))

  render(
    <QueryClientProvider client={queryClient}>
      <QueryPage />
    </QueryClientProvider>,
  )

  const editor = await screen.findByTestId('mock-editor')
  expect((editor as HTMLTextAreaElement).value).toBe('SELECT count(*) FROM logs')
})
