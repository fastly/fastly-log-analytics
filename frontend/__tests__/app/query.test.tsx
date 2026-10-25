import { act, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi, beforeEach } from 'vitest'
import QueryPage from '@/app/query/page'
import { createTestQueryClient, renderWithQuery } from '../helpers/page-smoke'
import { useServiceStore } from '@/stores/serviceStore'
import { useFilterStore } from '@/stores/filterStore'
import {
  _resetUrlHydrationFlag,
  hydrateFilterStoreFromUrl,
} from '@/lib/utils'
import React from 'react'
import { withNuqsTestingAdapter, type OnUrlUpdateFunction } from 'nuqs/adapters/testing'

type ApiStub = { data?: unknown; error?: unknown }
const { mockGet, mockPost } = vi.hoisted(() => ({
  mockGet: vi.fn<(url: string) => Promise<ApiStub>>(),
  mockPost: vi.fn<(url: string) => Promise<ApiStub>>(),
}))

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

// Mock CodeMirror so CodeEditor renders a simple controlled <textarea>.
vi.mock('@uiw/react-codemirror', async () => {
  const React = await import('react')
  return {
    default: React.forwardRef(function MockCodeMirror(
      { value, onChange }: { value: string; onChange: (v: string) => void },
      ref: React.Ref<HTMLTextAreaElement>,
    ) {
      return <textarea ref={ref} data-testid="mock-editor" value={value} onChange={(e) => onChange(e.target.value)} />
    }),
  }
})

vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/page-smoke')).reactVirtualModuleMock())

// Mock the API client
vi.mock('@/lib/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/api')>()),
  client: {
    GET: mockGet,
    POST: mockPost,
    use: vi.fn()
  },
  extractApiError: vi.fn(e => String(e)),
  getApiBase: vi.fn(() => 'http://test')
}))

const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })

// nuqs needs an adapter; seed it from the jsdom URL so tests that
// history.replaceState() a deep link before rendering keep working.
const renderWithNuqs = (ui: React.ReactElement, onUrlUpdate?: OnUrlUpdateFunction) =>
  renderWithQuery(ui, {
    queryClient,
    wrapper: withNuqsTestingAdapter({ searchParams: window.location.search, hasMemory: true, onUrlUpdate }),
  })

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
  mockGet.mockImplementation(async (url: string) => {
    if (url.includes('/api/log-fields/catalog')) {
      return { data: { fields: [] } }
    }
    if (url.includes('/api/schema')) {
      return { data: { schema: [] } }
    }
    if (url.includes('/api/presets')) {
      return { data: [] }
    }
    return { data: {} }
  })

  mockPost.mockResolvedValue({
    data: {
      columns: ['timestamp', 'client_ip'],
      data: [{ timestamp: '2026-01-01T00:00:00Z', client_ip: '1.1.1.1' }],
      total_rows: 1,
      elapsed_ms: 10
    }
  })

  renderWithNuqs(<QueryPage />)

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
  mockGet.mockResolvedValue({ data: { fields: [] } })
  mockPost.mockResolvedValue({
    data: { columns: [], data: [], total_rows: 0, elapsed_ms: 1 },
  })

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

  renderWithNuqs(<QueryPage />)

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
  mockGet.mockImplementation(() => new Promise<ApiStub>(() => {}))

  renderWithNuqs(<QueryPage />)

  expect(screen.getByTestId('query-engine-initializing')).toBeInTheDocument()
  expect(screen.getByText(/Initializing DuckDB analytical engine/i)).toBeInTheDocument()
})

test('enforces zero-CLS layout reservations and renders schema sidebar in Raw SQL mode', async () => {
  const user = userEvent.setup()
  mockGet.mockImplementation(async (url: string) => {
    if (url.includes('/api/schema')) {
      return {
        data: {
          schema: [
            { name: 'timestamp', type: 'TIMESTAMP' },
            { name: 'client_ip', type: 'VARCHAR' },
          ],
        },
      }
    }
    return { data: { fields: [] } }
  })

  renderWithNuqs(<QueryPage />)

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
  mockGet.mockResolvedValue({ data: { fields: [], schema: [] } })
  mockPost.mockResolvedValue({
    error: JSON.stringify({
      detail: {
        error: 'sql_not_permitted',
        message: 'only SELECT / WITH / UNION statements allowed (got DROP)',
      },
    }),
  })

  renderWithNuqs(<QueryPage />)

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
  mockGet.mockResolvedValue({ data: { fields: [], schema: [] } })
  mockPost.mockResolvedValue({
    data: {
      columns: ['url', 'requests'],
      data: [
        { url: '/index.html', requests: 42 },
        { url: '/api/v1/items', requests: 19 },
      ],
      total_rows: 2,
      elapsed_ms: 8,
    },
  })

  renderWithNuqs(<QueryPage />)

  await user.click(screen.getByRole('button', { name: /run query/i }))

  await waitFor(() => {
    expect(screen.getByTestId('query-results-table')).toBeInTheDocument()
  })
  expect(screen.getByTestId('query-csv-export-btn')).toBeInTheDocument()
  expect(screen.getByText('/index.html')).toBeInTheDocument()
})

test('hydrates Raw SQL mode when ?q= URL parameter is provided', async () => {
  mockGet.mockResolvedValue({ data: { fields: [], schema: [] } })
  window.history.replaceState({}, '', '/query?q=' + encodeURIComponent('SELECT count(*) FROM logs'))

  renderWithNuqs(<QueryPage />)

  const editor = await screen.findByTestId('mock-editor')
  expect((editor as HTMLTextAreaElement).value).toBe('SELECT count(*) FROM logs')
})

test('deep link ?mode=raw opens the Raw SQL editor', async () => {
  mockGet.mockResolvedValue({ data: { fields: [], schema: [] } })
  window.history.replaceState({}, '', '/query?mode=raw')

  renderWithNuqs(<QueryPage />)

  expect(await screen.findByTestId('mock-editor')).toBeInTheDocument()
  expect(screen.getByRole('tab', { name: /edit raw sql/i })).toHaveAttribute('aria-selected', 'true')
})

test('an unrecognised ?mode= value stays in structured mode', async () => {
  mockGet.mockResolvedValue({ data: { fields: [], schema: [] } })
  window.history.replaceState({}, '', '/query?mode=bogus')

  renderWithNuqs(<QueryPage />)

  expect(await screen.findByRole('tab', { name: /edit raw sql/i })).toHaveAttribute('aria-selected', 'false')
  expect(screen.queryByTestId('mock-editor')).not.toBeInTheDocument()
})

test('switching modes writes and clears ?mode=raw while preserving other params', async () => {
  const user = userEvent.setup()
  mockGet.mockResolvedValue({ data: { fields: [], schema: [] } })
  window.history.replaceState({}, '', '/query?start_time=2026-01-01T00:00:00Z&end_time=2026-01-01T01:00:00Z')
  const onUrlUpdate = vi.fn<OnUrlUpdateFunction>()

  renderWithNuqs(<QueryPage />, onUrlUpdate)

  await user.click(await screen.findByRole('tab', { name: /edit raw sql/i }))
  await waitFor(() => {
    const last = onUrlUpdate.mock.calls.at(-1)?.[0]
    expect(last?.searchParams.get('mode')).toBe('raw')
    expect(last?.searchParams.get('start_time')).toBe('2026-01-01T00:00:00Z')
    expect(last?.options.history).toBe('replace')
  })

  await user.click(screen.getByRole('tab', { name: /structured/i }))
  await waitFor(() => {
    const last = onUrlUpdate.mock.calls.at(-1)?.[0]
    expect(last?.searchParams.has('mode')).toBe(false)
    expect(last?.searchParams.get('end_time')).toBe('2026-01-01T01:00:00Z')
  })
})

test('dashboard "See Raw Logs" deep link (?start_time&end_time&filters) hydrates the filter store', async () => {
  mockGet.mockResolvedValue({ data: { fields: [], schema: [] } })
  const filters = { status_2: { values: ['500', '503'], mode: 'exclude' } }
  window.history.replaceState(
    {},
    '',
    `/query?start_time=2026-02-01T00:00:00Z&end_time=2026-02-01T06:00:00Z&filters=${encodeURIComponent(JSON.stringify(filters))}`,
  )

  renderWithNuqs(<QueryPage />)

  await screen.findByText('Query Explorer')
  const state = useFilterStore.getState()
  expect(state.startTime).toBe('2026-02-01T00:00:00Z')
  expect(state.endTime).toBe('2026-02-01T06:00:00Z')
  expect(state.filters.map((f) => [f.column, f.value, f.mode])).toEqual([
    ['status', '500', 'exclude'],
    ['status', '503', 'exclude'],
  ])
})
