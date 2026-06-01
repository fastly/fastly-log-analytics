import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi, beforeEach } from 'vitest'
import QueryPage from '@/app/query/page'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { useServiceStore } from '@/stores/serviceStore'
import { client } from '@/lib/api'
import React from 'react'

// Mock complicated components
vi.mock('@/components/CodeEditor', () => ({
  CodeEditor: ({ value, onChange }: any) => (
    <textarea data-testid="mock-editor" value={value} onChange={(e) => onChange(e.target.value)} />
  )
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

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: false, staleTime: 0 } }
})

beforeEach(() => {
  vi.clearAllMocks()
  useServiceStore.setState({ activeServiceId: 'test-svc', isInitialized: true })
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
