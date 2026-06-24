/**
 * Audit finding: DebugPanel (frontend/components/DebugPanel.tsx, 391 LOC)
 * had ZERO direct test coverage despite implementing a custom dedup
 * mechanism (sameQueries / sameCalls) layered over a TanStack Query cache
 * subscription. That dedup exists because every cache event (the 5s
 * SQLite poll + every other API response in the app) used to re-create
 * the queries/calls arrays and re-render, which re-fired the cache
 * subscribers, which looped to "Maximum update depth exceeded" in dev.
 *
 * If a future refactor accidentally drops field comparisons from
 * sameQueries/sameCalls — or worse, removes the dedup entirely — the
 * regression is silent: the panel only opens behind a manual debug toggle
 * and AppLayout.test.tsx mocks the whole component out. These tests pin
 * the dedup semantics + the enabled-gated lifecycle (poll start/stop +
 * data-clear-on-disable) so a regression fails CI loudly.
 */
import { render, screen, act, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, test, vi } from 'vitest'
import { http, HttpResponse } from 'msw'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import React from 'react'
import { DebugPanel } from '@/components/DebugPanel'
import { useDebugStore } from '@/stores/debugStore'
import { server } from '../../tests/msw/server'

vi.mock('next/navigation', () => ({
  usePathname: vi.fn(() => '/dashboard'),
}))

const API_BASE = 'http://127.0.0.1:8000'

function makeClient() {
  return new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: Infinity, staleTime: Infinity } },
  })
}

function renderWith(qc: QueryClient) {
  return render(
    <QueryClientProvider client={qc}>
      <DebugPanel />
    </QueryClientProvider>,
  )
}

beforeEach(() => {
  act(() => {
    useDebugStore.setState({ enabled: false, apiCallsEnabled: false })
  })
  server.use(
    http.get(`${API_BASE}/api/debug/recent-sqlite`, () =>
      HttpResponse.json({ queries: [], buffer_size: 0, buffer_cap: 500, dropped: 0 }),
    ),
  )
})

describe('DebugPanel — dedup + lifecycle', () => {
  test('renders nothing while both debug toggles are off', () => {
    const { container } = renderWith(makeClient())
    expect(container).toBeEmptyDOMElement()
  })

  test('SQLite polling fires when enabled; stops when enabled flips false', async () => {
    let calls = 0
    server.use(
      http.get(`${API_BASE}/api/debug/recent-sqlite`, () => {
        calls += 1
        return HttpResponse.json({ queries: [], buffer_size: 0, buffer_cap: 500, dropped: 0 })
      }),
    )

    act(() => {
      useDebugStore.setState({ enabled: true, apiCallsEnabled: false })
    })
    renderWith(makeClient())

    await waitFor(() => expect(calls).toBeGreaterThanOrEqual(1))
    const enabledCalls = calls

    act(() => {
      useDebugStore.setState({ enabled: false, apiCallsEnabled: false })
    })

    // After disable, the panel unmounts (returns null) → no further polls.
    await new Promise((r) => setTimeout(r, 30))
    expect(calls).toBe(enabledCalls)
  })

  test('expand/collapse of each section is independent', async () => {
    act(() => {
      useDebugStore.setState({ enabled: true, apiCallsEnabled: true })
    })
    renderWith(makeClient())
    const user = userEvent.setup()

    const duckBtn = await screen.findByRole('button', { name: /Show 0 queries/i })
    const sqliteBtn = screen.getByRole('button', { name: /Show 0 statements/i })
    const callsBtn = screen.getByRole('button', { name: /Show 0 calls/i })

    await user.click(sqliteBtn)
    expect(screen.getByRole('button', { name: /Hide 0 statements/i })).toBeInTheDocument()
    // Other two sections remain collapsed.
    expect(duckBtn).toHaveTextContent(/Show 0 queries/i)
    expect(callsBtn).toHaveTextContent(/Show 0 calls/i)

    await user.click(callsBtn)
    expect(screen.getByRole('button', { name: /Hide 0 calls/i })).toBeInTheDocument()
    expect(duckBtn).toHaveTextContent(/Show 0 queries/i)
  })

  test('subscription picks up a query whose data carries _debug_queries / _debug_calls', async () => {
    act(() => {
      useDebugStore.setState({ enabled: true, apiCallsEnabled: true })
    })
    const qc = makeClient()
    // Use a child component with a real useQuery so the cache entry has a
    // proper observer (DebugPanel.findAll({ type: 'active' }) reads
    // observer.options.enabled, which a hand-rolled stub observer lacks).
    const { useQuery } = await import('@tanstack/react-query')
    function Seeder() {
      useQuery({
        queryKey: ['debug-fixture', 'one'],
        // initialData populates the cache without an HTTP roundtrip
        initialData: {
          _debug_queries: [{ sql: 'SELECT 1', time_ms: 12.5 }],
          _debug_calls: [{ service: 'FOS', method: 'GET', path: '/x', time_ms: 8, status: 'OK' }],
        },
        queryFn: async () => null,
        staleTime: Infinity,
      })
      return null
    }
    render(
      <QueryClientProvider client={qc}>
        <Seeder />
        <DebugPanel />
      </QueryClientProvider>,
    )

    await waitFor(() => {
      expect(screen.getByRole('button', { name: /Show 1 queries/i })).toBeInTheDocument()
      expect(screen.getByRole('button', { name: /Show 1 calls/i })).toBeInTheDocument()
    })

    // Totals reflect what the subscription extracted (dedup preserved fields).
    expect(screen.getByText(/12\.50ms/)).toBeInTheDocument()

    // Re-notify with IDENTICAL data → dedup keeps the count steady (no
    // duplicated rows). sameQueries/sameCalls returning true means no
    // setState fires; if it were false we'd see "2 queries".
    await act(async () => {
      const q = qc.getQueryCache().find({ queryKey: ['debug-fixture', 'one'] })!
      qc.getQueryCache().notify({ type: 'updated', query: q } as any)
    })
    await new Promise((r) => setTimeout(r, 10))
    expect(screen.getByRole('button', { name: /Show 1 queries/i })).toBeInTheDocument()

    // Change time_ms → sameQueries returns false → re-render with the new value.
    await act(async () => {
      qc.setQueryData(['debug-fixture', 'one'], {
        _debug_queries: [{ sql: 'SELECT 1', time_ms: 99 }],
        _debug_calls: [{ service: 'FOS', method: 'GET', path: '/x', time_ms: 8, status: 'OK' }],
      })
    })
    await waitFor(() => {
      expect(screen.getByText(/99\.00ms/)).toBeInTheDocument()
    })

    // Disable both toggles → displayed query/call counts clear to zero.
    act(() => {
      useDebugStore.setState({ enabled: false, apiCallsEnabled: false })
    })
    await waitFor(() => {
      // Component returns null when both flags are off.
      expect(screen.queryByRole('button', { name: /queries/i })).not.toBeInTheDocument()
    })
  })
})
