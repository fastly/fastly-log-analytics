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
import { DEBUG_RESPONSES_COOKIE } from '@/lib/debug-cookie'
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
  // Satisfy the cookie-heal shim up front so the mount-time
  // refetchQueries({type:'active'}) doesn't re-run fixture queryFns (which
  // return null and would clobber initialData) in the tests below. The shim
  // itself is pinned by its own dedicated tests, which clear this.
  document.cookie = `${DEBUG_RESPONSES_COOKIE}=1; path=/`
  server.use(
    http.get(`${API_BASE}/api/debug/recent-sqlite`, () =>
      HttpResponse.json({ queries: [], buffer_size: 0, buffer_cap: 500, dropped: 0 }),
    ),
  )
})

describe('DebugPanel — cookie-heal shim', () => {
  test('toggle persisted on but cookie missing → writes cookie and invalidates the cache', async () => {
    // Pre-cookie browser state: localStorage says enabled, but the
    // fla.debugResponses cookie (introduced later, written only on toggle
    // FLIP) was never set — SSR omits x-debug-responses and every
    // server-prefetched page hydrates without the debug envelope.
    document.cookie = `${DEBUG_RESPONSES_COOKIE}=; path=/; max-age=0`
    act(() => {
      useDebugStore.setState({ enabled: true, apiCallsEnabled: false })
    })
    const qc = makeClient()
    // Invalidation (not a point-in-time refetch): the heavy data queries
    // mount enabled:false until service/filter hydration resolves, so they
    // must be marked stale to refetch when they switch on.
    const invalidateSpy = vi.spyOn(qc, 'invalidateQueries')
    renderWith(qc)

    await waitFor(() => {
      expect(document.cookie).toContain(`${DEBUG_RESPONSES_COOKIE}=1`)
    })
    expect(invalidateSpy).toHaveBeenCalled()
  })

  test('cookie already present → no redundant invalidation', async () => {
    document.cookie = `${DEBUG_RESPONSES_COOKIE}=1; path=/`
    act(() => {
      useDebugStore.setState({ enabled: true, apiCallsEnabled: false })
    })
    const qc = makeClient()
    const invalidateSpy = vi.spyOn(qc, 'invalidateQueries')
    renderWith(qc)

    await new Promise((r) => setTimeout(r, 20))
    expect(invalidateSpy).not.toHaveBeenCalled()
  })
})

describe('DebugPanel — dedup + lifecycle', () => {
  test('renders nothing while both debug toggles are off', () => {
    const { container } = renderWith(makeClient())
    expect(container).toBeEmptyDOMElement()
  })

  test('SQLite ring-buffer poll only runs in Process-wide scope', async () => {
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
    const user = userEvent.setup()

    // Default scope is 'This page' (reads _debug_sqlite off the page's own
    // responses) — the ring-buffer endpoint must see ZERO traffic. This is
    // both a UX contract (page view ≠ process view) and a noise contract
    // (no 5s poll per admin tab unless explicitly asked for).
    await new Promise((r) => setTimeout(r, 30))
    expect(calls).toBe(0)

    await user.click(screen.getByRole('button', { name: /Process-wide/i }))
    await waitFor(() => expect(calls).toBeGreaterThanOrEqual(1))
    const bufferScopeCalls = calls

    // Back to page scope → poll stops.
    await user.click(screen.getByRole('button', { name: /This page/i }))
    await new Promise((r) => setTimeout(r, 30))
    expect(calls).toBe(bufferScopeCalls)

    act(() => {
      useDebugStore.setState({ enabled: false, apiCallsEnabled: false })
    })

    // After disable, the panel unmounts (returns null) → no further polls.
    await new Promise((r) => setTimeout(r, 30))
    expect(calls).toBe(bufferScopeCalls)
  })

  test("page scope lists _debug_sqlite statements from this page's responses", async () => {
    act(() => {
      useDebugStore.setState({ enabled: true, apiCallsEnabled: false })
    })
    const qc = makeClient()
    const { useQuery } = await import('@tanstack/react-query')
    function Seeder() {
      useQuery({
        queryKey: ['debug-fixture', 'sqlite'],
        initialData: {
          _debug_sqlite: [
            { seq: 5, ts: '2026-07-07T12:00:00.000000Z', sql: 'SELECT a FROM t', params_kind: 'none', time_ms: 3.25, rows: -1, op: 'execute' },
            { seq: 6, ts: '2026-07-07T12:00:00.100000Z', sql: 'INSERT INTO t VALUES (?)', params_kind: 'seq[1]', time_ms: 1.75, rows: 1, op: 'execute' },
          ],
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

    // Extraction happens without any /api/debug/recent-sqlite traffic.
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /Show 2 statements/i })).toBeInTheDocument()
    })
    // Page-scoped total: 3.25 + 1.75.
    expect(screen.getByText(/5\.00ms/)).toBeInTheDocument()

    const user = userEvent.setup()
    await user.click(screen.getByRole('button', { name: /Show 2 statements/i }))
    expect(screen.getByText('SELECT a FROM t')).toBeInTheDocument()
    expect(screen.getByText('INSERT INTO t VALUES (?)')).toBeInTheDocument()
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
