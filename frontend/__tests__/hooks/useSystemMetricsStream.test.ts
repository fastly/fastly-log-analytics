/**
 * useSystemMetricsStream — fan-out hook that splits a single SSE
 * bundle from /api/admin/system-metrics/stream into the seven React
 * Query cache slices the admin overview cards already read from,
 * replacing seven independent polling queries.
 *
 * Audit finding: the hook's null-slice skip is load-bearing — a
 * failed component sample arrives as null and we MUST NOT overwrite
 * the last-good cached value (per the JSDoc on lines ~57-60).
 * Without coverage, a future refactor that swaps `!= null` for a
 * truthy check or a blanket setQueryData would silently blank out
 * cards on every transient sampler failure. Also covers the
 * malformed-JSON guard (sse_starlette keepalive in an unexpected
 * frame layout) which must not crash the consumer.
 *
 * @vitest-environment jsdom
 */
import { renderHook, act } from '@testing-library/react'
import { QueryClient } from '@tanstack/react-query'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { describe, it, expect, beforeEach, vi } from 'vitest'

// Capture the onEvent callback the hook registers with
// useServiceStream so each test can push raw SSE payloads through it
// without standing up the fetch / ReadableStream transport.
let capturedOnEvent: ((raw: string) => void) | null = null
let lastEnabled: boolean | undefined
let lastPath: string | undefined
let lastOpts: Record<string, unknown> | undefined

vi.mock('@/hooks/useServiceStream', () => ({
  useServiceStream: vi.fn(
    (enabled: boolean, path: string, onEvent: (raw: string) => void, opts?: Record<string, unknown>) => {
      lastEnabled = enabled
      lastPath = path
      capturedOnEvent = onEvent
      lastOpts = opts
    },
  ),
}))

function makeQueryClient() {
  // gcTime > 0 — the hook writes via setQueryData WITHOUT any
  // subscriber, and with gcTime: 0 the entry is collected before the
  // assertion reads it back.
  const qc = createTestQueryClient({ queries: { gcTime: 60_000, staleTime: 0 } })
  // useSystemMetricsStream gates the stream on useBootstrapSettled() — seed
  // bootstrap so the stream is enabled for the assertions below. The dedicated
  // gating test uses an UNSEEDED client to pin the pre-bootstrap hold.
  qc.setQueryData(['bootstrap'], { active_service_id: null })
  return qc
}

function wrapperWith(qc: QueryClient) {
  return makeQueryWrapper(qc)
}

beforeEach(() => {
  capturedOnEvent = null
  lastEnabled = undefined
  lastPath = undefined
  lastOpts = undefined
})

describe('useSystemMetricsStream', () => {
  it('subscribes to /api/admin/system-metrics/stream with the enabled flag', async () => {
    const qc = makeQueryClient()
    const { useSystemMetricsStream } = await import('@/hooks/useSystemMetricsStream')
    renderHook(() => useSystemMetricsStream(true), { wrapper: wrapperWith(qc) })

    expect(lastEnabled).toBe(true)
    expect(lastPath).toBe('/api/admin/system-metrics/stream')
    expect(capturedOnEvent).toBeInstanceOf(Function)
  })

  it('holds the stream until bootstrap resolves, then enables it (no per-load reconnect churn)', async () => {
    // Unseeded client: bootstrap has not landed yet, so the stream must stay
    // disabled — connecting now would open on the pre-bootstrap serviceId /
    // null admin token and abort + reconnect once bootstrap seeds them (the
    // "context canceled" the reverse proxy logs on every admin load).
    const qc = createTestQueryClient({ queries: { gcTime: 60_000, staleTime: 0 } })
    const { useSystemMetricsStream } = await import('@/hooks/useSystemMetricsStream')
    renderHook(() => useSystemMetricsStream(true), { wrapper: wrapperWith(qc) })

    expect(lastEnabled).toBe(false)

    // Bootstrap lands → the gate opens and the stream enables exactly once.
    act(() => {
      qc.setQueryData(['bootstrap'], { active_service_id: null })
    })

    expect(lastEnabled).toBe(true)
  })

  it('opts into soft service scoping so the global slices stream pre-service', async () => {
    const qc = makeQueryClient()
    const { useSystemMetricsStream } = await import('@/hooks/useSystemMetricsStream')
    renderHook(() => useSystemMetricsStream(true), { wrapper: wrapperWith(qc) })

    // optionalService:true lets the bundle connect on a fresh install (no
    // service yet) and deliver the global slices live; cache:'no-store'
    // keeps the browser from caching the stream body.
    expect(lastOpts).toMatchObject({ optionalService: true, cache: 'no-store' })
  })

  it('fans non-null payload slices into their corresponding query cache keys', async () => {
    const qc = makeQueryClient()
    const { useSystemMetricsStream } = await import('@/hooks/useSystemMetricsStream')
    renderHook(() => useSystemMetricsStream(true), { wrapper: wrapperWith(qc) })

    const payload = {
      health_snapshot: { status: 'ok' },
      metric_history_1h: [{ t: 1, v: 2 }],
      queries_summary: { total: 42 },
      slow_queries_count: 3,
      log_accounting: { rows: 1000 },
      metadata_storage: { bytes: 9999 },
      system_jobs: [{ id: 'job-1' }],
    }
    act(() => {
      capturedOnEvent!(JSON.stringify(payload))
    })

    expect(qc.getQueryData(['admin', 'health-snapshot'])).toEqual(payload.health_snapshot)
    expect(qc.getQueryData(['admin', 'metric-history-batch', '1h'])).toEqual(payload.metric_history_1h)
    expect(qc.getQueryData(['admin', 'overview', 'queries-summary'])).toEqual(payload.queries_summary)
    expect(qc.getQueryData(['admin', 'overview', 'slow-queries-count'])).toEqual(payload.slow_queries_count)
    expect(qc.getQueryData(['admin', 'overview', 'log-accounting'])).toEqual(payload.log_accounting)
    expect(qc.getQueryData(['admin', 'metadata-storage'])).toEqual(payload.metadata_storage)
    expect(qc.getQueryData(['system-jobs'])).toEqual(payload.system_jobs)
  })

  it('SKIPS null/undefined slices and preserves stale data from prior pushes', async () => {
    const qc = makeQueryClient()
    // Pre-seed the cache as if a healthy push had already happened.
    const stale = { status: 'ok', sampled_at: '2026-06-15T10:00:00Z' }
    const staleHistory = [{ t: 0, v: 1 }]
    qc.setQueryData(['admin', 'health-snapshot'], stale)
    qc.setQueryData(['admin', 'metric-history-batch', '1h'], staleHistory)
    qc.setQueryData(['admin', 'metadata-storage'], { bytes: 12345 })

    const { useSystemMetricsStream } = await import('@/hooks/useSystemMetricsStream')
    renderHook(() => useSystemMetricsStream(true), { wrapper: wrapperWith(qc) })

    // Next push: health sampler failed (null), history sampler missing
    // (undefined → omitted from JSON), only queries_summary refreshed.
    act(() => {
      capturedOnEvent!(
        JSON.stringify({
          health_snapshot: null,
          // metric_history_1h omitted entirely
          queries_summary: { total: 99 },
          metadata_storage: null,
        }),
      )
    })

    // Stale entries preserved — this is the intentional design.
    expect(qc.getQueryData(['admin', 'health-snapshot'])).toEqual(stale)
    expect(qc.getQueryData(['admin', 'metric-history-batch', '1h'])).toEqual(staleHistory)
    expect(qc.getQueryData(['admin', 'metadata-storage'])).toEqual({ bytes: 12345 })
    // Fresh slice did land.
    expect(qc.getQueryData(['admin', 'overview', 'queries-summary'])).toEqual({ total: 99 })
  })

  it('ignores malformed JSON event payloads without crashing the hook', async () => {
    const qc = makeQueryClient()
    const { useSystemMetricsStream } = await import('@/hooks/useSystemMetricsStream')
    renderHook(() => useSystemMetricsStream(true), { wrapper: wrapperWith(qc) })

    // The hook's try/catch must swallow this — no throw escapes into
    // the test. We also confirm a follow-up valid push still lands,
    // proving the hook is still wired up after the bad frame.
    expect(() => {
      act(() => {
        capturedOnEvent!('not-json-at-all')
      })
    }).not.toThrow()
    expect(qc.getQueryData(['admin', 'health-snapshot'])).toBeUndefined()

    act(() => {
      capturedOnEvent!(JSON.stringify({ health_snapshot: { status: 'ok' } }))
    })
    expect(qc.getQueryData(['admin', 'health-snapshot'])).toEqual({ status: 'ok' })
  })

  it('forwards the enabled=false gate to useServiceStream', async () => {
    const qc = makeQueryClient()
    const { useSystemMetricsStream } = await import('@/hooks/useSystemMetricsStream')
    renderHook(() => useSystemMetricsStream(false), { wrapper: wrapperWith(qc) })

    expect(lastEnabled).toBe(false)
    // onEvent is still registered (the underlying hook decides whether
    // to actually open the stream based on `enabled`), but the gate
    // value made it through.
  })
})
