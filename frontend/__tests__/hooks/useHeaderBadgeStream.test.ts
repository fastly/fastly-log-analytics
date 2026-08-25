/**
 * useHeaderBadgeStream — analyst-safe push channel that merges
 * latest_log_at + local_rows into the bootstrap.header_badge slot.
 *
 * @vitest-environment jsdom
 */
import { renderHook, waitFor, act } from '@testing-library/react'
import { QueryClient } from '@tanstack/react-query'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

vi.mock('@/lib/api', () => ({
  getApiBase: () => 'http://test',
}))

// useHeaderBadgeStream now subscribes to bootstrap to detect analyst
// status reactively. Mock useBootstrap so every test can control
// is_remote_analyst. Each test that wants the hook to fire must set
// mockBootstrap = { settings: { is_remote_analyst: true } }.
const mockBootstrap: { data: any } = {
  data: { settings: { is_remote_analyst: true }, header_badge: {} },
}
vi.mock('@/hooks/useBootstrap', () => ({
  useBootstrap: () => mockBootstrap,
}))

let mockState: {
  activeServiceId: string | null
  services: Array<{ id: string; name: string; accessLevel: 'read_write' | 'read_only' }>
} = {
  activeServiceId: 'svc-1',
  services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_only' }],
}

vi.mock('@/stores/serviceStore', () => {
  const useServiceStore: any = vi.fn((selector?: (s: any) => any) =>
    selector ? selector(mockState) : mockState,
  )
  useServiceStore.getState = () => mockState
  return { useServiceStore }
})

function makeStreamResponse(messages: string[]): Response {
  const enc = new TextEncoder()
  let i = 0
  const stream = new ReadableStream({
    pull(controller) {
      if (i >= messages.length) {
        controller.close()
        return
      }
      controller.enqueue(enc.encode(messages[i]))
      i += 1
    },
  })
  return new Response(stream, { status: 200 })
}

function makeQueryClient(seedBootstrap?: Record<string, unknown>) {
  const qc = createTestQueryClient({ queries: { gcTime: 60_000, staleTime: 0 } })
  if (seedBootstrap) {
    qc.setQueryData(['bootstrap'], seedBootstrap)
  }
  return qc
}

function wrapperWith(qc: QueryClient) {
  return makeQueryWrapper(qc)
}

beforeEach(() => {
  vi.stubGlobal('fetch', vi.fn())
  mockState = {
    activeServiceId: 'svc-1',
    services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_only' }],
  }
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('useHeaderBadgeStream', () => {
  it('merges incoming payload into bootstrap.header_badge', async () => {
    const seed = { settings: { is_remote_analyst: true }, header_badge: { latest_log_at: '2026-06-15T22:00:00Z', local_rows: 1000 } }
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([
        `data: ${JSON.stringify({ latest_log_at: '2026-06-15T22:05:00Z', local_rows: 1234 })}\r\n\r\n`,
      ]),
    )

    const qc = makeQueryClient(seed)
    const { useHeaderBadgeStream } = await import('@/hooks/useHeaderBadgeStream')
    renderHook(() => useHeaderBadgeStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => {
      const bs = qc.getQueryData<any>(['bootstrap'])
      expect(bs?.header_badge?.latest_log_at).toBe('2026-06-15T22:05:00Z')
      expect(bs?.header_badge?.local_rows).toBe(1234)
      // Unrelated bootstrap fields should be preserved.
      expect(bs?.settings?.is_remote_analyst).toBe(true)
    })
  })

  it('does not crash when bootstrap cache is empty (cold load)', async () => {
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([
        `data: ${JSON.stringify({ latest_log_at: '2026-06-15T22:05:00Z', local_rows: 1234 })}\r\n\r\n`,
      ]),
    )

    const qc = makeQueryClient(/* no seed */)
    const { useHeaderBadgeStream } = await import('@/hooks/useHeaderBadgeStream')
    renderHook(() => useHeaderBadgeStream(true), { wrapper: wrapperWith(qc) })

    // Just give the stream time to drain. With no bootstrap entry,
    // the hook's setQueryData callback receives undefined and
    // returns it unchanged — no crash, no overwritten cache.
    // Wrap the drain in act() so the hook's setQueryData update lands
    // inside act (the SSE stream resolves async, so without this React 19
    // logs a "not wrapped in act(...)" warning for the late update).
    await act(async () => {
      await new Promise(r => setTimeout(r, 50))
    })
    expect(qc.getQueryData(['bootstrap'])).toBeUndefined()
  })

  it('does NOT open a connection when disabled is false', async () => {
    const qc = makeQueryClient({})
    const { useHeaderBadgeStream } = await import('@/hooks/useHeaderBadgeStream')
    renderHook(() => useHeaderBadgeStream(false), { wrapper: wrapperWith(qc) })
    await new Promise(r => setTimeout(r, 30))
    expect(fetch).not.toHaveBeenCalled()
  })

  it('uses /api/log-extents/stream (the analyst-safe endpoint, NOT /api/sync-status/stream)', async () => {
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([`data: ${JSON.stringify({ latest_log_at: 'x', local_rows: 1 })}\r\n\r\n`]),
    )

    const qc = makeQueryClient({})
    const { useHeaderBadgeStream } = await import('@/hooks/useHeaderBadgeStream')
    renderHook(() => useHeaderBadgeStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => expect(fetch).toHaveBeenCalled())
    const [url] = vi.mocked(fetch).mock.calls[0]
    expect(String(url)).toContain('/api/log-extents/stream')
    expect(String(url)).not.toContain('/api/sync-status/stream')
  })

  it('aborts on unmount', async () => {
    let abortSignal: AbortSignal | undefined
    vi.mocked(fetch).mockImplementation(async (_url: any, init?: any) => {
      abortSignal = (init as RequestInit | undefined)?.signal as AbortSignal
      const stream = new ReadableStream<Uint8Array>({ start() { /* never closes */ } })
      return new Response(stream, { status: 200 })
    })

    const qc = makeQueryClient({})
    const { useHeaderBadgeStream } = await import('@/hooks/useHeaderBadgeStream')
    const { unmount } = renderHook(() => useHeaderBadgeStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => expect(fetch).toHaveBeenCalled())
    expect(abortSignal?.aborted).toBe(false)
    act(() => unmount())
    expect(abortSignal?.aborted).toBe(true)
  })
})
