/**
 * useSyncStatusStream — push channel that mirrors /api/sync-status/stream
 * events into the same React Query cache key useSyncStatus reads from.
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

let mockState: {
  activeServiceId: string | null
  services: Array<{ id: string; name: string; accessLevel: 'read_write' | 'read_only' }>
} = {
  activeServiceId: 'svc-1',
  services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_write' }],
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

function makeQueryClient() {
  // gcTime > 0 because this hook writes to the cache without
  // subscribing — useSyncStatus is the consumer. With gcTime: 0 the
  // written entry is immediately collected before waitFor reads it.
  return createTestQueryClient({ queries: { gcTime: 60_000, staleTime: 0 } })
}

function wrapperWith(qc: QueryClient) {
  return makeQueryWrapper(qc)
}

beforeEach(() => {
  vi.stubGlobal('fetch', vi.fn())
  mockState = {
    activeServiceId: 'svc-1',
    services: [{ id: 'svc-1', name: 'Test', accessLevel: 'read_write' }],
  }
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('useSyncStatusStream', () => {
  it('writes incoming SSE events into the [sync-status, serviceId] cache', async () => {
    const snapshot = { latest_log_at: '2026-06-15T10:00:00Z', local_rows: 100 }
    // Build a fresh Response per call — React StrictMode (and any
    // hook re-render with new deps) re-invokes the effect, and a
    // ReadableStream body can only be read once.
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([`data: ${JSON.stringify(snapshot)}\n\n`]),
    )

    const qc = makeQueryClient()
    const { useSyncStatusStream } = await import('@/hooks/useSyncStatusStream')
    renderHook(() => useSyncStatusStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => {
      expect(qc.getQueryData(['sync-status', 'svc-1'])).toEqual(snapshot)
    })
    expect(fetch).toHaveBeenCalled()
    const [url, init] = vi.mocked(fetch).mock.calls[0]
    expect(String(url)).toContain('/api/sync-status/stream')
    expect((init as RequestInit)?.headers).toMatchObject({
      'Accept': 'text/event-stream',
      'x-service-id': 'svc-1',
    })
  })

  it('does NOT open a connection when disabled is false', async () => {
    const qc = makeQueryClient()
    const { useSyncStatusStream } = await import('@/hooks/useSyncStatusStream')
    renderHook(() => useSyncStatusStream(false), { wrapper: wrapperWith(qc) })
    await new Promise(r => setTimeout(r, 30))
    expect(fetch).not.toHaveBeenCalled()
  })

  it('does NOT open a connection when activeServiceId is null', async () => {
    mockState = { activeServiceId: null, services: [] }
    const qc = makeQueryClient()
    const { useSyncStatusStream } = await import('@/hooks/useSyncStatusStream')
    renderHook(() => useSyncStatusStream(true), { wrapper: wrapperWith(qc) })
    await new Promise(r => setTimeout(r, 30))
    expect(fetch).not.toHaveBeenCalled()
  })

  it('skips malformed event payloads silently', async () => {
    const good = { latest_log_at: '2026-06-15T11:00:00Z', local_rows: 200 }
    vi.mocked(fetch).mockImplementation(async () =>
      makeStreamResponse([
        `data: not-json\n\n`,
        `data: ${JSON.stringify(good)}\n\n`,
      ]),
    )

    const qc = makeQueryClient()
    const { useSyncStatusStream } = await import('@/hooks/useSyncStatusStream')
    renderHook(() => useSyncStatusStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => {
      expect(qc.getQueryData(['sync-status', 'svc-1'])).toEqual(good)
    })
  })

  it('aborts the connection on unmount', async () => {
    let abortSignal: AbortSignal | undefined
    vi.mocked(fetch).mockImplementation(async (_url: any, init?: any) => {
      abortSignal = (init as RequestInit | undefined)?.signal as AbortSignal
      // Hand back a stream that never closes — only the abort can stop it.
      const stream = new ReadableStream<Uint8Array>({
        start() { /* hold the connection open */ },
      })
      return new Response(stream, { status: 200 })
    })

    const qc = makeQueryClient()
    const { useSyncStatusStream } = await import('@/hooks/useSyncStatusStream')
    const { unmount } = renderHook(() => useSyncStatusStream(true), { wrapper: wrapperWith(qc) })

    await waitFor(() => expect(fetch).toHaveBeenCalled())
    expect(abortSignal?.aborted).toBe(false)
    act(() => unmount())
    expect(abortSignal?.aborted).toBe(true)
  })
})
