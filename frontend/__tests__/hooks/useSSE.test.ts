/**
 * @vitest-environment jsdom
 *
 * useSSE — fetch-based SSE consumer used by every wizard, the cron live log,
 * the teardown dialog, and the deploy-to-Fastly button. If this hook breaks,
 * a lot of UI hangs without obvious symptoms.
 *
 * The hook reads from `response.body.getReader()` and parses chunks split on
 * `\n\n`. We mock fetch to return a controlled ReadableStream so the test
 * can drive the message sequence deterministically.
 */
import { renderHook, act } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('@/lib/api', () => ({
  getApiBase: () => 'http://test',
}))

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

beforeEach(() => {
  vi.stubGlobal('fetch', vi.fn())
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('useSSE', () => {
  it('starts in idle state with empty lines', async () => {
    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())
    expect(result.current.status).toBe('idle')
    expect(result.current.lines).toEqual([])
    expect(result.current.error).toBeNull()
  })

  it('captures data: events through to a terminal done event', async () => {
    const messages = [
      'data: {"type":"status","message":"starting"}\n\n',
      'data: {"type":"file_done","file_name":"a.gz"}\n\n',
      'data: {"type":"done","message":"all good"}\n\n',
    ]
    vi.mocked(fetch).mockResolvedValue(makeStreamResponse(messages))

    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/some-stream')
    })

    expect(result.current.status).toBe('done')
    expect(result.current.lines).toHaveLength(3)
    expect(result.current.lines[0]).toMatchObject({ type: 'status', message: 'starting' })
    expect(result.current.lines[2]).toMatchObject({ type: 'done' })
    expect(result.current.isDone).toBe(true)
  })

  it('captures an error event and stores the message', async () => {
    const messages = [
      'data: {"type":"status","message":"starting"}\n\n',
      'data: {"type":"error","message":"503 Service Unavailable"}\n\n',
    ]
    vi.mocked(fetch).mockResolvedValue(makeStreamResponse(messages))

    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/x')
    })

    expect(result.current.status).toBe('error')
    expect(result.current.error).toBe('503 Service Unavailable')
  })

  it('ignores `:` keepalive comment lines', async () => {
    const messages = [
      ':                              (keepalive)\n\n',
      'data: {"type":"status","message":"hello"}\n\n',
      ':                              (keepalive)\n\n',
      'data: {"type":"done"}\n\n',
    ]
    vi.mocked(fetch).mockResolvedValue(makeStreamResponse(messages))

    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/x')
    })

    // Only data: lines become entries — comments are silently dropped
    expect(result.current.lines).toHaveLength(2)
    expect(result.current.lines[0].message).toBe('hello')
  })

  it('handles a chunk that splits across read boundaries', async () => {
    // First chunk only partially contains the first message; second has the rest.
    const messages = [
      'data: {"type":"status","mes',
      'sage":"split"}\n\ndata: {"type":"done"}\n\n',
    ]
    vi.mocked(fetch).mockResolvedValue(makeStreamResponse(messages))

    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/x')
    })

    expect(result.current.lines.map(l => l.message)).toEqual(['split', undefined])
    expect(result.current.status).toBe('done')
  })

  it('parses CRLF event boundaries (sse-starlette wire format)', async () => {
    // sse-starlette flushes events as `data: ...\r\n\r\n` (RFC-spec
    // CRLF), not `\n\n`. Pre-fix, useSSE split on `\n\n` only — every
    // chunk arrived fine, but the buffer accumulated forever and no
    // `data:` line was ever extracted. Symptom: every consumer stuck
    // on "Waiting for stream..." despite a healthy 200 response.
    // Pinning the CRLF format here means a regression to LF-only
    // splitting fails this test even before anyone touches the
    // backend.
    const messages = [
      'data: {"type":"status","message":"starting"}\r\n\r\n',
      'data: {"type":"file_done","file_name":"a.gz"}\r\n\r\n',
      'data: {"type":"done","message":"ok"}\r\n\r\n',
    ]
    vi.mocked(fetch).mockResolvedValue(makeStreamResponse(messages))

    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/some-stream')
    })

    expect(result.current.status).toBe('done')
    expect(result.current.lines).toHaveLength(3)
    expect(result.current.lines[0]).toMatchObject({ type: 'status', message: 'starting' })
    expect(result.current.lines[1]).toMatchObject({ type: 'file_done', file_name: 'a.gz' })
    expect(result.current.lines[2]).toMatchObject({ type: 'done', message: 'ok' })
  })

  it('parses a buffer that mixes CRLF and LF event boundaries', async () => {
    // Defensive: if a proxy or intermediary normalises CRLF to LF
    // mid-stream, the parser should still cleanly split both halves.
    const messages = [
      'data: {"type":"status","message":"crlf"}\r\n\r\ndata: {"type":"done"}\n\n',
    ]
    vi.mocked(fetch).mockResolvedValue(makeStreamResponse(messages))

    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/x')
    })

    expect(result.current.lines.map(l => l.message)).toEqual(['crlf', undefined])
    expect(result.current.status).toBe('done')
  })

  it('reset() clears state back to idle', async () => {
    vi.mocked(fetch).mockResolvedValue(makeStreamResponse(['data: {"type":"done"}\n\n']))
    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/x')
    })
    expect(result.current.status).toBe('done')

    act(() => {
      result.current.reset()
    })

    expect(result.current.status).toBe('idle')
    expect(result.current.lines).toEqual([])
    expect(result.current.error).toBeNull()
  })

  it('surfaces a friendlier error when fetch fails with TypeError', async () => {
    vi.mocked(fetch).mockRejectedValue(new TypeError('Failed to fetch'))
    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/x')
    })

    expect(result.current.status).toBe('error')
    expect(result.current.error).toMatch(/Backend API unreachable/)
  })

  it('surfaces non-2xx HTTP status as an error', async () => {
    vi.mocked(fetch).mockResolvedValue(new Response('nope', { status: 500 }))
    const { useSSE } = await import('@/hooks/useSSE')
    const { result } = renderHook(() => useSSE())

    await act(async () => {
      await result.current.start('/api/x')
    })

    expect(result.current.status).toBe('error')
    expect(result.current.error).toMatch(/500/)
  })
})
