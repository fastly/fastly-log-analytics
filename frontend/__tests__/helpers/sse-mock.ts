import { vi } from 'vitest'

export function makeStreamResponse(messages: string[], status = 200): Response {
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
  return new Response(stream, { status })
}

export function makeNeverClosingStreamResponse(initialChunks: string[] = []): Response {
  const enc = new TextEncoder()
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of initialChunks) {
        controller.enqueue(enc.encode(chunk))
      }
    },
  })
  return new Response(stream, { status: 200 })
}

export type MockSSEStatus = 'idle' | 'streaming' | 'done' | 'error'

export function createUseSSEMock() {
  const start = vi.fn()
  const stop = vi.fn()
  const reset = vi.fn()
  const state: {
    lines: Array<Record<string, unknown>>
    status: MockSSEStatus
    error: string | null
    isDone: boolean
    start: typeof start
    stop: typeof stop
    reset: typeof reset
  } = {
    lines: [],
    status: 'idle',
    error: null,
    isDone: false,
    start,
    stop,
    reset,
  }
  return {
    state,
    start,
    stop,
    reset,
    resetState() {
      state.lines = []
      state.status = 'idle'
      state.error = null
      state.isDone = false
      start.mockClear()
      stop.mockClear()
      reset.mockClear()
    },
  }
}
