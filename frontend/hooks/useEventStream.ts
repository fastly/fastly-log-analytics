'use client'

import { useEffect, useRef, useState } from 'react'
import { parseSSEFrames } from '@/lib/sse-parser'

/**
 * Shared fetch-based SSE transport primitive.
 *
 * The frontend has three SSE consumers with distinct public APIs — the
 * imperative wizard/live-log consumer (``useSSE``), the declarative
 * service-scoped push channel (``useServiceStream``), and the multiplexed
 * admin stream (``useAdminEventStream``, itself an adapter over
 * ``useServiceStream``). They used to each hand-roll the same fetch +
 * ``ReadableStream`` read loop, ``TextDecoder`` + buffer framing, reader
 * cancellation, and reconnect bookkeeping.
 *
 * This module factors that out ONCE:
 *   - ``runEventStream`` — a single connection's read loop (fetch → decode →
 *     frame → dispatch), with abort + an extra liveness gate. Reused by the
 *     imperative ``useSSE`` and by the declarative ``useEventStream`` below.
 *   - ``useEventStream`` — the declarative hook: reconnect-forever with the
 *     1s→30s exponential backoff, connection-state surface, stale-callback
 *     ref, and abort-on-unmount. ``useServiceStream`` is a thin adapter over
 *     it.
 *
 * Framing is delegated to ``parseSSEFrames`` (``lib/sse-parser.ts``), which
 * preserves the multi-separator split (``\r\n\r\n`` | ``\n\n`` | ``\r\r``) —
 * sse-starlette emits CRLF, so a naive ``\n\n`` parser returns zero frames.
 */

/** Public connection-state for the declarative stream, surfaced for UI. */
export type SSEConnectionState = 'idle' | 'connecting' | 'open' | 'reconnecting'

/**
 * Error thrown by ``runEventStream`` on a non-2xx response. Carries the HTTP
 * status so callers can branch on it (e.g. stop reconnecting on 401/403)
 * without string-parsing the message. The message mirrors the historical
 * ``useSSE`` text so any UI rendering ``error`` is unchanged.
 */
export class EventStreamHttpError extends Error {
  readonly status: number
  constructor(status: number) {
    super(`HTTP error! status: ${status}`)
    this.name = 'EventStreamHttpError'
    this.status = status
  }
}

export interface RunEventStreamOptions {
  /** Fully-built absolute URL (caller owns URL + query-param construction). */
  url: string
  method?: 'GET' | 'POST'
  body?: BodyInit
  headers?: Record<string, string>
  cache?: RequestCache
  signal: AbortSignal
  /** Fired once the response is OK and the body reader is obtained. */
  onOpen?: () => void
  /** Fired for each complete SSE frame (raw frame text, pre-trim). */
  onFrame: (frame: string) => void
  /**
   * Extra liveness gate layered on top of the ``AbortSignal`` — used by the
   * imperative ``useSSE`` for its request-id / mounted race guard. When it
   * returns ``false`` mid-read, the loop cancels the reader and returns
   * without dispatching further frames.
   */
  isActive?: () => boolean
  /** Injectable fetch, for tests. Defaults to the global ``fetch``. */
  fetchImpl?: typeof fetch
}

/**
 * Run a single SSE connection to completion: fetch, read the body stream,
 * decode, split into frames, and dispatch each frame to ``onFrame``.
 *
 * Returns normally when the stream closes (server ``done`` or EOF). Throws
 * ``EventStreamHttpError`` on a non-2xx response, or propagates fetch/read
 * errors (e.g. ``TypeError`` for a network failure, ``AbortError`` on abort).
 */
export async function runEventStream(opts: RunEventStreamOptions): Promise<void> {
  const { url, method = 'GET', body, headers, cache, signal, onOpen, onFrame, isActive } = opts
  const fetchFn = opts.fetchImpl ?? fetch

  // Raw fetch (not the typed `client`): SSE needs ReadableStream access via
  // `response.body.getReader()`. openapi-fetch's middleware consumes the body
  // as JSON before it can be streamed.
  const response = await fetchFn(url, {
    method,
    ...(cache ? { cache } : {}),
    headers,
    body,
    signal,
  })

  // Liveness check after the fetch settles (stop()/unmount while pending).
  if (isActive && !isActive()) {
    try {
      response.body?.getReader().cancel().catch(() => {})
    } catch {
      /* best-effort */
    }
    return
  }

  if (!response.ok) {
    throw new EventStreamHttpError(response.status)
  }

  const reader = response.body?.getReader()
  if (!reader) {
    throw new Error('Response body is null')
  }

  // Ensure the reader is released when the connection is aborted — mirrors the
  // historical hand-rolled cleanup that called `reader.cancel()`.
  const onAbort = () => {
    try {
      reader.cancel().catch(() => {})
    } catch {
      /* best-effort */
    }
  }
  signal.addEventListener('abort', onAbort, { once: true })

  onOpen?.()

  const decoder = new TextDecoder()
  let buffer = ''

  try {
    while (true) {
      const { done, value } = await reader.read()

      if (isActive && !isActive()) {
        onAbort()
        return
      }

      if (done) break

      if (value) {
        buffer += decoder.decode(value, { stream: true })
        // SSE event-boundary is an empty line; the spec allows "\n\n",
        // "\r\n\r\n", or "\r\r". parseSSEFrames covers all three.
        const { frames, remainder } = parseSSEFrames(buffer)
        buffer = remainder
        for (const frame of frames) {
          onFrame(frame)
        }
      }
    }
  } finally {
    signal.removeEventListener('abort', onAbort)
  }
}

export interface EventStreamRequest {
  url: string
  method?: 'GET' | 'POST'
  body?: BodyInit
  headers?: Record<string, string>
  cache?: RequestCache
}

function requestKey(req: EventStreamRequest | null): string {
  if (!req) return ''
  // Headers are rebuilt with a stable key order each render, so JSON.stringify
  // yields a deterministic reconnect key.
  return [
    req.method ?? 'GET',
    req.url,
    req.cache ?? '',
    JSON.stringify(req.headers ?? {}),
    typeof req.body === 'string' ? req.body : '',
  ].join('\u0000')
}

export interface UseEventStreamOptions {
  /** Notified on every connection-state transition. */
  onStateChange?: (state: SSEConnectionState) => void
}

/**
 * Declarative reconnecting SSE hook. Opens ``request`` while ``enabled`` is
 * true, dispatches each ``data:`` frame's raw payload to ``onEvent``, and
 * reconnects forever with exponential backoff (1s → 30s cap), resetting the
 * backoff on every successful connect. Stops reconnecting on 401/403.
 *
 * ``onEvent`` is intentionally read through a ref and NOT a reconnect trigger —
 * callers pass an inline arrow that changes every render; including it would
 * tear down and reopen the stream on every render.
 *
 * Reconnects whenever ``request`` changes (url / headers / cache / method /
 * body), via a derived key.
 */
export function useEventStream(
  enabled: boolean,
  request: EventStreamRequest | null,
  onEvent: (raw: string) => void,
  opts: UseEventStreamOptions = {},
): { state: SSEConnectionState } {
  const [state, setState] = useState<SSEConnectionState>('idle')

  const onEventRef = useRef(onEvent)
  const onStateChangeRef = useRef(opts.onStateChange)
  const requestRef = useRef(request)
  // Keep the stale-callback / latest-request refs fresh WITHOUT making them
  // reconnect triggers. Updated in an effect (not during render) so the
  // react-hooks/refs rule is satisfied; declared before the connection effect
  // so the refs are current by the time a reconnect-triggering re-run reads
  // them. Initial mount is covered by the useRef initial values above.
  useEffect(() => {
    onEventRef.current = onEvent
    onStateChangeRef.current = opts.onStateChange
    requestRef.current = request
  })

  const key = requestKey(enabled ? request : null)

  useEffect(() => {
    const req = requestRef.current
    if (!enabled || !req) {
      // eslint-disable-next-line react-hooks/set-state-in-effect
      setState('idle')
      onStateChangeRef.current?.('idle')
      return
    }

    const abort = new AbortController()
    let cancelled = false
    let attempt = 0

    const safeSetState = (next: SSEConnectionState) => {
      if (!cancelled) {
        setState(next)
        onStateChangeRef.current?.(next)
      }
    }

    const run = async () => {
      while (!cancelled) {
        safeSetState(attempt === 0 ? 'connecting' : 'reconnecting')
        try {
          await runEventStream({
            url: req.url,
            method: req.method,
            body: req.body,
            headers: req.headers,
            cache: req.cache,
            signal: abort.signal,
            onOpen: () => {
              attempt = 0 // successful connect resets the backoff
              safeSetState('open')
            },
            onFrame: (frame) => {
              const dataLine = frame.split('\n').find((l) => l.startsWith('data:'))
              if (!dataLine) return
              const raw = dataLine.slice('data:'.length).trim()
              if (!raw) return
              onEventRef.current(raw)
            },
          })
        } catch (err) {
          if (cancelled) return
          // Auth failures are terminal — retrying just 403-loops.
          if (err instanceof EventStreamHttpError && (err.status === 401 || err.status === 403)) {
            cancelled = true
            return
          }
          // Otherwise fall through to backoff retry.
        }
        if (cancelled) return
        safeSetState('reconnecting')
        const delay = Math.min(30_000, 1_000 * 2 ** attempt++)
        await new Promise((resolve) => setTimeout(resolve, delay))
      }
    }

    run()

    return () => {
      cancelled = true
      abort.abort()
    }
    // `key` captures every reconnect-worthy change in `request`; `request`
    // itself is read from a ref so a new-identity object with the same key
    // doesn't churn the connection. onEvent is deliberately excluded (see doc).
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [enabled, key])

  return { state }
}
