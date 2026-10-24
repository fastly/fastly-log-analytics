import { getApiBase } from '@/lib/api'
import type { EventStreamRequest } from '@/hooks/useEventStream'

interface BuildStreamRequestOptions {
  /** Path (with any query string) appended to `getApiBase()`. */
  path: string
  serviceId?: string | null
  method?: 'GET' | 'POST'
  body?: BodyInit
  /** Merged last, so callers can override any default header. */
  extraHeaders?: Record<string, string>
  cache?: RequestCache
}

/**
 * Single source of the URL + auth/scope headers for raw-fetch SSE streams
 * (useSSE, useServiceStream). String concat, never `new URL()`: an empty
 * `getApiBase()` (relative proxying) must not throw.
 */
export function buildStreamRequest({
  path,
  serviceId,
  method,
  body,
  extraHeaders,
  cache,
}: BuildStreamRequestOptions): EventStreamRequest {
  return {
    url: `${getApiBase()}${path}`,
    ...(method ? { method } : {}),
    ...(body !== undefined ? { body } : {}),
    headers: {
      Accept: 'text/event-stream',
      ...(serviceId ? { 'x-service-id': serviceId } : {}),
      ...extraHeaders,
    },
    ...(cache ? { cache } : {}),
  }
}

/**
 * Shared SSE event-boundary parser.
 *
 * SSE specifies an empty line as the event separator, which the spec allows
 * as "\n\n", "\r\n\r\n", or "\r\r". sse-starlette emits CRLF; the previous
 * hand-rolled backend used LF. Splitting on "\n\n" alone misses every
 * sse-starlette event — the consumer's buffer just grows forever, no
 * frames are dispatched, and every useSSE/useServiceStream caller appears
 * stuck on "Waiting for stream...". The regex below covers all three
 * spec-allowed separators.
 *
 * Returns the complete frames from the buffer plus the remainder (any
 * trailing partial frame the caller should retain for the next chunk).
 */
export function parseSSEFrames(buffer: string): { frames: string[]; remainder: string } {
  const parts = buffer.split(/\r\n\r\n|\n\n|\r\r/)
  return { frames: parts.slice(0, -1), remainder: parts.at(-1) ?? "" }
}
