import { getApiBase } from '@/lib/api'
import type { EventStreamRequest } from '@/hooks/useEventStream'

export interface BuildStreamRequestOptions {
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
