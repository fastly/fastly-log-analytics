'use client'

import { useState, useCallback, useRef, useEffect } from 'react'
import { getApiBase } from '@/lib/api'
import { parseSSEFrames } from '@/lib/sse-parser'
import { useServiceStore } from '@/stores/serviceStore'
import { useAdminTokenStore } from '@/stores/adminTokenStore'

export type SSEStatus = 'idle' | 'streaming' | 'done' | 'error'

export interface SSELine {
  type?: string
  message?: string
  /**
   * Monotonic per-stream id assigned at append time. Stable React key for
   * components rendering an append-only / bounded SSE feed (e.g.
   * CronLiveLog) — index-based keys cause stale-DOM bleed when the array
   * is sliced for the visible tail.
   */
  _id?: number
  [key: string]: unknown
}

export function useSSE() {
  const [lines, setLines] = useState<SSELine[]>([])
  const [status, setStatus] = useState<SSEStatus>('idle')
  const [error, setError] = useState<string | null>(null)

  // Track the active stream reader
  const readerRef = useRef<ReadableStreamDefaultReader | null>(null)

  // Track the current request ID to avoid race conditions from StrictMode
  const requestIdRef = useRef<number>(0)
  // Track if component is mounted
  const mountedRef = useRef<boolean>(true)
  // Monotonic line counter for stable React keys (see SSELine._id).
  const lineSeqRef = useRef<number>(0)

  const stop = useCallback(() => {
    // 1. Cancel the reader if it exists
    if (readerRef.current) {
      try {
        readerRef.current.cancel().catch(() => {});
      } catch (e) {}
      readerRef.current = null;
    }
    // 2. Invalidate any pending fetch by incrementing the request ID
    requestIdRef.current++;

    if (mountedRef.current) {
      setStatus('idle');
    }
  }, [])

  const start = useCallback(async (urlPath: string, body?: Record<string, unknown>) => {
    // Stop any existing stream before starting a new one
    stop();

    // String concat (not `new URL(path, base)`): the public Fastly deploy's
    // getApiBase() returns "" for relative proxying, and `new URL(p, "")`
    // throws TypeError that the surrounding catch swallows into a silent
    // retry loop. See [[sse-hook-url-pitfall]].
    const url = `${getApiBase()}${urlPath}`

    if (mountedRef.current) {
      setLines([])
      setStatus('streaming')
      setError(null)
      lineSeqRef.current = 0
    }

    const currentReqId = requestIdRef.current

    try {
      const serviceId = useServiceStore.getState().activeServiceId
      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
        'Accept': 'text/event-stream',
        'Cache-Control': 'no-cache',
      }
      if (serviceId) {
        headers['x-service-id'] = serviceId
      }
      // Admin SSE endpoints (cron-runs stream, provision stream, etc.)
      // sit behind ADMIN_SHARED_SECRET when configured; lib/api.ts
      // injects X-Admin-Token on the openapi-fetch path but fetch()
      // here bypasses that middleware, so without this the stream
      // 401-loops silently inside the catch.
      const adminToken = useAdminTokenStore.getState().token
      if (adminToken) {
        headers['X-Admin-Token'] = adminToken
      }

      // Raw fetch (not typed `client`): SSE needs ReadableStream access via
      // `response.body.getReader()`. openapi-fetch's middleware consumes the
      // body as JSON before we can stream it.
      const response = await fetch(url.toString(), {
        method: body ? 'POST' : 'GET',
        cache: 'no-store',
        headers,
        body: body ? JSON.stringify(body) : undefined,
      })

      // If stop() was called or component unmounted while fetch was pending
      if (currentReqId !== requestIdRef.current || !mountedRef.current) {
        // Cancel the response body to avoid a connection leak
        const reader = response.body?.getReader()
        if (reader) {
          try { reader.cancel().catch(() => {}); } catch(e) {}
        }
        return
      }

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }

      const reader = response.body?.getReader()
      if (!reader) {
        throw new Error('Response body is null')
      }

      readerRef.current = reader
      const decoder = new TextDecoder()
      let buffer = ''

      while (true) {
        const { done, value } = await reader.read()

        // Always check if we're still the active request AND still mounted
        if (currentReqId !== requestIdRef.current || !mountedRef.current) {
          if (reader) try { reader.cancel().catch(() => {}); } catch(e) {}
          return;
        }

        if (done) break

        if (value) {
          buffer += decoder.decode(value, { stream: true })
          // SSE event-boundary is an empty line, which the spec allows as
          // "\n\n", "\r\n\r\n", or "\r\r". sse-starlette emits CRLF; the
          // prior hand-rolled backend emitted LF. parseSSEFrames covers
          // all three separators — see lib/sse-parser.ts.
          const { frames: parts, remainder } = parseSSEFrames(buffer)
          buffer = remainder

          const newLines: SSELine[] = []
          let finalStatus: SSEStatus | null = null
          let finalError: string | null = null

          for (let part of parts) {
            part = part.trim()
            if (part.startsWith('data:')) {
              const dataStr = part.replace(/^data:\s*/, '')
              try {
                const data = JSON.parse(dataStr) as SSELine
                newLines.push(data)
                if (data.type === 'done') {
                  finalStatus = 'done'
                } else if (data.type === 'error') {
                  finalStatus = 'error'
                  finalError = typeof data.message === 'string' ? data.message : 'Unknown error'
                }
              } catch (e) {
                console.error('Failed to parse SSE message:', dataStr, e)
              }
            }
          }

          if (mountedRef.current && newLines.length > 0) {
            const stamped = newLines.map((line) => ({ ...line, _id: ++lineSeqRef.current }))
            setLines((prev) => [...prev, ...stamped])
            if (finalStatus) {
              setStatus(finalStatus)
              if (finalError) setError(finalError)
            }
          }
        }
      }

      if (currentReqId !== requestIdRef.current || !mountedRef.current) return

      // Stream ended without a 'done' event — mark done if still streaming
      setStatus((prev) => (prev === 'streaming' ? 'done' : prev))
    } catch (err: any) {
      // If we unmounted or the request was replaced, ignore all errors
      if (currentReqId !== requestIdRef.current || !mountedRef.current) return

      setStatus('error')
      if (err instanceof TypeError && (err.message === 'Failed to fetch' || err.message.includes('network error'))) {
        setError('Backend API unreachable. Ensure the FastAPI server is running (usually on port 8000).')
      } else {
        setError(err instanceof Error ? err.message : 'Unknown error')
      }
    } finally {
      if (readerRef.current) {
        readerRef.current = null
      }
    }
  }, [stop])

  // Cleanup on unmount
  useEffect(() => {
    mountedRef.current = true
    return () => {
      mountedRef.current = false
      if (readerRef.current) {
        try { readerRef.current.cancel().catch(() => {}); } catch(e) {}
        readerRef.current = null
      }
    }
  }, [])

  const reset = useCallback(() => {
    stop();
    if (mountedRef.current) {
      setLines([])
      setStatus('idle')
      setError(null)
    }
  }, [stop])

  return {
    lines,
    status,
    isDone: status === 'done' || status === 'error',
    error,
    start,
    stop,
    reset,
  }
}
