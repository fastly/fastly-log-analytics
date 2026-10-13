'use client'

import { useState, useCallback, useRef, useEffect } from 'react'
import { getApiBase } from '@/lib/api'
import { runEventStream } from '@/hooks/useEventStream'
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

  // AbortController for the active stream — stop()/unmount aborts it, which
  // cancels the underlying reader inside runEventStream.
  const abortRef = useRef<AbortController | null>(null)

  // Track the current request ID to avoid race conditions from StrictMode
  const requestIdRef = useRef<number>(0)
  // Track if component is mounted
  const mountedRef = useRef<boolean>(true)
  // Monotonic line counter for stable React keys (see SSELine._id).
  const lineSeqRef = useRef<number>(0)

  const stop = useCallback(() => {
    // 1. Abort the active connection (cancels the reader in runEventStream)
    if (abortRef.current) {
      try {
        abortRef.current.abort()
      } catch {
        /* best-effort */
      }
      abortRef.current = null
    }
    // 2. Invalidate any pending fetch by incrementing the request ID
    requestIdRef.current++;

    if (mountedRef.current) {
      setStatus('idle');
    }
  }, [])

  const start = useCallback(async (
    urlPath: string,
    body?: Record<string, unknown>,
    extraHeaders?: Record<string, string>,
  ) => {
    // Stop any existing stream before starting a new one
    stop();

    const serviceId = useServiceStore.getState().activeServiceId
    let finalUrlPath = urlPath
    if (serviceId) {
      const separator = finalUrlPath.includes('?') ? '&' : '?'
      const isPathParameterized = finalUrlPath.includes('/services/')
      if (!finalUrlPath.includes('service=') && !isPathParameterized) {
        finalUrlPath = `${finalUrlPath}${separator}service=${encodeURIComponent(serviceId)}`
      }
    }
    const url = `${getApiBase()}${finalUrlPath}`

    if (mountedRef.current) {
      setLines([])
      setStatus('streaming')
      setError(null)
      lineSeqRef.current = 0
    }

    const currentReqId = requestIdRef.current
    const abort = new AbortController()
    abortRef.current = abort
    const isActive = () => currentReqId === requestIdRef.current && mountedRef.current

    try {
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
      // injects X-Admin-Token on the openapi-fetch path but the raw
      // fetch inside runEventStream bypasses that middleware, so
      // without this the stream 401-loops silently inside the catch.
      const adminToken = useAdminTokenStore.getState().token
      if (adminToken) {
        headers['X-Admin-Token'] = adminToken
      }
      if (extraHeaders) {
        Object.assign(headers, extraHeaders)
      }

      await runEventStream({
        url,
        method: body ? 'POST' : 'GET',
        cache: 'no-store',
        headers,
        body: body ? JSON.stringify(body) : undefined,
        signal: abort.signal,
        isActive,
        onFrame: (frame) => {
          const part = frame.trim()
          if (!part.startsWith('data:')) return
          const dataStr = part.replace(/^data:\s*/, '')
          let data: SSELine
          try {
            data = JSON.parse(dataStr) as SSELine
          } catch (e) {
            console.error('Failed to parse SSE message:', dataStr, e)
            return
          }

          let finalStatus: SSEStatus | null = null
          let finalError: string | null = null
          if (data.type === 'done') {
            finalStatus = 'done'
          } else if (data.type === 'error') {
            finalStatus = 'error'
            finalError = typeof data.message === 'string' ? data.message : 'Unknown error'
          }

          if (mountedRef.current) {
            const stamped = { ...data, _id: ++lineSeqRef.current }
            setLines((prev) => [...prev, stamped])
            if (finalStatus) {
              setStatus(finalStatus)
              if (finalError) setError(finalError)
            }
          }
        },
      })

      if (!isActive()) return

      // Stream ended without a 'done' event — if we are still 'streaming',
      // that means the connection was closed unexpectedly before a terminal
      // 'done' or 'error' event was sent. This is an error!
      setStatus((prev) => {
        if (prev === 'streaming') {
          setError('Connection closed unexpectedly before completion.')
          return 'error'
        }
        return prev
      })
    } catch (err) {
      // If we unmounted or the request was replaced, ignore all errors
      if (!isActive()) return

      setStatus('error')
      if (err instanceof TypeError && (err.message === 'Failed to fetch' || err.message.includes('network error'))) {
        setError('Backend API unreachable. Ensure the FastAPI server is running (usually on port 8000).')
      } else {
        setError(err instanceof Error ? err.message : 'Unknown error')
      }
    } finally {
      if (abortRef.current === abort) {
        abortRef.current = null
      }
    }
  }, [stop])

  // Cleanup on unmount
  useEffect(() => {
    mountedRef.current = true
    return () => {
      mountedRef.current = false
      if (abortRef.current) {
        try {
          abortRef.current.abort()
        } catch {
          /* best-effort */
        }
        abortRef.current = null
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
