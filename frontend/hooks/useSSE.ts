'use client'

import { useState, useCallback, useRef, useEffect } from 'react'
import { buildServiceHref } from '@/lib/utils'
import { useServiceStore } from '@/stores/filterStore'
import { buildStreamRequest } from './sseStreamAppliers'
import { runEventStream } from './useEventStream'

export * from './sseStreamAppliers'
export * from './useEventStream'
export * from './useAdminStream'
export * from './useClipboardToast'
export * from './usePersistentPreferences'

// ── Imperative SSE Hook ──────────────────────────────────────────────────────

export type SSEStatus = 'idle' | 'streaming' | 'done' | 'error'
export interface SSELine {
  type?: string
  message?: string
  _id?: number
  [key: string]: unknown
}

export function useSSE({ maxLines }: { maxLines?: number } = {}) {
  const [lines, setLines] = useState<SSELine[]>([])
  const [status, setStatus] = useState<SSEStatus>('idle')
  const [error, setError] = useState<string | null>(null)
  const abortRef = useRef<AbortController | null>(null)
  const requestIdRef = useRef<number>(0)
  const mountedRef = useRef<boolean>(true)
  const lineSeqRef = useRef<number>(0)

  const stop = useCallback(() => {
    if (abortRef.current) {
      try {
        abortRef.current.abort()
      } catch {
        /* best-effort */
      }
      abortRef.current = null
    }
    requestIdRef.current++
    if (mountedRef.current) setStatus('idle')
  }, [])

  const start = useCallback(
    async (urlPath: string, body?: Record<string, unknown>, extraHeaders?: Record<string, string>) => {
      stop()
      const serviceId = useServiceStore.getState().activeServiceId
      let finalUrlPath = urlPath
      if (serviceId && !finalUrlPath.includes('service=') && !finalUrlPath.includes('/services/')) {
        finalUrlPath = buildServiceHref(finalUrlPath, serviceId)
      }
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
        const request = buildStreamRequest({
          path: finalUrlPath,
          serviceId,
          method: body ? 'POST' : 'GET',
          body: body ? JSON.stringify(body) : undefined,
          cache: 'no-store',
          extraHeaders: { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache', ...extraHeaders },
        })
        await runEventStream({
          ...request,
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
            if (data.type === 'done') finalStatus = 'done'
            else if (data.type === 'error') {
              finalStatus = 'error'
              finalError = typeof data.message === 'string' ? data.message : 'Unknown error'
            }
            if (mountedRef.current) {
              const stamped = { ...data, _id: ++lineSeqRef.current }
              setLines((prev) => {
                const next = [...prev, stamped]
                return maxLines && next.length > maxLines ? next.slice(next.length - maxLines) : next
              })
              if (finalStatus) {
                setStatus(finalStatus)
                if (finalError) setError(finalError)
              }
            }
          },
        })
        if (!isActive()) return
        setStatus((prev) => {
          if (prev === 'streaming') {
            setError('Connection closed unexpectedly before completion.')
            return 'error'
          }
          return prev
        })
      } catch (err) {
        if (!isActive()) return
        setStatus('error')
        if (err instanceof TypeError && (err.message === 'Failed to fetch' || err.message.includes('network error'))) {
          setError('Backend API unreachable. Ensure the FastAPI server is running (usually on port 8000).')
        } else {
          setError(err instanceof Error ? err.message : 'Unknown error')
        }
      } finally {
        if (abortRef.current === abort) abortRef.current = null
      }
    },
    [stop, maxLines],
  )

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
    stop()
    if (mountedRef.current) {
      setLines([])
      setStatus('idle')
      setError(null)
    }
  }, [stop])

  return { lines, status, isDone: status === 'done' || status === 'error', error, start, stop, reset }
}
