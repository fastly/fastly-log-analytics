'use client'

import { useState, useCallback, useRef, useEffect, useMemo, useSyncExternalStore, type Dispatch, type SetStateAction } from 'react'
import type { VisibilityState } from '@tanstack/react-table'
import { runEventStream } from '@/hooks/useEventStream'
import { formatDate, formatRelative, formatTimeAgo, getTimezoneAbbr } from '@/lib/date'
import { buildStreamRequest } from '@/lib/admin-stream-apply'
import { buildServiceHref } from '@/lib/utils'
import { useServiceStore } from '@/stores/serviceStore'
import { useTimezoneStore } from '@/stores/timezoneStore'

export type SSEStatus = 'idle' | 'streaming' | 'done' | 'error'

export interface SSELine {
  type?: string
  message?: string
  _id?: number
  [key: string]: unknown
}

interface UseSSEOptions {
  maxLines?: number
}

export function useSSE({ maxLines }: UseSSEOptions = {}) {
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
    stop();

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
        extraHeaders: {
          'Content-Type': 'application/json',
          'Cache-Control': 'no-cache',
          ...extraHeaders,
        },
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
          if (data.type === 'done') {
            finalStatus = 'done'
          } else if (data.type === 'error') {
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
      if (abortRef.current === abort) {
        abortRef.current = null
      }
    }
  }, [stop, maxLines])

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

/**
 * Active timezone (IANA tz string).
 */
export function useTimezone(): string {
  return useTimezoneStore(s => s.timezone)
}

export function useDateFormat() {
  const timezone = useTimezoneStore(state => state.timezone)

  const short = useCallback((date: string | Date) => formatDate(date, timezone, 'yyyy-MM-dd h:mm a'), [timezone])
  const full = useCallback((date: string | Date) => formatDate(date, timezone, 'yyyy-MM-dd h:mm:ss a'), [timezone])
  const time = useCallback((date: string | Date) => formatDate(date, timezone, 'h:mm a'), [timezone])
  const timeMs = useCallback((date: string | Date) => formatDate(date, timezone, 'h:mm:ss.SSS a'), [timezone])
  const abbr = useCallback(() => getTimezoneAbbr(new Date(), timezone), [timezone])
  const relative = useCallback((date: string | Date) => formatRelative(date), [])
  const timeAgo = useCallback((date: string | Date) => formatTimeAgo(typeof date === 'string' ? date : date.toISOString()), [])
  const format = useCallback((date: string | Date, pattern: string) => formatDate(date, timezone, pattern), [timezone])

  return useMemo(() => ({
    short,
    full,
    time,
    timeMs,
    abbr,
    relative,
    timeAgo,
    format
  }), [short, full, time, timeMs, abbr, relative, timeAgo, format])
}

let _now = Date.now()
const _listeners = new Set<() => void>()
let _interval: ReturnType<typeof setInterval> | null = null

function _subscribe(cb: () => void): () => void {
  if (_listeners.size === 0) {
    _now = Date.now()
  }
  _listeners.add(cb)
  if (_interval === null) {
    _interval = setInterval(() => {
      _now = Date.now()
      for (const l of _listeners) l()
    }, 1000)
  }
  return () => {
    _listeners.delete(cb)
    if (_listeners.size === 0 && _interval !== null) {
      clearInterval(_interval)
      _interval = null
    }
  }
}

function _getSnapshot(): number {
  return _now
}

function _getServerSnapshot(): number {
  return 0
}

const _noopUnsubscribe = () => {}
function _noopSubscribe(): () => void {
  return _noopUnsubscribe
}

/**
 * Returns Date.now()-equivalent value that updates once per second.
 */
export function useNowMs(enabled = true): number {
  return useSyncExternalStore(
    enabled ? _subscribe : _noopSubscribe,
    _getSnapshot,
    _getServerSnapshot,
  )
}

/**
 * Returns seconds elapsed since `startedAt` using the shared 1-second tick.
 */
export function useElapsedTime(startedAt: string | null | undefined): number {
  const nowMs = useNowMs(Boolean(startedAt))
  if (!startedAt) return 0
  return Math.max(0, (nowMs - new Date(startedAt).getTime()) / 1000)
}

/**
 * State that starts from `source` and is reset to it whenever `source` changes.
 */
export function useDerivedState<T>(source: T): [T, Dispatch<SetStateAction<T>>] {
  const [state, setState] = useState(source)
  const [seen, setSeen] = useState(source)
  if (!Object.is(seen, source)) {
    setSeen(source)
    setState(source)
  }
  return [state, setState]
}

/**
 * Increments a counter each time `open` transitions to `true`.
 */
export function useOpenGeneration(open: boolean): number {
  const [generation, setGeneration] = useState(0)
  const [wasOpen, setWasOpen] = useState(open)
  if (open !== wasOpen) {
    setWasOpen(open)
    if (open) setGeneration((g) => g + 1)
  }
  return generation
}

export function useColumnVisibility(initial?: VisibilityState) {
  const [visibility, setVisibility] = useState<VisibilityState>(initial ?? {})
  const handleChange = useCallback(
    (id: string, vis: boolean) => setVisibility(prev => ({ ...prev, [id]: vis })),
    []
  )
  return [visibility, setVisibility, handleChange] as const
}

const storageListeners = new Set<() => void>()

function subscribeStorage(onChange: () => void): () => void {
  storageListeners.add(onChange)
  window.addEventListener('storage', onChange)
  return () => {
    storageListeners.delete(onChange)
    window.removeEventListener('storage', onChange)
  }
}

function readStorage(key: string): string | null {
  try {
    return window.localStorage.getItem(key)
  } catch {
    return null
  }
}

/**
 * A localStorage string as external state.
 */
export function useLocalStorageString(key: string): readonly [string | null, (value: string) => void] {
  const value = useSyncExternalStore(
    subscribeStorage,
    () => readStorage(key),
    () => null,
  )
  const write = useCallback(
    (next: string) => {
      try {
        window.localStorage.setItem(key, next)
      } catch {
        /* ignore quota / private-mode errors */
      }
      storageListeners.forEach((notify) => notify())
    },
    [key],
  )
  return [value, write] as const
}
