'use client'

import { useEffect, useEffectEvent, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useBootstrapPending, useIsAnalyst } from '@/hooks/useBootstrap'
import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api'
import { queryKeys } from '@/lib/query-keys'
import { parseSSEFrames } from '@/lib/admin-stream-apply'
import { useServiceStore } from '@/stores/filterStore'
import type { components } from '@/types/api.generated'

/** Public connection-state for the declarative stream, surfaced for UI. */
export type SSEConnectionState = 'idle' | 'connecting' | 'open' | 'reconnecting'

/**
 * Error thrown by ``runEventStream`` on a non-2xx response.
 */
export class EventStreamHttpError extends Error {
  readonly status: number
  constructor(status: number) {
    super(`HTTP error! status: ${status}`)
    this.name = 'EventStreamHttpError'
    this.status = status
  }
}

interface RunEventStreamOptions {
  url: string
  method?: 'GET' | 'POST'
  body?: BodyInit
  headers?: Record<string, string>
  cache?: RequestCache
  signal: AbortSignal
  onOpen?: () => void
  onFrame: (frame: string) => void
  isActive?: () => boolean
  fetchImpl?: typeof fetch
}

/**
 * Run a single SSE connection to completion: fetch, read the body stream,
 * decode, split into frames, and dispatch each frame to ``onFrame``.
 */
export async function runEventStream(opts: RunEventStreamOptions): Promise<void> {
  const { url, method = 'GET', body, headers, cache, signal, onOpen, onFrame, isActive } = opts
  const fetchFn = opts.fetchImpl ?? fetch

  const response = await fetchFn(url, {
    method,
    ...(cache ? { cache } : {}),
    headers,
    body,
    signal,
  })

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
  return [
    req.method ?? 'GET',
    req.url,
    req.cache ?? '',
    JSON.stringify(req.headers ?? {}),
    typeof req.body === 'string' ? req.body : '',
  ].join('\u0000')
}

interface UseEventStreamOptions {
  onStateChange?: (state: SSEConnectionState) => void
}

/**
 * Declarative reconnecting SSE hook.
 */
export function useEventStream(
  enabled: boolean,
  request: EventStreamRequest | null,
  onEvent: (raw: string) => void,
  opts: UseEventStreamOptions = {},
): { state: SSEConnectionState } {
  const [state, setState] = useState<SSEConnectionState>('idle')

  const emitEvent = useEffectEvent(onEvent)
  const emitState = useEffectEvent((next: SSEConnectionState) => opts.onStateChange?.(next))
  const latestRequest = useEffectEvent(() => request)

  const key = requestKey(enabled ? request : null)

  useEffect(() => {
    const req = latestRequest()
    if (!enabled || !req) {
      emitState('idle')
      return
    }

    const abort = new AbortController()
    let cancelled = false
    let attempt = 0

    const safeSetState = (next: SSEConnectionState) => {
      if (!cancelled) {
        setState(next)
        emitState(next)
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
              attempt = 0
              safeSetState('open')
            },
            onFrame: (frame) => {
              const dataLine = frame.split('\n').find((l) => l.startsWith('data:'))
              if (!dataLine) return
              const raw = dataLine.slice('data:'.length).trim()
              if (!raw) return
              emitEvent(raw)
            },
          })
        } catch (err) {
          if (cancelled) return
          if (err instanceof EventStreamHttpError && (err.status === 401 || err.status === 403)) {
            cancelled = true
            return
          }
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
      setState('idle')
    }
  }, [enabled, key])

  return { state }
}

type KeyFn = (serviceId: string) => readonly unknown[]

/**
 * Pre-seed ``null`` into each per-service cache entry that does not exist yet,
 * then report ``true`` once that has happened.
 */
export function useSeededReady(
  serviceId: string | null | undefined,
  keyFns: readonly KeyFn[],
  readyWithoutService = false,
): boolean {
  const queryClient = useQueryClient()
  const [ready, setReady] = useState(false)
  useEffect(() => {
    if (serviceId) {
      for (const keyFn of keyFns) {
        const key = keyFn(serviceId)
        if (queryClient.getQueryData(key) === undefined) {
          queryClient.setQueryData(key, null)
        }
      }
    }
    const next = serviceId ? true : readyWithoutService
    queueMicrotask(() => {
      setReady(next)
    })
  }, [serviceId, queryClient, keyFns, readyWithoutService])
  return ready
}

const SEED_KEYS = [queryKeys.syncStatus] as const
const LAST_SYNC_SEED_KEYS = [queryKeys.lastSync] as const

export type SyncStatus = components['schemas']['SyncStatusResponse']
type CronRunEntry = components['schemas']['CronRunEntry']
type LastSyncInfo = { [K in 'started_at' | 'status' | 'duration_s']: CronRunEntry[K] | null }

/**
 * Single source of truth for `/api/sync-status`.
 */
export function useSyncStatus() {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const bootstrapPending = useBootstrapPending()
  const isAnalyst = useIsAnalyst()
  const ready = useSeededReady(activeServiceId, SEED_KEYS)

  return useQuery({
    queryKey: queryKeys.syncStatus(activeServiceId),
    queryFn: async ({ signal }) => {
      return (await unwrapClient(
        client.GET('/api/sync-status', {
          signal,
          params: { query: { skip_fos: true } },
        }),
      )) as SyncStatus
    },
    enabled: !!activeServiceId && !bootstrapPending && !isAnalyst && ready,
    staleTime: 60_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: false,
    retry: false,
  })
}

/**
 * Fetches the most recent ``sync``-task row from ``/api/cron-runs``,
 * surfaced as the "Last Sync: Xs ago" header badge.
 */
export function useLastSync() {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const isAnalyst = useIsAnalyst()
  const bootstrapPending = useBootstrapPending()
  const ready = useSeededReady(activeServiceId, LAST_SYNC_SEED_KEYS)

  return useQuery({
    queryKey: queryKeys.lastSync(activeServiceId),
    queryFn: async ({ signal }) => {
      const data = await unwrapClient(
        client.GET('/api/cron-runs', {
          signal,
          params: { query: { task: 'log_discovery', per_page: 1, page: 1, skip_total: true } },
        }),
      )
      const e = data?.entries?.[0]
      return {
        started_at: e?.started_at ?? null,
        status: e?.status ?? null,
        duration_s: e?.duration_s ?? null,
      } satisfies LastSyncInfo
    },
    enabled: !!activeServiceId && !bootstrapPending && !isAnalyst && ready,
    staleTime: 5 * 60_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: false,
    retry: false,
  })
}
