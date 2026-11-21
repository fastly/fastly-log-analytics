'use client'

import { useCallback, useEffect, useMemo, useRef, useSyncExternalStore } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useBootstrapSettled } from '@/hooks/useBootstrap'
import { queryKeys } from '@/lib/api'
import { useServiceStore } from '@/stores/filterStore'
import type { Schema } from '@/types/api'
import {
  type SSEConnectionState,
  type EventStreamRequest,
  type CronRunsApplier,
  buildStreamRequest,
  applySyncStatus,
  applySystemMetrics,
  applyShare,
  makeCronRunsApplier,
  parseSSEJson,
} from './sseStreamAppliers'
import { useEventStream } from './useEventStream'
import { IS_CLOUD_MODE, isCloudMode } from '@/lib/cloud-engine/mode'
import { observeCatalogCommit } from '@/lib/cloud-engine/dashboard/catalog-manifest'
import { notifyDashboardCatalogChange } from '@/lib/cloud-engine/dashboard/client'
import { browserSource } from '@/lib/cloud-engine/dashboard/sources'

// ── Service & Admin Multiplex Streams ────────────────────────────────────────

function useServiceStoreHydrated(): boolean {
  const persist = useServiceStore.persist
  const subscribe = useCallback(
    (onStoreChange: () => void) => persist?.onFinishHydration?.(onStoreChange) ?? (() => {}),
    [persist],
  )
  const getSnapshot = () => persist?.hasHydrated?.() ?? true
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot)
}

export function useServiceStream(
  enabled: boolean,
  path: string,
  onEvent: (raw: string) => void,
  opts: { cache?: RequestCache; optionalService?: boolean } = {},
): { state: SSEConnectionState } {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const storeHydrated = useServiceStoreHydrated()
  const { cache, optionalService = false } = opts
  const awaitingHydration = optionalService && !storeHydrated
  const effectiveEnabled = enabled && (optionalService || !!activeServiceId) && !awaitingHydration
  const request: EventStreamRequest | null = effectiveEnabled
    ? buildStreamRequest({ path, serviceId: activeServiceId, cache })
    : null
  return useEventStream(effectiveEnabled, request, onEvent)
}

export const adminStreamHooks = {
  useServiceStream,
  applySyncStatus,
  applySystemMetrics,
  applyShare,
  makeCronRunsApplier,
}

type BootstrapHeaderBadge = Schema<'ShareHeaderBadgeResponse'>
interface BootstrapShape {
  header_badge?: BootstrapHeaderBadge | null
  [key: string]: unknown
}
type BadgeStreamEvent = Schema<'ShareHeaderBadgeResponse'>

interface IngestEventFrame {
  id?: string
  event?: string
  retry?: number
  data?: {
    generation?: unknown
    manifest?: unknown
  }
}

function parseIngestEventFrame(raw: string): IngestEventFrame | null {
  let id: string | undefined
  let event: string | undefined
  let retry: number | undefined
  const dataLines: string[] = []
  for (const line of raw.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n')) {
    if (!line || line.startsWith(':')) continue
    const separator = line.indexOf(':')
    const field = separator === -1 ? line : line.slice(0, separator)
    const value = separator === -1 ? '' : line.slice(separator + 1).replace(/^ /, '')
    switch (field) {
      case 'id':
        id = value
        break
      case 'event':
        event = value
        break
      case 'retry': {
        const parsed = Number(value)
        if (Number.isSafeInteger(parsed) && parsed >= 0) retry = parsed
        break
      }
      case 'data':
        dataLines.push(value)
        break
    }
  }
  if (dataLines.length === 0) return { ...(id ? { id } : {}), ...(event ? { event } : {}), ...(retry !== undefined ? { retry } : {}) }
  try {
    const data = JSON.parse(dataLines.join('\n')) as unknown
    if (!data || typeof data !== 'object' || Array.isArray(data)) return null
    return {
      ...(id ? { id } : {}),
      ...(event ? { event } : {}),
      ...(retry !== undefined ? { retry } : {}),
      data: data as IngestEventFrame['data'],
    }
  } catch {
    return null
  }
}

export function useHeaderBadgeStream(enabled: boolean) {
  const queryClient = useQueryClient()
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const cursorRef = useRef<string | null>(null)
  const cloudActive = IS_CLOUD_MODE || isCloudMode()

  useEffect(() => {
    if (!enabled || !cloudActive) return
    cursorRef.current = null
    const controller = new AbortController()
    let stopped = false
    let timer: ReturnType<typeof setTimeout> | undefined
    let inFlight = false
    let resumePending = false
    let reconnectMs = 5_000
    const source = browserSource(
      typeof window !== 'undefined' ? window.location.origin : '',
      { serviceId: activeServiceId ?? undefined },
    )

    const poll = async () => {
      if (stopped || controller.signal.aborted || document.hidden || inFlight) return
      inFlight = true
      try {
        const url = new URL('/api/ingest/events', window.location.origin)
        if (cursorRef.current) url.searchParams.set('cursor', cursorRef.current)
        const response = await fetch(url.toString(), {
          headers: {
            Accept: 'text/event-stream',
            'x-service-id': activeServiceId ?? '',
          },
          credentials: 'include',
          signal: controller.signal,
        })
        if (response.status === 401 || response.status === 403) {
          stopped = true
          return
        }
        if (!response.ok) throw new Error(`Ingest event read failed (${response.status})`)
        if (stopped || controller.signal.aborted || document.hidden) return
        const parsed = parseIngestEventFrame(await response.text())
        if (stopped || controller.signal.aborted || document.hidden || !parsed) return
        if (parsed.retry !== undefined) {
          reconnectMs = Math.min(30_000, Math.max(1_000, parsed.retry))
        }
        if (parsed.id) cursorRef.current = parsed.id
        if (parsed.event === 'ready' || parsed.event === 'commit' || parsed.event === 'resync') {
          const generation = Number(parsed.data?.generation)
          if (Number.isSafeInteger(generation) && generation >= 0) {
            observeCatalogCommit(source.fetchJson, {
              type: parsed.event,
              generation,
              manifest: typeof parsed.data?.manifest === 'string' ? parsed.data.manifest : undefined,
            })
            notifyDashboardCatalogChange()
            if (parsed.event !== 'ready') {
              void queryClient.invalidateQueries({ queryKey: queryKeys.syncStatus(activeServiceId) })
            }
          }
        }
      } catch {
        /* transient requests reconnect on the last bounded retry interval */
      } finally {
        inFlight = false
        if (!stopped && !controller.signal.aborted && !document.hidden) {
          if (resumePending) {
            resumePending = false
            void poll()
          } else {
            timer = setTimeout(() => {
              timer = undefined
              void poll()
            }, reconnectMs)
          }
        }
      }
    }

    const onVisibilityChange = () => {
      if (timer !== undefined) clearTimeout(timer)
      timer = undefined
      resumePending = !document.hidden && inFlight
      if (!document.hidden && !inFlight) void poll()
    }

    document.addEventListener('visibilitychange', onVisibilityChange)
    void poll()
    return () => {
      stopped = true
      document.removeEventListener('visibilitychange', onVisibilityChange)
      if (timer !== undefined) clearTimeout(timer)
      controller.abort()
      cursorRef.current = null
    }
  }, [enabled, cloudActive, activeServiceId, queryClient])

  return useServiceStream(enabled && !cloudActive, '/api/log-extents/stream', (raw) => {
    const payload = parseSSEJson<BadgeStreamEvent>(raw)
    if (payload) {
      queryClient.setQueryData<BootstrapShape | undefined>(queryKeys.bootstrap(), (prev) => {
        if (!prev) return prev
        return {
          ...prev,
          header_badge: {
            ...(prev.header_badge ?? {}),
            rum:
              payload.rum === undefined
                ? prev.header_badge?.rum
                : payload.rum === null
                  ? null
                  : { ...prev.header_badge?.rum, ...payload.rum },
            request:
              payload.request === undefined
                ? prev.header_badge?.request
                : payload.request === null
                  ? null
                  : { ...prev.header_badge?.request, ...payload.request },
            latest_log_at:
              payload.latest_log_at !== undefined ? payload.latest_log_at : prev.header_badge?.latest_log_at ?? null,
            local_rows: payload.local_rows !== undefined ? payload.local_rows : prev.header_badge?.local_rows ?? null,
          },
        }
      })
    }
  })
}

export type AdminEventChannel = 'sync-status' | 'cron-runs' | 'system-metrics' | 'share' | 'celery-status'

export function useAdminEventStream(enabled: boolean, channels: AdminEventChannel[]) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const queryClient = useQueryClient()
  const bootstrapSettled = useBootstrapSettled()
  const channelsParam = useMemo(() => [...channels].sort().join(','), [channels])
  const cronApplierRef = useRef<CronRunsApplier | null>(null)

  useEffect(() => {
    if (!enabled || !channelsParam.split(',').includes('cron-runs')) return
    const applier = adminStreamHooks.makeCronRunsApplier(queryClient, activeServiceId)
    cronApplierRef.current = applier
    return () => {
      applier.cleanup()
      if (cronApplierRef.current === applier) cronApplierRef.current = null
    }
  }, [enabled, activeServiceId, channelsParam, queryClient])

  const cloudActive = IS_CLOUD_MODE || isCloudMode()
  return adminStreamHooks.useServiceStream(
    enabled && !cloudActive && bootstrapSettled && channelsParam.length > 0,
    `/api/admin/events/stream?channels=${channelsParam}`,
    (raw) => {
      const env = parseSSEJson<{ channel?: string; data?: unknown }>(raw)
      if (!env) return
      switch (env.channel) {
        case 'sync-status':
          adminStreamHooks.applySyncStatus(queryClient, activeServiceId, env.data)
          break
        case 'system-metrics':
          adminStreamHooks.applySystemMetrics(queryClient, activeServiceId, env.data)
          break
        case 'share':
          adminStreamHooks.applyShare(queryClient, env.data)
          break
        case 'celery-status':
          queryClient.setQueryData(queryKeys.admin.celeryStatus(), env.data)
          break
        case 'cron-runs':
          if (!cronApplierRef.current) {
            cronApplierRef.current = adminStreamHooks.makeCronRunsApplier(queryClient, activeServiceId)
          }
          cronApplierRef.current.apply(env.data)
          break
        default:
          break
      }
    },
    { cache: 'no-store', optionalService: true },
  )
}
