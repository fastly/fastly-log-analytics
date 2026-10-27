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
} from './sseStreamAppliers'
import { useEventStream } from './useEventStream'

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

export function useHeaderBadgeStream(enabled: boolean) {
  const queryClient = useQueryClient()
  return useServiceStream(enabled, '/api/log-extents/stream', (raw) => {
    try {
      const payload = JSON.parse(raw) as BadgeStreamEvent
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
    } catch {
      /* ignore */
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

  return adminStreamHooks.useServiceStream(
    enabled && bootstrapSettled && channelsParam.length > 0,
    `/api/admin/events/stream?channels=${channelsParam}`,
    (raw) => {
      let env: { channel?: string; data?: unknown }
      try {
        env = JSON.parse(raw) as { channel?: string; data?: unknown }
      } catch {
        return
      }
      if (!env || typeof env !== 'object') return
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
