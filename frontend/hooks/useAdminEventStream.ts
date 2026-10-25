'use client'

import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useBootstrapSettled } from '@/hooks/useBootstrap'
import { useEventStream, type EventStreamRequest, type SSEConnectionState } from '@/hooks/useEventStream'
import {
  applyShare,
  applySyncStatus,
  applySystemMetrics,
  makeCronRunsApplier,
  type CronRunsApplier,
} from '@/lib/admin-stream-apply'
import { queryKeys } from '@/lib/query-keys'
import { buildStreamRequest } from '@/lib/admin-stream-apply'
import { showToast, showToastWithAction } from '@/lib/utils'
import { copyToClipboard } from '@/lib/utils'
import { useServiceStore } from '@/stores/serviceStore'
import type { Schema } from '@/types/api'

interface UseServiceStreamOpts {
  cache?: RequestCache
  optionalService?: boolean
}

function useServiceStoreHydrated(): boolean {
  const persist = useServiceStore.persist
  const subscribe = useCallback(
    (onStoreChange: () => void) =>
      persist?.onFinishHydration?.(onStoreChange) ?? (() => {}),
    [persist],
  )
  const getSnapshot = () => persist?.hasHydrated?.() ?? true
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot)
}

/**
 * Subscribe to a service-scoped SSE channel and dispatch each ``data:``
 * payload to ``onEvent``.
 */
export function useServiceStream(
  enabled: boolean,
  path: string,
  onEvent: (raw: string) => void,
  opts: UseServiceStreamOpts = {},
): { state: SSEConnectionState } {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
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
}

type BootstrapHeaderBadge = Schema<'ShareHeaderBadgeResponse'>

interface BootstrapShape {
  header_badge?: BootstrapHeaderBadge | null
  [key: string]: unknown
}

type BadgeStreamEvent = Schema<'ShareHeaderBadgeResponse'>

export function useHeaderBadgeStream(enabled: boolean) {
  const queryClient = useQueryClient()

  return useServiceStream(
    enabled,
    '/api/log-extents/stream',
    (raw) => {
      try {
        const payload = JSON.parse(raw) as BadgeStreamEvent
        queryClient.setQueryData<BootstrapShape | undefined>(queryKeys.bootstrap(), (prev) => {
          if (!prev) return prev
          return {
            ...prev,
            header_badge: {
              ...(prev.header_badge ?? {}),
              rum: payload.rum === undefined ? prev.header_badge?.rum
                : payload.rum === null ? null : { ...prev.header_badge?.rum, ...payload.rum },
              request: payload.request === undefined ? prev.header_badge?.request
                : payload.request === null ? null : { ...prev.header_badge?.request, ...payload.request },
              latest_log_at: payload.latest_log_at !== undefined ? payload.latest_log_at : prev.header_badge?.latest_log_at ?? null,
              local_rows: payload.local_rows !== undefined ? payload.local_rows : prev.header_badge?.local_rows ?? null,
            },
          }
        })
      } catch {
        // Malformed payload; skip.
      }
    },
  )
}

export type AdminEventChannel = 'sync-status' | 'cron-runs' | 'system-metrics' | 'share' | 'celery-status'

export function useAdminEventStream(enabled: boolean, channels: AdminEventChannel[]) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const queryClient = useQueryClient()
  const bootstrapSettled = useBootstrapSettled()

  const channelsParam = useMemo(() => [...channels].sort().join(','), [channels])

  const cronApplierRef = useRef<CronRunsApplier | null>(null)
  useEffect(() => {
    if (!enabled || !channelsParam.split(',').includes('cron-runs')) {
      return
    }
    const applier = makeCronRunsApplier(queryClient, activeServiceId)
    cronApplierRef.current = applier
    return () => {
      applier.cleanup()
      if (cronApplierRef.current === applier) cronApplierRef.current = null
    }
  }, [enabled, activeServiceId, channelsParam, queryClient])

  const path = `/api/admin/events/stream?channels=${channelsParam}`

  return adminStreamHooks.useServiceStream(
    enabled && bootstrapSettled && channelsParam.length > 0,
    path,
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
          applySyncStatus(queryClient, activeServiceId, env.data)
          break
        case 'system-metrics':
          applySystemMetrics(queryClient, activeServiceId, env.data)
          break
        case 'share':
          applyShare(queryClient, env.data)
          break
        case 'celery-status':
          queryClient.setQueryData(queryKeys.admin.celeryStatus(), env.data)
          break
        case 'cron-runs': {
          if (!cronApplierRef.current) {
            cronApplierRef.current = makeCronRunsApplier(queryClient, activeServiceId)
          }
          cronApplierRef.current.apply(env.data)
          break
        }
        default:
          break
      }
    },
    { cache: 'no-store', optionalService: true },
  )
}

/**
 * Auto-resetting flash state with automatic unmount timer cleanup.
 */
export function useAutoFlash<T = true>(resetMs: number = 2000) {
  const [value, setValue] = useState<T | null>(null)
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const clearTimer = useCallback(() => {
    if (timerRef.current !== null) {
      clearTimeout(timerRef.current)
      timerRef.current = null
    }
  }, [])

  const clear = useCallback(() => {
    clearTimer()
    setValue(null)
  }, [clearTimer])

  const set = useCallback(
    (nextValue: T | null) => {
      clearTimer()
      setValue(nextValue)
    },
    [clearTimer],
  )

  useEffect(() => clearTimer, [clearTimer])

  const flash = useCallback(
    (nextValue: T = true as unknown as T) => {
      clearTimer()
      setValue(nextValue)
      timerRef.current = setTimeout(() => {
        timerRef.current = null
        setValue(null)
      }, resetMs)
    },
    [clearTimer, resetMs],
  )

  return {
    active: value !== null,
    value,
    flash,
    set,
    clear,
  }
}

/**
 * Copy a string to the clipboard and surface a "just copied" flag that
 * auto-resets after ``resetMs``.
 */
export function useCopyToClipboard(resetMs: number = 1500) {
  const { active: copied, flash } = useAutoFlash(resetMs)

  const copy = useCallback(
    async (value: string) => {
      if (await copyToClipboard(value)) {
        flash()
      }
    },
    [flash],
  )
  return { copied, copy }
}

interface DeferredDeleteOptions {
  id: string
  toastMessage: string
  performDelete: () => Promise<void>
  errorMessage: string | ((error: unknown) => string)
  durationMs?: number
}

/**
 * Shared 5.5s optimistic-hide + Undo-toast deferred deletion hook.
 */
export function useDeferredDeleteWithUndo() {
  const [pendingDeleteIds, setPendingDeleteIds] = useState<Set<string>>(() => new Set())
  const pendingTimersRef = useRef<Map<string, ReturnType<typeof setTimeout>>>(new Map())

  useEffect(() => {
    const timers = pendingTimersRef.current
    return () => {
      for (const [, t] of timers) clearTimeout(t)
      timers.clear()
    }
  }, [])

  const scheduleDelete = useCallback(
    ({ id, toastMessage, performDelete, errorMessage, durationMs = 5500 }: DeferredDeleteOptions) => {
      setPendingDeleteIds((prev) => {
        const next = new Set(prev)
        next.add(id)
        return next
      })
      const timer = setTimeout(async () => {
        pendingTimersRef.current.delete(id)
        try {
          await performDelete()
        } catch (err) {
          setPendingDeleteIds((prev) => {
            const next = new Set(prev)
            next.delete(id)
            return next
          })
          const msg = typeof errorMessage === 'function' ? errorMessage(err) : errorMessage
          showToast(msg, 'error')
        }
      }, durationMs)
      pendingTimersRef.current.set(id, timer)
      showToastWithAction(toastMessage, {
        actionLabel: 'Undo',
        onAction: () => {
          const t = pendingTimersRef.current.get(id)
          if (t) {
            clearTimeout(t)
            pendingTimersRef.current.delete(id)
          }
          setPendingDeleteIds((prev) => {
            const next = new Set(prev)
            next.delete(id)
            return next
          })
        },
        durationMs,
      })
    },
    [],
  )

  return { pendingDeleteIds, scheduleDelete }
}
