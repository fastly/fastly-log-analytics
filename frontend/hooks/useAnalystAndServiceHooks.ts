'use client'

import * as React from 'react'
import { startTransition, useEffect, useEffectEvent, useRef } from 'react'
import { useRouter } from 'next/navigation'
import { useQueryState } from 'nuqs'
import { getPageLoadId } from '@/lib/api'
import { isUserActive, msSinceLastInteraction, isIpFamilyField } from '@/lib/utils'
import { useFilterStore, useServiceStore } from '@/stores/filterStore'
import { useBootstrapCacheSelector } from './useBootstrap'

/**
 * Bidirectional sync between the active service ID and the URL's
 * `?service=` query param.
 */
export function useUrlServiceSync() {
  useUrlServiceToStore()
  useServiceUrlWriteback()
}

/** Selects the service a `?service=` URL names. */
export function useUrlServiceToStore() {
  const activeServiceId = useServiceStore(state => state.activeServiceId)
  const services = useServiceStore(state => state.services)
  const isInitialized = useServiceStore(state => state.isInitialized)
  const setActiveServiceId = useServiceStore(state => state.setActiveServiceId)
  const [urlService] = useQueryState('service', {
    history: 'replace',
    shallow: true,
  })

  useEffect(() => {
    if (!urlService) return
    if (urlService === activeServiceId) return
    if (isInitialized && !services.some(s => s.id === urlService)) return
    setActiveServiceId(urlService)
  }, [urlService, activeServiceId, services, isInitialized, setActiveServiceId])
}

/**
 * Writes the active service into `?service=`. Mount it inside the page's
 * Suspense boundary: a URL update that reaches the boundary before it has
 * hydrated makes React drop the server HTML and client-render the page.
 */
export function useServiceUrlWriteback() {
  const activeServiceId = useServiceStore(state => state.activeServiceId)
  const services = useServiceStore(state => state.services)
  const isInitialized = useServiceStore(state => state.isInitialized)
  const [urlService, setUrlService] = useQueryState('service', {
    history: 'replace',
    shallow: true,
  })

  useEffect(() => {
    if (!isInitialized) return
    const hasServices = services.length > 0
    const target = hasServices ? activeServiceId : null
    if (target === urlService) return
    if (urlService && services.some((s) => s.id === urlService)) return
    if (hasServices && activeServiceId && !services.some((s) => s.id === activeServiceId)) return
    startTransition(() => {
      setUrlService(target)
    })
  }, [activeServiceId, services, isInitialized, urlService, setUrlService])
}

/**
 * Invokes `callback(prevServiceId, nextServiceId)` whenever the active service
 * changes away from a previously non-null service ID (ignoring initial mount /
 * hydration from null -> initial service).
 */
export function useServiceSwitchReset(
  activeServiceId: string | null | undefined,
  callback: (prevServiceId: string, nextServiceId: string | null) => void,
) {
  const storeActiveServiceId = useServiceStore((s) => s.activeServiceId)
  const resolvedServiceId =
    activeServiceId === undefined ? storeActiveServiceId : activeServiceId
  const prevServiceRef = useRef<string | null>(resolvedServiceId)
  const onReset = useEffectEvent(callback)

  useEffect(() => {
    const prev = prevServiceRef.current
    if (prev && prev !== resolvedServiceId) {
      onReset(prev, resolvedServiceId)
    }
    prevServiceRef.current = resolvedServiceId
  }, [resolvedServiceId])
}

/**
 * Whether the current session is an "analyst" (read-only role).
 */
export function useIsAnalyst(): boolean {
  return useBootstrapCacheSelector(
    (bootstrapData) =>
      (bootstrapData?.settings as Record<string, unknown> | undefined)?.is_remote_analyst === true,
    false,
  )
}

/**
 * Whether the current session has client-IP PII masking enabled.
 */
export function useMaskIps(): boolean {
  return useBootstrapCacheSelector(
    (bootstrapData) =>
      (bootstrapData?.settings as { mask_ips?: boolean } | undefined)?.mask_ips === true,
    false,
  )
}

/**
 * Defense-in-depth UX guard for PII masking: strip any IP-family filter from
 * the filter store when the current session masks client IPs.
 */
export function useEnforceMaskedFilters(): void {
  const maskIps = useMaskIps()
  const filters = useFilterStore(s => s.filters)
  const removeFilter = useFilterStore(s => s.removeFilter)

  React.useEffect(() => {
    if (!maskIps) return
    for (const f of filters) {
      if (isIpFamilyField(f.column.replace(/_\d+$/, ''))) removeFilter(f.id)
    }
  }, [maskIps, filters, removeFilter])
}

/**
 * fetch() wrapper that aborts after `timeoutMs` (default 30s).
 */
export async function fetchWithTimeout(
  url: string,
  init: RequestInit = {},
  timeoutMs = 30_000,
): Promise<Response> {
  const ctrl = new AbortController()
  const timer = setTimeout(() => ctrl.abort(), timeoutMs)
  const callerSignal = init.signal
  const abortFromCaller = () => ctrl.abort()
  if (callerSignal) {
    if (callerSignal.aborted) {
      ctrl.abort()
    } else {
      callerSignal.addEventListener('abort', abortFromCaller, { once: true })
    }
  }
  try {
    return await fetch(url, { ...init, signal: ctrl.signal })
  } finally {
    clearTimeout(timer)
    callerSignal?.removeEventListener('abort', abortFromCaller)
  }
}

/**
 * Shared transport for remote-analyst requests (heartbeat, logout,
 * share-login, TOS acknowledge).
 */
export function analystFetch(path: string, init: RequestInit = {}, timeoutMs?: number): Promise<Response> {
  const loadId = getPageLoadId()
  return fetchWithTimeout(
    path,
    {
      ...init,
      credentials: 'include',
      headers: {
        'X-Remote-Analyst': '1',
        'X-User-Active': isUserActive() ? '1' : '0',
        ...(loadId ? { 'X-Page-Load-ID': loadId } : {}),
        ...init.headers,
      },
    },
    timeoutMs,
  )
}

export interface HeartbeatOptions {
  enabled: boolean
  idleAfterMs?: number
  intervalMs?: number
  failuresBeforeOverlay?: number
}

const DEFAULT_IDLE = 15_000
const DEFAULT_INTERVAL = 30_000
const DEFAULT_FAILURES = 2

export function useAnalystHeartbeat({
  enabled,
  idleAfterMs = DEFAULT_IDLE,
  intervalMs = DEFAULT_INTERVAL,
  failuresBeforeOverlay = DEFAULT_FAILURES,
}: HeartbeatOptions) {
  const router = useRouter()
  const [disconnected, setDisconnected] = React.useState(false)

  React.useEffect(() => {
    if (!enabled) return
    if (typeof window === 'undefined') return

    let timer: ReturnType<typeof setInterval> | null = null
    let cancelled = false
    let inFlight = false
    const controller = new AbortController()
    let consecutiveFailures = 0

    const tick = async () => {
      if (cancelled || inFlight) return
      if (document.hidden) return
      if (msSinceLastInteraction() < idleAfterMs) return
      inFlight = true
      try {
        const res = await analystFetch('/api/share/heartbeat', { signal: controller.signal }, 10_000)
        if (cancelled) return
        if (res.status === 401 || res.status === 403) {
          cancelled = true
          if (timer) clearInterval(timer)
          router.replace('/share-login')
          return
        }
        if (!res.ok) {
          consecutiveFailures++
        } else {
          consecutiveFailures = 0
          setDisconnected(false)
        }
      } catch {
        if (!cancelled) consecutiveFailures++
      } finally {
        inFlight = false
      }
      if (!cancelled && consecutiveFailures >= failuresBeforeOverlay) {
        setDisconnected(true)
      }
    }

    timer = setInterval(tick, intervalMs)

    return () => {
      cancelled = true
      controller.abort()
      if (timer) clearInterval(timer)
    }
  }, [enabled, idleAfterMs, intervalMs, failuresBeforeOverlay, router])

  return { disconnected }
}

/**
 * Self-service logout for remote analysts (and Cloud passkey sessions).
 */
export function useAnalystLogout(): { logout: () => Promise<void>; isLoggingOut: boolean } {
  const [isLoggingOut, setIsLoggingOut] = React.useState(false)

  const logout = React.useCallback(async () => {
    if (isLoggingOut) return
    setIsLoggingOut(true)
    try {
      const { IS_CLOUD_MODE } = await import('@/lib/cloud-engine/mode')
      if (IS_CLOUD_MODE) {
        const [{ clearDashboardBrowserCaches }, { clearOpsCaches }] = await Promise.all([
          import('@/lib/cloud-engine/dashboard/sources'),
          import('@/lib/cloud-engine/telemetry/sources'),
        ])
        await Promise.allSettled([
          analystFetch('/api/auth/logout', { method: 'POST' }, 10_000),
          clearDashboardBrowserCaches(),
          clearOpsCaches(),
        ])
      } else {
        await analystFetch('/api/share/logout', { method: 'POST' }, 10_000)
      }
    } catch {
      // Swallow — redirect anyway.
    } finally {
      if (typeof window !== 'undefined') {
        window.location.assign('/share-login')
      } else {
        setIsLoggingOut(false)
      }
    }
  }, [isLoggingOut])

  return { logout, isLoggingOut }
}
