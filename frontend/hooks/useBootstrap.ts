'use client'

import { useCallback, useEffect, useMemo, useState, useSyncExternalStore } from 'react'
import { useSearchParams } from 'next/navigation'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useShallow } from 'zustand/react/shallow'
import { client, unwrapClient, queryKeys, seedBootstrapDependentCaches } from "@/lib/api";
import { DEBUG_RESPONSES_COOKIE } from "@/lib/utils";
import { useDebugStore } from '@/stores/filterStore'
import { useFilterStore } from '@/stores/filterStore'
import { usePopGeoStore } from '@/stores/filterStore'
import { useServiceStore } from '@/stores/filterStore'
import { useSessionRoleStore } from '@/stores/filterStore'
import { toService } from '@/types/api'
import { buildFiltersPayload, type FiltersPayload } from '@/types/api'

export {
  useUrlServiceSync,
  useServiceSwitchReset,
  useIsAnalyst,
  useMaskIps,
  useEnforceMaskedFilters,
  fetchWithTimeout,
  analystFetch,
  useAnalystHeartbeat,
  useAnalystLogout,
  type HeartbeatOptions,
} from './useAnalystAndServiceHooks'

/** Returns a debounced copy of value that only updates after delay ms of silence. */
export function useDebounce<T>(value: T, delay = 300): T {
  const [debounced, setDebounced] = useState<T>(value)
  useEffect(() => {
    const id = setTimeout(() => setDebounced(value), delay)
    return () => clearTimeout(id)
  }, [value, delay])
  return debounced
}

/**
 * Returns a memoised FiltersPayload built from the global filter store.
 */
export function useFilterPayload(includeEdgeOnly = false): FiltersPayload {
  const { filters, edgeOnly } = useFilterStore(
    useShallow(s => ({ filters: s.filters, edgeOnly: s.edgeOnly }))
  )
  return useMemo(() => {
    const payload = buildFiltersPayload(filters)
    if (includeEdgeOnly && edgeOnly) {
      payload['edge'] = { mode: 'include', values: ['true'] }
    }
    return payload
  }, [filters, edgeOnly, includeEdgeOnly])
}

/**
 * Same as `useFilterPayload`, but debounces the resulting payload so rapid
 * pill add/remove/toggleMode bursts collapse into a single React Query key
 * change.
 */
export function useDebouncedFilterPayload(includeEdgeOnly = false): FiltersPayload {
  return useDebounce(useFilterPayload(includeEdgeOnly), 250)
}

const subscribeNever = () => () => {}

/**
 * Returns false on the server and on the client's FIRST (hydration) render,
 * then true after mount.
 */
export function useMounted(): boolean {
  return useSyncExternalStore(
    subscribeNever,
    () => true,
    () => false,
  )
}

/**
 * Subscribe to a derived slice of the React Query `['bootstrap']` cache entry
 * via `useSyncExternalStore` without triggering network fetches.
 */
export function useBootstrapCacheSelector<T>(
  selector: (data: Record<string, unknown> | undefined) => T,
  serverFallback: T,
): T {
  const queryClient = useQueryClient()
  return useSyncExternalStore(
    (onChange) => queryClient.getQueryCache().subscribe(onChange),
    () => selector(queryClient.getQueryData<Record<string, unknown>>(queryKeys.bootstrap())),
    () => serverFallback,
  )
}

export function useEffectiveServiceId(): string | null | undefined {
  const stored = useServiceStore(s => s.activeServiceId)
  const isInitialized = useServiceStore(s => s.isInitialized)
  const searchParams = useSearchParams()
  const queryClient = useQueryClient()

  if (isInitialized) return stored
  if (stored) return stored

  const urlServiceId = searchParams?.get('service')
  if (urlServiceId) return urlServiceId

  const bootstrap = queryClient.getQueryData(queryKeys.bootstrap()) as
    | { active_service_id?: string | null }
    | undefined
  return bootstrap?.active_service_id ?? stored
}

export function useIsDataReady(): boolean {
  return !!useEffectiveServiceId()
}

export function useBootstrapResolved(): boolean {
  const queryClient = useQueryClient()
  return queryClient.getQueryData(queryKeys.bootstrap()) !== undefined
}

export function useBootstrapPending(): boolean {
  const queryClient = useQueryClient()
  const s = queryClient.getQueryState(queryKeys.bootstrap())
  return s !== undefined && s.status === 'pending'
}

export function useBootstrapSettled(): boolean {
  return useBootstrapCacheSelector(
    (bootstrapData) => bootstrapData !== undefined,
    false,
  )
}

let _lastSyncedBootstrapData: unknown = null
let _lastReconciledBootstrapState: {
  data: unknown
  isStale: boolean
  activeServiceId: string | null
} | null = null

export function useBootstrapSyncEffects(
  query: { data: unknown; isStale?: boolean; dataUpdatedAt?: number },
  activeServiceId?: string | null,
) {
  const storeActiveServiceId = useServiceStore((state) => state.activeServiceId)
  const resolvedActiveServiceId =
    activeServiceId === undefined ? storeActiveServiceId : activeServiceId
  const isRealQueryResult = typeof query.dataUpdatedAt === 'number'
  const data = query.data as
    | {
        services?: Parameters<typeof toService>[0][]
        active_service_id?: string | null
        pop_geo?: Parameters<ReturnType<typeof usePopGeoStore.getState>['setMap']>[0]
        pops?: Record<string, [number | null, number | null]>
        settings?: {
          is_remote_analyst?: boolean
        }
        debug_state?: {
          query_debug_visibility?: string
          api_call_debug_visibility?: string
        }
      }
    | undefined

  useEffect(() => {
    if (!data || !isRealQueryResult) return
    if (
      _lastSyncedBootstrapData === data &&
      useServiceStore.getState().isInitialized !== false
    ) {
      return
    }
    _lastSyncedBootstrapData = data
    const { setServices, setInitialized } = useServiceStore.getState()
    setServices((data.services ?? []).map(toService))
    setInitialized(true)

    usePopGeoStore.getState().setMap(data.pop_geo ?? {})

    const popsRaw = data.pops
    if (popsRaw) {
      const locations: Record<string, [number, number]> = {}
      for (const [code, [lat, lon]] of Object.entries(popsRaw)) {
        if (lat != null && lon != null) locations[code] = [lat, lon]
      }
      usePopGeoStore.getState().setLocations(locations)
    }

    useSessionRoleStore.getState().setIsRemoteAnalyst(
      data?.settings?.is_remote_analyst === true,
    )

    const isAnalyst = data?.settings?.is_remote_analyst === true
    const queryVis = data?.debug_state?.query_debug_visibility ?? 'disabled'
    const apiVis = data?.debug_state?.api_call_debug_visibility ?? 'disabled'

    const queryDebugEnabled = isAnalyst
      ? (queryVis === 'analysts' || queryVis === 'both')
      : (queryVis === 'admins' || queryVis === 'both')

    const apiCallsDebugEnabled = isAnalyst
      ? (apiVis === 'analysts' || apiVis === 'both')
      : (apiVis === 'admins' || apiVis === 'both')

    const debugStore = useDebugStore.getState()
    if (debugStore.enabled !== queryDebugEnabled || debugStore.apiCallsEnabled !== apiCallsDebugEnabled) {
      useDebugStore.setState({ enabled: queryDebugEnabled, apiCallsEnabled: apiCallsDebugEnabled })
      const shouldAttach = queryDebugEnabled || apiCallsDebugEnabled
      document.cookie = `${DEBUG_RESPONSES_COOKIE}=${shouldAttach ? '1' : '0'}; path=/; max-age=31536000; samesite=lax`
    }
  }, [data, isRealQueryResult])

  const bootstrapIsStale = Boolean(query.isStale)

  useEffect(() => {
    if (!data || !isRealQueryResult) return
    if (
      _lastReconciledBootstrapState &&
      _lastReconciledBootstrapState.data === data &&
      _lastReconciledBootstrapState.isStale === bootstrapIsStale &&
      _lastReconciledBootstrapState.activeServiceId === resolvedActiveServiceId &&
      useServiceStore.getState().isInitialized !== false
    ) {
      return
    }
    _lastReconciledBootstrapState = {
      data,
      isStale: bootstrapIsStale,
      activeServiceId: resolvedActiveServiceId,
    }
    const services = (data.services ?? []).map(toService)
    const currentServiceExists = services.some(s => s.id === resolvedActiveServiceId)

    if (!resolvedActiveServiceId && services.length > 0) {
      const defaultId = data.active_service_id && services.some(s => s.id === data.active_service_id)
        ? data.active_service_id
        : services[0]?.id
      if (defaultId) useServiceStore.getState().setActiveServiceId(defaultId)
    } else if (resolvedActiveServiceId && !currentServiceExists) {
      if (bootstrapIsStale) return
      const defaultId = services.length > 0 ? (
        (data.active_service_id && services.some(s => s.id === data.active_service_id))
          ? data.active_service_id
          : services[0]?.id
      ) : null
      if (resolvedActiveServiceId !== defaultId) useServiceStore.getState().setActiveServiceId(defaultId)
    }
  }, [data, bootstrapIsStale, resolvedActiveServiceId, isRealQueryResult])
}

export function useBootstrap() {
  const queryClient = useQueryClient()
  const query = useQuery({
    queryKey: queryKeys.bootstrap(),
    queryFn: async () => {
      const data = await unwrapClient(client.GET('/api/bootstrap'))
      useSessionRoleStore.getState().setIsRemoteAnalyst(
        data?.settings?.is_remote_analyst === true,
      )
      seedBootstrapDependentCaches(queryClient, data)
      return data
    },
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    retry: (failureCount: number, error: unknown) => {
      const err = error as { status?: number; response?: { status?: number } } | null
      const status = err?.status ?? err?.response?.status
      if (status !== undefined && status >= 400 && status < 500) return false
      return failureCount < 1
    },
    refetchInterval: query => {
      if (query.state.status === 'error') return 5000 + Math.floor(Math.random() * 2000)
      const settings = (query.state.data as { settings?: { initializing?: boolean } } | undefined)?.settings
      return settings?.initializing === true ? 2000 : false
    },
  })

  const activeServiceId = useServiceStore(state => state.activeServiceId)
  useBootstrapSyncEffects(query, activeServiceId)

  return query
}

/**
 * Returns the log fields catalog, optionally scoped to a service ID to include custom fields.
 */
export function useLogFieldsCatalog(serviceId?: string) {
  const effectiveSid = useEffectiveServiceId()
  const sid = serviceId ?? effectiveSid ?? undefined
  const bootstrapPending = useBootstrapPending()

  return useQuery({
    queryKey: queryKeys.logFieldsCatalog(sid),
    queryFn: async () => {
      return await unwrapClient(
        client.GET('/api/log-fields/catalog', {
          params: { query: { service_id: sid } },
        }),
      )
    },
    staleTime: Infinity,
    enabled: !bootstrapPending,
  })
}

/**
 * Returns a function that maps a log field id to its human-readable label.
 */
export function useFieldLabel(): (colId: string) => string {
  const { data } = useLogFieldsCatalog()
  return useCallback(
    (colId: string) => data?.fields?.find((f) => f.id === colId)?.label ?? colId,
    [data],
  )
}

interface UseFieldValuesOptions {
  field: string
  search: string
  limit?: number
  enabled?: boolean
}

export function useFieldValues({ field, search, limit = 50, enabled = true }: UseFieldValuesOptions) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const { startTime, endTime } = useFilterStore(
    useShallow(s => ({ startTime: s.startTime, endTime: s.endTime }))
  )
  const filterPayload = useDebouncedFilterPayload()

  return useQuery({
    queryKey: queryKeys.filtered('dashboard', 'field_values', activeServiceId, startTime, endTime, filterPayload, field, search),
    queryFn: async () => {
      return await unwrapClient(
        client.POST('/api/dashboard/field-values', {
          body: {
            start_time: startTime,
            end_time: endTime,
            filters: filterPayload,
            field,
            search,
            limit,
          },
        }),
      )
    },
    enabled: !!activeServiceId && enabled,
  })
}

/**
 * Single source of truth for "is this log field / field-group actually enabled
 * for the active service?" — derived from `bootstrap.active_log_field_ids`.
 */
export function useActiveLogFields() {
  const { data: bootstrap } = useBootstrap()
  const { data: catalog } = useLogFieldsCatalog()

  return useMemo(() => {
    const ids =
      (bootstrap as { active_log_field_ids?: string[] } | undefined)?.active_log_field_ids ?? []
    const activeIds = new Set<string>(ids)
    const ready = activeIds.size > 0

    const isFieldActive = (id: string): boolean => !ready || activeIds.has(id)

    const groups = (catalog?.groups ?? []) as Array<{ id: string; fields: string[] }>
    const isGroupActive = (groupId: string): boolean => {
      if (!ready) return true
      const grp = groups.find((g) => g?.id === groupId)
      return grp ? grp.fields.some((f) => activeIds.has(f)) : false
    }

    return { ready, isFieldActive, isGroupActive }
  }, [bootstrap, catalog])
}
