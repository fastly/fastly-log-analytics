import { useQuery, useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import { queryKeys, seedBootstrapDependentCaches } from '@/lib/query-keys'
import { useSessionRoleStore } from '@/stores/sessionRoleStore'
import { useServiceStore } from '@/stores/serviceStore'
import { usePopGeoStore } from '@/stores/popGeoStore'
import { useDebugStore } from '@/stores/debugStore'
import { DEBUG_RESPONSES_COOKIE } from '@/lib/utils'
import { useEffect, useSyncExternalStore } from 'react'
import { toService } from '@/types/api'

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

    // Seed the PoP geo map for the shared <PopLabel> (city/state/country
    // shown next to PoP codes everywhere). Parsed server-side from the
    // /datacenters cache; empty when the cache isn't populated.
    usePopGeoStore.getState().setMap(data.pop_geo ?? {})

    const popsRaw = data.pops
    if (popsRaw) {
      const locations: Record<string, [number, number]> = {}
      for (const [code, [lat, lon]] of Object.entries(popsRaw)) {
        if (lat != null && lon != null) locations[code] = [lat, lon]
      }
      usePopGeoStore.getState().setLocations(locations)
    }

    // When the SSR HydrationBoundary pre-populates the bootstrap cache,
    // useBootstrap's queryFn is skipped on the first client mount, so mirror
    // the role flag here too. See sessionRoleStore.
    useSessionRoleStore.getState().setIsRemoteAnalyst(
      data?.settings?.is_remote_analyst === true,
    )

    // Synchronize client-side debugStore with the backend-defined debug settings
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

    // Note: views + log-fields-catalog cache seeding now happens
    // inside the queryFn (synchronously after the fetch resolves) so
    // dependent hooks gated on bootstrap status find data already in
    // their target cache. Moving it here would re-introduce the race
    // where dependent hooks re-render before useEffect runs.
  }, [data, isRealQueryResult])

  // `isStale` gates the revert branch below. When true, query.data is a cached
  // snapshot that may predate the live service set (a refetch hasn't yet
  // replaced it — e.g. the 5-min staleTime window after a service was just
  // added). Reverting a just-selected activeServiceId against a stale snapshot
  // is the "Switch To a freshly-added service bounces back" bug.
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
      // HARDENING: only evict a selected service when the bootstrap snapshot is
      // fresh (just fetched, not a stale cached copy). A stale snapshot can be
      // missing a brand-new service the user just switched to — reverting off
      // it bounces them back to the previous service (and AppLayout then to
      // /admin). When stale, defer: the pending refetch lands a fresh list
      // (this effect re-runs on query.data change) and reconciles correctly
      // against real data. Switch-to call sites also invalidate bootstrap to
      // trigger that refetch promptly.
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
      const data = await unwrapClient(client.GET("/api/bootstrap"))
      // Mirror the caller's role so lib/api.ts's 401 handler can tell an
      // admin (must NOT be bounced to the analyst /share-login) from a remote
      // analyst whose session died (should be). See sessionRoleStore.
      useSessionRoleStore.getState().setIsRemoteAnalyst(
        data?.settings?.is_remote_analyst === true,
      )
      // Seed dependent caches INSIDE the queryFn so subscribers that
      // gate on `bootstrap === 'pending' → fire own fetch` find data
      // already in their target cache by the time React Query unblocks
      // them. Doing this in a useEffect outside the queryFn races:
      // bootstrap status transitions pending→success and the
      // dependent hook re-renders BEFORE useEffect runs, so its
      // `enabled` flips true and it queries an empty cache. Seeding
      // here closes that race.
      seedBootstrapDependentCaches(queryClient, data)
      return data
    },
    // Bootstrap returns the services list + role flags + analyst session
    // metadata — none of which change within a typical browsing session.
    // staleTime: 5min so revisits to ANY route within that window skip
    // the refetch and don't re-block AppLayout's loading flag.
    // gcTime: 30min keeps the cache entry alive across brief tab
    // backgrounding so returning to the tab doesn't pay the cold fetch.
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    // Bootstrap is the cold-path fan-out; during an incident the global
    // 2-retry policy multiplies its load. Keep the 4xx-skip (admin 401s must
    // never retry) but cap bootstrap at a SINGLE retry — the backend
    // single-flight + admin SSR dedup are the real storm guards, and
    // refetchOnMount / SSE invalidation / the manual Retry button still
    // recover a transient blip. See the 2026-06-23 bootstrap-storm outage.
    retry: (failureCount: number, error: unknown) => {
      const err = error as { status?: number; response?: { status?: number } } | null
      const status = err?.status ?? err?.response?.status
      if (status !== undefined && status >= 400 && status < 500) return false
      return failureCount < 1
    },
    // Once that single retry is spent the query parks in `error` forever, and
    // AppLayout renders a dead-end "Can't reach the server" card that only
    // recovers if the user clicks Retry. A deploy takes the backend away for a
    // few seconds, so an open tab burns its one retry and strands the user on
    // that card long after the server is back.
    //
    // Poll — but ONLY while errored, which is what keeps the 2026-06-23
    // bootstrap-storm guard intact. This is a flat one-request-per-interval
    // per stranded client, not the exponential fan-out that caused the storm,
    // and it stops the instant a fetch succeeds. Jittered so a fleet of tabs
    // orphaned by the same restart doesn't rendezvous into a thundering herd.
    // refetchIntervalInBackground stays default-false: hidden tabs don't poll.
    //
    // Also poll every 2s while the backend reports it is still initializing
    // (first-run warmup) so AppLayout leaves its "Initializing" state as soon
    // as it is ready.
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
