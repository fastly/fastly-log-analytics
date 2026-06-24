import { useQuery, useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { queryKeys } from '@/lib/query-keys'
import { useAdminTokenStore } from '@/stores/adminTokenStore'
import { useServiceStore } from '@/stores/serviceStore'
import { usePopGeoStore } from '@/stores/popGeoStore'
import type { PopGeo } from '@/lib/pop'
import { useEffect } from 'react'
import { toService } from '@/types/api'

export function useBootstrap() {
  const queryClient = useQueryClient()
  const query = useQuery({
    queryKey: queryKeys.bootstrap(),
    queryFn: async () => {
      const { data } = await client.GET("/api/bootstrap")
      // Phase Q: capture the shared-secret admin token (null when
      // ADMIN_SHARED_SECRET is unset on the backend, or when bootstrap
      // is hit on the analyst branch) so the fetch interceptor in
      // lib/api.ts can inject X-Admin-Token on subsequent admin calls.
      const adminToken = (data as any)?.settings?.admin_token
      const nextToken = typeof adminToken === 'string' && adminToken ? adminToken : null
      const prevToken = useAdminTokenStore.getState().token
      useAdminTokenStore.getState().setToken(nextToken)
      // Restart-warmup recovery. When the SSR bootstrap fetch failed (backend
      // still booting → the helper returns null), no token is seeded at render
      // time, so the admin cards/tables fire their first queries with no
      // X-Admin-Token → 401 admin_token_required — and React Query does NOT
      // retry a 4xx, so they'd sit errored until the next poll / refocus or a
      // manual reload. This queryFn is the client-side bootstrap fetch that
      // runs on exactly that SSR-miss path; once it lands the token
      // (empty → present), refetch the queries that errored so /admin
      // self-heals. Guarded on the transition so the happy path (token seeded
      // synchronously by <HydrateAdminToken>, so this queryFn is short-
      // circuited by the cache) never triggers it; the predicate scopes it to
      // errored queries, not a blanket refetch. The bootstrap query itself is
      // pending→success here, so it isn't matched (no self-invalidation loop).
      if (nextToken && !prevToken) {
        queryClient.invalidateQueries({ predicate: (q) => q.state.status === 'error' })
      }
      // Seed dependent caches INSIDE the queryFn so subscribers that
      // gate on `bootstrap === 'pending' → fire own fetch` find data
      // already in their target cache by the time React Query unblocks
      // them. Doing this in a useEffect outside the queryFn races:
      // bootstrap status transitions pending→success and the
      // dependent hook re-renders BEFORE useEffect runs, so its
      // `enabled` flips true and it queries an empty cache. Seeding
      // here closes that race.
      if (data?.active_service_id) {
        const sid = data.active_service_id
        const seededViews = (data as any).views
        if (Array.isArray(seededViews)) {
          queryClient.setQueryData(['views', sid], seededViews)
        }
        const seededCatalog = (data as any).log_fields_catalog
        if (seededCatalog) {
          queryClient.setQueryData(['log-fields-catalog', sid], seededCatalog)
        }
        // Admin-only; analyst sessions get null from the backend.
        const seededSyncStatus = (data as any).sync_status
        if (seededSyncStatus) {
          queryClient.setQueryData(['sync-status', sid], seededSyncStatus)
        }
        // Available to both admin and analyst.
        const seededLogExtents = (data as any).log_extents
        if (seededLogExtents) {
          queryClient.setQueryData(['log-extents', sid], seededLogExtents)
        }
        // Schema seed for /query — only fires when the cron-populated
        // status cache has a real schema list, so a cold-start bootstrap
        // (empty schema) still falls through to /api/schema's live
        // get_schema fallback instead of caching an empty payload.
        const seededSchemaList = (data as any).schema
        const seededTableName = (data as any).table_name
        if (Array.isArray(seededSchemaList) && seededSchemaList.length > 0 && typeof seededTableName === 'string') {
          queryClient.setQueryData(['admin', 'schema', sid], { schema: seededSchemaList, table_name: seededTableName })
        }
        // Admin-only: /logs cron tab schedule tiles + tab-independent
        // recent-runs delta. Keys mirror useLogsPageState exactly.
        const seededCronSchedule = (data as any).cron_schedule
        if (seededCronSchedule) {
          queryClient.setQueryData(['admin', 'cron-schedule', sid], seededCronSchedule)
        }
        const seededCronRunsFirstPage = (data as any).cron_runs_first_page
        if (seededCronRunsFirstPage) {
          queryClient.setQueryData(['admin', 'cron-logs-recent', sid], seededCronRunsFirstPage)
        }
        // "Last Sync" header badge. Without this seed every admin page
        // load fires GET /api/cron-runs?task=sync&per_page=1 on mount
        // PLUS 1-2 SSE-invalidation-driven refetches.
        const seededLastSync = (data as any).last_sync
        if (seededLastSync) {
          queryClient.setQueryData(['last-sync', sid], seededLastSync)
        }
        // /scoring/labels seed — TopFlaggedTable + admin Labels tab +
        // dashboard Flag column all read from ['scoring-labels', sid].
        // Skipping the standalone fetch on cold load.
        const seededScoringLabels = (data as any).scoring_labels
        if (seededScoringLabels) {
          queryClient.setQueryData(['scoring-labels', sid], seededScoringLabels)
        }
      }
      // /admin/share page mounts a useQuery on ['admin','share','status']
      // that pays 187 ms p95 on cold load. Bootstrap now carries the
      // same payload — seed it so InvitationsPanel renders without
      // round-trip. ADMIN ONLY (analyst sessions get null from backend).
      const seededShareStatus = (data as any).share_status
      if (seededShareStatus) {
        queryClient.setQueryData(['admin', 'share', 'status'], seededShareStatus)
      }
      // /api/services returns {services, _section_timings} — bootstrap
      // exposes the same enriched list. Seed ServicesTable's queryKey
      // so /admin's cold-load round-trip evaporates.
      const seededServices = (data as any)?.services
      if (Array.isArray(seededServices)) {
        queryClient.setQueryData(['services'], { services: seededServices, _section_timings: [] })
      }
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
      const status = (error as { response?: { status?: number } } | null)?.response?.status
      if (status !== undefined && status >= 400 && status < 500) return false
      return failureCount < 1
    },
  })

  const activeServiceId = useServiceStore(state => state.activeServiceId)
  const setActiveServiceId = useServiceStore(state => state.setActiveServiceId)
  const setServices = useServiceStore(state => state.setServices)
  const setInitialized = useServiceStore(state => state.setInitialized)

  useEffect(() => {
    if (!query.data) return
    setServices((query.data.services ?? []).map(toService))
    setInitialized(true)

    // Seed the PoP geo map for the shared <PopLabel> (city/state/country
    // shown next to PoP codes everywhere). Parsed server-side from the
    // /datacenters cache; empty when the cache isn't populated.
    usePopGeoStore.getState().setMap((query.data.pop_geo ?? {}) as unknown as Record<string, PopGeo>)

    // Phase Q + RSC race fix: the SSR HydrationBoundary pre-populates
    // the bootstrap cache, which means useBootstrap's queryFn does NOT
    // run on the first client mount (the cache hit short-circuits it).
    // That left the setToken(...) call inside queryFn unreachable on
    // every SSR-hydrated load, so the Zustand store stayed empty and
    // every admin API call that fired before the next bootstrap
    // refetch 401'd with admin_token_required (see ServicesTable +
    // OperationsOverview cards on /admin).
    //
    // Mirror the token write here as well so it ALWAYS lands as soon
    // as query.data is available, regardless of whether the data came
    // from the network or from the SSR-hydrated cache. The queryFn
    // copy stays as the first-render fast path for the no-SSR
    // (pure-CSR) case.
    const adminToken = (query.data as any)?.settings?.admin_token
    useAdminTokenStore.getState().setToken(
      typeof adminToken === 'string' && adminToken ? adminToken : null,
    )

    // Note: views + log-fields-catalog cache seeding now happens
    // inside the queryFn (synchronously after the fetch resolves) so
    // dependent hooks gated on bootstrap status find data already in
    // their target cache. Moving it here would re-introduce the race
    // where dependent hooks re-render before useEffect runs.
  }, [query.data, setServices, setInitialized, queryClient])

  useEffect(() => {
    if (!query.data) return
    const services = (query.data.services ?? []).map(toService)
    const currentServiceExists = services.some(s => s.id === activeServiceId)

    if (!activeServiceId && services.length > 0) {
      const defaultId = query.data.active_service_id && services.some(s => s.id === query.data!.active_service_id)
        ? query.data.active_service_id
        : services[0]?.id
      if (defaultId) setActiveServiceId(defaultId)
    } else if (activeServiceId && !currentServiceExists) {
      const defaultId = services.length > 0 ? (
        (query.data.active_service_id && services.some(s => s.id === query.data!.active_service_id))
          ? query.data.active_service_id
          : services[0]?.id
      ) : null
      if (activeServiceId !== defaultId) setActiveServiceId(defaultId)
    }
  }, [query.data, activeServiceId, setActiveServiceId])

  return query
}
