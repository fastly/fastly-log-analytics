import { useQuery, useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { useEffect } from 'react'
import { toService } from '@/types/api'

export function useBootstrap() {
  const queryClient = useQueryClient()
  const query = useQuery({
    queryKey: ['bootstrap'],
    queryFn: async () => {
      const { data } = await client.GET("/api/bootstrap")
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
  })

  const activeServiceId = useServiceStore(state => state.activeServiceId)
  const setActiveServiceId = useServiceStore(state => state.setActiveServiceId)
  const setServices = useServiceStore(state => state.setServices)
  const setInitialized = useServiceStore(state => state.setInitialized)

  useEffect(() => {
    if (!query.data) return
    setServices((query.data.services ?? []).map(toService))
    setInitialized(true)

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
