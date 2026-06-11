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

    // Seed the views cache from the bootstrap response so ViewSelector
    // and useUrlFilterSync skip their own /api/views/{id} round-trip on
    // initial load. The existing ['views', activeServiceId] query keeps
    // its semantics for service-switch — if the user switches to a
    // service not in this seed, the granular query fires normally.
    const seededActive = query.data.active_service_id
    const seededViews = (query.data as any).views
    if (seededActive && Array.isArray(seededViews)) {
      queryClient.setQueryData(['views', seededActive], seededViews)
    }

    // Seed the log-fields catalog cache from the bootstrap response so
    // useLogFieldsCatalog hits cache on first call instead of paying a
    // ~35 KB / 200 ms /api/log-fields/catalog round-trip on every cold
    // page load (perf audit Phase D). The dedicated endpoint stays for
    // any caller that bypasses the bootstrap seed (e.g. logging-out
    // analyst flows). Query key matches queryKeys.logFieldsCatalog().
    const seededCatalog = (query.data as any).log_fields_catalog
    if (seededActive && seededCatalog) {
      queryClient.setQueryData(['log-fields-catalog', seededActive], seededCatalog)
    }
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
