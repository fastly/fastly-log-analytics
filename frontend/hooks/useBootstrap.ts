import { useQuery } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { useEffect } from 'react'
import { toService } from '@/types/api'

export function useBootstrap() {
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
  }, [query.data, setServices, setInitialized])

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
