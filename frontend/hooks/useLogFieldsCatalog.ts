'use client'

import { useQuery } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { queryKeys } from '@/lib/query-keys'
import { useServiceStore } from '@/stores/serviceStore'

/** Returns the log fields catalog, optionally scoped to a service ID to include custom fields. 
 * If no serviceId is provided, it defaults to the active service from the store.
 */
export function useLogFieldsCatalog(serviceId?: string) {
  const { activeServiceId } = useServiceStore()
  const sid = serviceId ?? activeServiceId ?? undefined
  
  return useQuery({
    queryKey: queryKeys.logFieldsCatalog(sid),
    queryFn: async () => {
      const { data } = await client.GET('/api/log-fields/catalog', {
        params: { query: { service_id: serviceId } },
      })
      return data as { fields?: any[]; groups?: any[]; insights?: any[]; presets?: any } | undefined
    },
    staleTime: Infinity,
  })
}
