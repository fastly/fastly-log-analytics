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
        // Use the resolved `sid` (which falls back to the active service)
        // so callers that don't pass a serviceId still get per-service
        // custom_fields. Previously this used the raw `serviceId` param
        // and silently dropped the fallback, so any UI that called
        // useLogFieldsCatalog() — including the main dashboard's column
        // header lookup — got the global catalog without custom labels.
        params: { query: { service_id: sid } },
      })
      return data as { fields?: any[]; groups?: any[]; insights?: any[]; presets?: any } | undefined
    },
    staleTime: Infinity,
  })
}
