'use client'

import { useQuery, useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { queryKeys } from '@/lib/query-keys'
import { useServiceStore } from '@/stores/serviceStore'

/** Returns the log fields catalog, optionally scoped to a service ID to include custom fields.
 * If no serviceId is provided, it defaults to the active service from the store.
 */
export function useLogFieldsCatalog(serviceId?: string) {
  const { activeServiceId } = useServiceStore()
  const sid = serviceId ?? activeServiceId ?? undefined
  const queryClient = useQueryClient()

  // Perf audit Phase D: useBootstrap seeds this query's cache with
  // the catalog payload that bootstrap now folds in. Without
  // coordination, this hook fires in PARALLEL with bootstrap and
  // beats the seed (the seeding useEffect runs AFTER bootstrap's
  // promise resolves, but useLogFieldsCatalog already started its
  // own fetch by then).
  //
  // Gate logic:
  //   - If bootstrap query is currently PENDING in this query client
  //     (someone is observing it), wait — its seeder will populate
  //     our cache shortly.
  //   - If bootstrap has no recorded state (never observed in this
  //     query client — e.g., standalone usage in a test that mocks
  //     /api/log-fields/catalog directly), fire normally.
  //   - If bootstrap has data already (warm), fire normally — React
  //     Query will return the seeded cache via queryKey match.
  const bootstrapState = queryClient.getQueryState(['bootstrap'])
  const bootstrapPending = bootstrapState !== undefined && bootstrapState.status === 'pending'

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
    enabled: !bootstrapPending,
  })
}
