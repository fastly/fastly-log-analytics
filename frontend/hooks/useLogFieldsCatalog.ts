'use client'

import { useCallback, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useShallow } from 'zustand/react/shallow'
import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import { queryKeys } from '@/lib/query-keys'
import { useBootstrapPending, useEffectiveServiceId } from '@/hooks/useIsDataReady'
import { useBootstrap } from '@/hooks/useBootstrap'
import { useServiceStore } from '@/stores/serviceStore'
import { useFilterStore } from '@/stores/filterStore'
import { useDebouncedFilterPayload } from '@/hooks/useFilterPayload'

/** Returns the log fields catalog, optionally scoped to a service ID to include custom fields.
 * If no serviceId is provided, it defaults to the active service from the store —
 * falling back to bootstrap.active_service_id when the persisted Zustand store
 * hasn't been populated yet (cold load + SSR-hydrated bootstrap cache). Without
 * that fallback the cache key would be ['log-fields-catalog'] (length-1) instead
 * of ['log-fields-catalog', sid] (length-2), missing the SSR seed.
 */
export function useLogFieldsCatalog(serviceId?: string) {
  const effectiveSid = useEffectiveServiceId()
  const sid = serviceId ?? effectiveSid ?? undefined

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
  const bootstrapPending = useBootstrapPending()

  return useQuery({
    queryKey: queryKeys.logFieldsCatalog(sid),
    queryFn: async () => {
      return await unwrapClient(
        client.GET('/api/log-fields/catalog', {
          // Use the resolved `sid` (which falls back to the active service)
          // so callers that don't pass a serviceId still get per-service
          // custom_fields. Previously this used the raw `serviceId` param
          // and silently dropped the fallback, so any UI that called
          // useLogFieldsCatalog() — including the main dashboard's column
          // header lookup — got the global catalog without custom labels.
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
 * Falls back to the raw id if the catalog hasn't loaded or the field is unknown.
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
 * for the active service?" — derived from `bootstrap.active_log_field_ids`
 * (the flat set of field-ids present in the active log format + enabled custom
 * fields).
 */
export function useActiveLogFields() {
  const { data: bootstrap } = useBootstrap()
  const { data: catalog } = useLogFieldsCatalog()

  return useMemo(() => {
    const ids =
      (bootstrap as { active_log_field_ids?: string[] } | undefined)?.active_log_field_ids ?? []
    const activeIds = new Set<string>(ids)
    // activeIds empty == bootstrap not ready yet → treat everything as active.
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
