'use client'

import { useMemo } from 'react'
import { useFilterStore } from '@/stores/filterStore'
import { useShallow } from 'zustand/react/shallow'
import { buildFiltersPayload, type FiltersPayload } from '@/types/filters'
import { useDebounce } from '@/hooks/useDebounce'

/**
 * Returns a memoised FiltersPayload built from the global filter store.
 *
 * @param includeEdgeOnly  When true, injects `{ edge: { mode: 'include', values: ['true'] } }`
 *                         if the user has the Edge-only toggle enabled.  Defaults to false.
 *
 * Replaces the per-page pattern:
 *   const filterPayload = React.useMemo(() => buildFiltersPayload(filters), [filters])
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
 * change. Use this in any path where the payload becomes part of a
 * `useQuery` key — a user adding 3 pills in quick succession should fire
 * one fetch, not three (each of the previous two cancelled mid-flight).
 *
 * Keep `useFilterPayload` (no debounce) for URL writeback and any path that
 * must reflect store state immediately.
 */
export function useDebouncedFilterPayload(includeEdgeOnly = false): FiltersPayload {
  return useDebounce(useFilterPayload(includeEdgeOnly), 250)
}
