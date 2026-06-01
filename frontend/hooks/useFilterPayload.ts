'use client'

import { useMemo } from 'react'
import { useFilterStore } from '@/stores/filterStore'
import { useShallow } from 'zustand/react/shallow'
import { buildFiltersPayload, type FiltersPayload } from '@/types/filters'

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
