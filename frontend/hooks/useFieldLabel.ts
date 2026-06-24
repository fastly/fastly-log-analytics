'use client'

import { useCallback } from 'react'
import { useLogFieldsCatalog } from './useLogFieldsCatalog'

/**
 * Returns a function that maps a log field id to its human-readable label.
 * Falls back to the raw id if the catalog hasn't loaded or the field is unknown.
 *
 * Wrapped in ``useCallback`` so the returned function identity is stable
 * across renders as long as the catalog hasn't changed — prevents
 * downstream React.memo'd consumers (DataTable header renderers, etc.)
 * from re-rendering on every parent re-render.
 */
export function useFieldLabel(): (colId: string) => string {
  const { data } = useLogFieldsCatalog()
  return useCallback(
    (colId: string) => data?.fields?.find((f: any) => f.id === colId)?.label ?? colId,
    [data],
  )
}
