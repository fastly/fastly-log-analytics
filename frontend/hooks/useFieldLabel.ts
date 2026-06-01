'use client'

import { useLogFieldsCatalog } from './useLogFieldsCatalog'

/**
 * Returns a function that maps a log field id to its human-readable label.
 * Falls back to the raw id if the catalog hasn't loaded or the field is unknown.
 */
export function useFieldLabel(): (colId: string) => string {
  const { data } = useLogFieldsCatalog()
  return (colId: string) =>
    data?.fields?.find((f: any) => f.id === colId)?.label ?? colId
}
