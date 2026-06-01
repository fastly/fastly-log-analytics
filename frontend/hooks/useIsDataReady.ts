'use client'

import { useServiceStore } from '@/stores/serviceStore'
import { useFilterStore } from '@/stores/filterStore'
import { useShallow } from 'zustand/react/shallow'

/**
 * Returns true when the app has a selected service AND the date range is
 * settled (either user-specified or auto-range has synced from the backend).
 *
 * Replaces the per-page pattern:
 *   const isReady = !!activeServiceId && (!isAutoRange || hasSyncedExtents)
 */
export function useIsDataReady(): boolean {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const { isAutoRange, hasSyncedExtents } = useFilterStore(
    useShallow(s => ({ isAutoRange: s.isAutoRange, hasSyncedExtents: s.hasSyncedExtents }))
  )
  return !!activeServiceId && (!isAutoRange || hasSyncedExtents)
}
