'use client'

import { useShallow } from 'zustand/react/shallow'
import { useFilterStore } from '@/stores/filterStore'

/**
 * Active time-range selection (primary + compare). Subscribe here when a
 * component renders against the dashboard time window; do NOT bundle filter
 * pills or edgeOnly — those are separate concerns with their own consumers.
 */
export function useTimeRange() {
  return useFilterStore(
    useShallow(s => ({
      startTime: s.startTime,
      endTime: s.endTime,
      compareMode: s.compareMode,
      compareStartTime: s.compareStartTime,
      compareEndTime: s.compareEndTime,
    }))
  )
}
