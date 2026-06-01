'use client'

import { useShallow } from 'zustand/react/shallow'
import { useServiceStore } from '@/stores/serviceStore'
import { useFilterStore } from '@/stores/filterStore'
import { useTimezoneStore } from '@/stores/timezoneStore'

/**
 * Compound hook that returns the three global store slices every page needs.
 * Replaces the three separate store reads at the top of every page component.
 */
export function usePageContext() {
  const { activeServiceId, services } = useServiceStore(
    useShallow(s => ({ activeServiceId: s.activeServiceId, services: s.services }))
  )
  const { startTime, endTime, filters, edgeOnly, compareMode, compareStartTime, compareEndTime } =
    useFilterStore(
      useShallow(s => ({
        startTime: s.startTime,
        endTime: s.endTime,
        filters: s.filters,
        edgeOnly: s.edgeOnly,
        compareMode: s.compareMode,
        compareStartTime: s.compareStartTime,
        compareEndTime: s.compareEndTime,
      }))
    )
  const timezone = useTimezoneStore(s => s.timezone)

  return {
    activeServiceId,
    services,
    startTime,
    endTime,
    filters,
    edgeOnly,
    compareMode,
    compareStartTime,
    compareEndTime,
    timezone,
  }
}
