'use client'

import { useMemo } from 'react'
import { useShallow } from 'zustand/react/shallow'
import { useMounted } from '@/hooks/useMounted'
import { quantizeAnchor } from '@/lib/time-window'
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

/**
 * Shared hook that resolves the `(relativeRange, isAutoRange, anchor)` tuple
 * for analytics report pages. Floors `anchor` to the 60s quantum, preferring
 * `nowServerStr` before client hydration (`!mounted`) so SSR-seeded query keys
 * byte-match on first paint.
 */
export function useReportRangeAnchor(nowServerStr?: string) {
  const mounted = useMounted()
  const relativeRange = useFilterStore((s) => s.relativeRange)
  const isAutoRange = useFilterStore((s) => s.isAutoRange)
  const storeEndTime = useFilterStore((s) => s.endTime)

  const anchor = useMemo(() => {
    const baseVal = !mounted && nowServerStr ? nowServerStr : storeEndTime
    return quantizeAnchor(baseVal || undefined)
  }, [storeEndTime, nowServerStr, mounted])

  return { relativeRange, isAutoRange, anchor }
}
