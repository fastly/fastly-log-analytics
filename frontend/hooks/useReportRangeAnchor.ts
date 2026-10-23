'use client'

import { useMemo } from 'react'
import { useMounted } from '@/hooks/useMounted'
import { quantizeAnchor } from '@/lib/time-window'
import { useFilterStore } from '@/stores/filterStore'

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
