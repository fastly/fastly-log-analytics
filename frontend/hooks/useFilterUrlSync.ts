'use client'

import { useEffect, useRef } from 'react'
import { useShallow } from 'zustand/react/shallow'
import { useFilterStore } from '@/stores/filterStore'
import { useFilterPayload } from '@/hooks/useFilterPayload'
import type { FiltersPayload } from '@/types/filters'

/**
 * Bidirectional sync between the global filterStore and the page URL.
 *
 * On mount: hydrates startTime/endTime/filters from `?start_time`,
 * `?end_time`, and `?filters=<json>` URL params. Subsequent store
 * mutations rewrite those params via `history.replaceState` so:
 *
 *   1. Browser back-nav to this page restores the user's filters
 *      (they're encoded in the URL, not just in volatile Zustand state).
 *   2. Copy-paste of the URL shares the dashboard view with another user.
 *   3. Hard refresh preserves the visible state.
 *
 * Why one-shot hydration: without the `hydrated` ref guard, the initial
 * URL read would re-fire every time the store changes (because the read
 * effect runs after the write effect on the same tick), churning the
 * filter pills' UUIDs.
 *
 * Why replaceState rather than `router.replace`: avoids triggering Next's
 * router refresh (which would refetch the page's data and re-mount
 * sub-trees). replaceState updates the URL silently — React state owns the
 * UI; the URL is just a mirror.
 */
export function useFilterUrlSync(): void {
  const hydrated = useRef(false)
  const { startTime, endTime, setRange, addFilter, clearFilters } = useFilterStore(
    useShallow(state => ({
      startTime: state.startTime,
      endTime: state.endTime,
      setRange: state.setRange,
      addFilter: state.addFilter,
      clearFilters: state.clearFilters,
    })),
  )
  const filterPayload = useFilterPayload()

  // Hydrate from URL on first mount only.
  useEffect(() => {
    if (hydrated.current) return
    if (typeof window === 'undefined') return

    const params = new URLSearchParams(window.location.search)
    const qsStart = params.get('start_time')
    const qsEnd = params.get('end_time')
    const qsFilters = params.get('filters')

    if (qsStart && qsEnd) {
      setRange(qsStart, qsEnd)
    }

    if (qsFilters) {
      try {
        const parsed = JSON.parse(qsFilters) as FiltersPayload
        if (parsed && typeof parsed === 'object') {
          clearFilters()
          for (const [rawCol, spec] of Object.entries(parsed)) {
            if (!spec || !Array.isArray(spec.values)) continue
            // Strip the `_<n>` dedupe suffix the payload format adds
            // when the same column has both include + exclude buckets.
            const col = rawCol.replace(/_\d+$/, '')
            for (const v of spec.values) {
              addFilter(col, String(v), spec.mode === 'exclude' ? 'exclude' : 'include')
            }
          }
        }
      } catch {
        // Malformed ?filters= — ignore silently rather than break the page.
      }
    }

    hydrated.current = true
  }, [setRange, addFilter, clearFilters])

  // Write store → URL on subsequent state changes (after hydration).
  useEffect(() => {
    if (!hydrated.current) return
    if (typeof window === 'undefined') return

    const url = new URL(window.location.href)
    if (Object.keys(filterPayload).length > 0) {
      url.searchParams.set('filters', JSON.stringify(filterPayload))
    } else {
      url.searchParams.delete('filters')
    }
    if (startTime) {
      url.searchParams.set('start_time', startTime)
    } else {
      url.searchParams.delete('start_time')
    }
    if (endTime) {
      url.searchParams.set('end_time', endTime)
    } else {
      url.searchParams.delete('end_time')
    }
    window.history.replaceState({}, '', url.toString())
  }, [filterPayload, startTime, endTime])
}
