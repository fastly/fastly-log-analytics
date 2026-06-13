'use client'

import { useEffect, useRef } from 'react'
import { usePathname } from 'next/navigation'
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
// Map a pill label like "24h" / "3d" to its duration in hours. Returns null
// for anything that doesn't match — the hook then falls back to absolute
// start_time/end_time params (or to the store default).
function rangeLabelToHours(label: string): number | null {
  const m = /^(\d+)([hd])$/.exec(label)
  if (!m) return null
  const n = parseInt(m[1], 10)
  if (!Number.isFinite(n) || n <= 0) return null
  return m[2] === 'd' ? n * 24 : n
}

export function useFilterUrlSync(): void {
  const pathname = usePathname()
  const hydrated = useRef(false)
  const { startTime, endTime, isAutoRange, relativeRange, setRange, setRelativeRange, addFilter, clearFilters } = useFilterStore(
    useShallow(state => ({
      startTime: state.startTime,
      endTime: state.endTime,
      isAutoRange: state.isAutoRange,
      relativeRange: state.relativeRange,
      setRange: state.setRange,
      setRelativeRange: state.setRelativeRange,
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
    const qsRange = params.get('range')
    const qsStart = params.get('start_time')
    const qsEnd = params.get('end_time')
    const qsFilters = params.get('filters')

    // ?range= wins over ?start_time/?end_time so a bookmarked "rolling
    // last 24h" stays rolling. Absolute timestamps are only honored when
    // no relative range is present (saved views, chart-zoom links).
    const rangeHours = qsRange ? rangeLabelToHours(qsRange) : null
    if (qsRange && rangeHours !== null) {
      const now = new Date()
      const start = new Date(now.getTime() - rangeHours * 3600 * 1000).toISOString()
      setRelativeRange(qsRange, start, now.toISOString())
    } else if (qsStart && qsEnd) {
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
  }, [setRange, setRelativeRange, addFilter, clearFilters])

  // Write store → URL on subsequent state changes (after hydration) or when path changes
  useEffect(() => {
    if (!hydrated.current) return
    if (typeof window === 'undefined') return

    const url = new URL(window.location.href)
    if (Object.keys(filterPayload).length > 0) {
      url.searchParams.set('filters', JSON.stringify(filterPayload))
    } else {
      url.searchParams.delete('filters')
    }
    // Three modes:
    //   1. relativeRange set (pill click)        → ?range=<label>, no absolute times.
    //      Bookmarks track a rolling window: reload re-derives [now-d, now].
    //   2. !isAutoRange + no relativeRange       → ?start_time=&end_time= (absolute).
    //      Custom datetime, chart zoom, applied saved view — user pinned a window.
    //   3. isAutoRange (cold load, post-Reset)   → no time params.
    //      Store defaults to last 24h from page-load time; URL stays clean so
    //      reload picks up the new "now".
    if (relativeRange) {
      url.searchParams.set('range', relativeRange)
      url.searchParams.delete('start_time')
      url.searchParams.delete('end_time')
    } else if (!isAutoRange && startTime && endTime) {
      url.searchParams.set('start_time', startTime)
      url.searchParams.set('end_time', endTime)
      url.searchParams.delete('range')
    } else {
      url.searchParams.delete('start_time')
      url.searchParams.delete('end_time')
      url.searchParams.delete('range')
    }
    window.history.replaceState({}, '', url.toString())
  }, [filterPayload, startTime, endTime, isAutoRange, relativeRange, pathname])
}
