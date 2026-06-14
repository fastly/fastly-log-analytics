'use client'

import { useEffect } from 'react'
import { usePathname } from 'next/navigation'
import { useShallow } from 'zustand/react/shallow'
import { useFilterStore } from '@/stores/filterStore'
import { useFilterPayload } from '@/hooks/useFilterPayload'

/**
 * Bidirectional sync between the global filterStore and the page URL.
 *
 * On mount: hydration is owned by `hydrateFilterStoreFromUrl` invoked
 * synchronously from QueryProvider's useState initializer — see
 * [lib/urlFilterHydration.ts](../lib/urlFilterHydration.ts). That ran
 * before this hook's first useEffect, so the store already reflects URL
 * state and we only need the write-back loop here.
 *
 * Subsequent store mutations rewrite those params via
 * `history.replaceState` so:
 *
 *   1. Browser back-nav to this page restores the user's filters
 *      (they're encoded in the URL, not just in volatile Zustand state).
 *   2. Copy-paste of the URL shares the dashboard view with another user.
 *   3. Hard refresh preserves the visible state.
 *
 * Why replaceState rather than `router.replace`: avoids triggering Next's
 * router refresh (which would refetch the page's data and re-mount
 * sub-trees). replaceState updates the URL silently — React state owns the
 * UI; the URL is just a mirror.
 */
export function useFilterUrlSync(): void {
  const pathname = usePathname()
  const { startTime, endTime, isAutoRange, relativeRange } = useFilterStore(
    useShallow(state => ({
      startTime: state.startTime,
      endTime: state.endTime,
      isAutoRange: state.isAutoRange,
      relativeRange: state.relativeRange,
    })),
  )
  const filterPayload = useFilterPayload()

  // Write store → URL on state changes or when path changes.
  useEffect(() => {
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
