import { useEffect } from 'react'
import { useFilterStore } from '@/stores/filterStore'
import { useReportConfig } from './useReportConfig'
import { useActiveService } from './useActiveService'
import { client } from '@/lib/api'
import { useQueryClient } from '@tanstack/react-query'

// All sync URL → filterStore mapping (range, ?filters, ?filter_<col>=)
// now happens inside ``hydrateFilterStoreFromUrl`` BEFORE first render,
// so every consumer's first React Query key already reflects the URL.
// This hook only handles the two cases that can't run synchronously:
//
//   - ``?view=<id>`` — needs an async API call into /api/views/{sid}
//     (or the React Query cache the bootstrap seeds), so it has to wait
//     for ``activeServiceId`` to be known.
//   - ``?metric=<val>`` — feeds per-page useState in useReportConfig
//     (the report-level metric is a page-local choice, not on filterStore).
//
// Anything else in the URL has already been consumed and stripped by
// the time this effect runs.
export function useViewMetricUrlSync() {
  const addFilter = useFilterStore(s => s.addFilter)
  const clearFilters = useFilterStore(s => s.clearFilters)
  const setRange = useFilterStore(s => s.setRange)
  const setRelativeRange = useFilterStore(s => s.setRelativeRange)
  const toggleEdgeOnly = useFilterStore(s => s.toggleEdgeOnly)
  const toggleCompareMode = useFilterStore(s => s.toggleCompareMode)
  const setCompareRange = useFilterStore(s => s.setCompareRange)
  // Subscribe to the toggle-state values so the restore can compare without
  // poking `useFilterStore.getState()` (which doesn't exist on the test mock).
  const currentEdgeOnly = useFilterStore(s => s.edgeOnly)
  const currentCompareMode = useFilterStore(s => s.compareMode)
  const { activeServiceId } = useActiveService()
  const queryClient = useQueryClient()
  const { setMetric } = useReportConfig({
    defaultMetric: 'requests',
    defaultInterval: '1 minute',
    defaultTrend: 'off'
  })

  useEffect(() => {
    if (typeof window === 'undefined' || !activeServiceId) return

    const params = new URLSearchParams(window.location.search)
    const viewId = params.get('view')
    const urlMetric = params.get('metric')

    const loadView = async (id: string) => {
      // Prefer the views cache (seeded by /api/bootstrap or warmed by
      // ViewSelector's useQuery). Falls back to a direct GET only when
      // the cache is cold — keeps the legacy ?view=<id> deep-link path
      // working even before ViewSelector mounts.
      let views = queryClient.getQueryData(['views', activeServiceId]) as any
      if (!views) {
        const { data } = await client.GET("/api/views/{service_id}", {
          params: { path: { service_id: activeServiceId } }
        })
        views = data
      }
      const view = (views as any)?.find((v: any) => v.id === id)
      if (view) {
        // filters_json carries TWO shapes for backward compatibility:
        //   - Legacy (older saved views): a bare FilterPill[] array.
        //   - Current: { filters: FilterPill[], _view_extras: {...} }
        //     where extras carries edgeOnly / compareMode / relativeRange.
        // Detect by Array.isArray and apply each branch's recovery.
        const parsed = JSON.parse(view.filters_json)
        const isLegacy = Array.isArray(parsed)
        const pills = isLegacy ? parsed : parsed?.filters
        const extras = isLegacy ? null : parsed?._view_extras

        // Range — prefer relativeRange when extras carry one (rolling window
        // re-derived from now), else fall back to the saved absolute range.
        if (extras?.relativeRange) {
          // setRelativeRange records the label so the URL-sync round-trips
          // as ?range=<label> and reload re-derives [now-duration, now].
          setRelativeRange(extras.relativeRange, view.start_time, view.end_time)
        } else if (view.start_time && view.end_time) {
          setRange(view.start_time, view.end_time)
        }

        // Edge-only toggle — flip if it differs from current.
        if (extras?.edgeOnly != null && extras.edgeOnly !== currentEdgeOnly) {
          toggleEdgeOnly()
        }

        // Compare-mode toggle + range. Flip if needed; then overwrite range.
        if (extras?.compareMode != null && extras.compareMode !== currentCompareMode) {
          toggleCompareMode()
        }
        if (extras?.compareStartTime && extras?.compareEndTime) {
          setCompareRange(extras.compareStartTime, extras.compareEndTime)
        }

        clearFilters()
        if (Array.isArray(pills)) {
          pills.forEach((f: any) => addFilter(f.column, f.value, f.mode))
        }

        const url = new URL(window.location.href)
        url.searchParams.delete('view')
        window.history.replaceState({}, '', url.toString())
      }
    }

    if (viewId) {
      loadView(viewId)
      return
    }

    if (urlMetric) {
      setMetric(urlMetric)
      const url = new URL(window.location.href)
      url.searchParams.delete('metric')
      window.history.replaceState({}, '', url.toString())
    }
    // addFilter/setRange/setMetric/clearFilters are Zustand store methods —
    // their identities are stable across renders (Zustand returns the same
    // function refs), so listing them in deps documents the dependency
    // without re-firing the effect. The exhaustive-deps lint is happy and
    // a future reader sees the full data flow.
  }, [activeServiceId, addFilter, setRange, setRelativeRange, toggleEdgeOnly, toggleCompareMode, setCompareRange, setMetric, clearFilters, currentEdgeOnly, currentCompareMode, queryClient])
}
