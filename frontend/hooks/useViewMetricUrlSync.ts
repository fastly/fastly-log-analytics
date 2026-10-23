import { useEffect } from 'react'
import { parseAsString, useQueryStates } from 'nuqs'
import { useFilterStore } from '@/stores/filterStore'
import { useActiveService } from './useActiveService'
import { useReportConfig } from './useReportConfig'
import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import { queryKeys } from '@/lib/query-keys'
import { useQueryClient } from '@tanstack/react-query'
import type { components } from '@/types/api.generated'
import type { FilterPill } from '@/types/filters'

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
const viewMetricParsers = { view: parseAsString, metric: parseAsString }
// Hoisted so nuqs hands back a stable setter and the effect below doesn't re-fire per render.
const viewMetricOptions = { history: 'replace', shallow: true } as const

export function useViewMetricUrlSync(options?: { setMetric?: (m: string) => void }) {
  // Consume-and-strip params: read once, apply, then clear from the URL.
  const [{ view: viewId, metric: urlMetric }, setUrlParams] = useQueryStates(viewMetricParsers, viewMetricOptions)
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
  const { setMetric: fallbackSetMetric } = useReportConfig({ defaultMetric: 'requests', defaultInterval: '1 minute', defaultTrend: 'off' })
  const queryClient = useQueryClient()
  const setMetric = options?.setMetric ?? fallbackSetMetric

  useEffect(() => {
    if (!activeServiceId) return

    const loadView = async (id: string) => {
      // Prefer the views cache (seeded by /api/bootstrap or warmed by
      // ViewSelector's useQuery). Falls back to a direct GET only when
      // the cache is cold — keeps the legacy ?view=<id> deep-link path
      // working even before ViewSelector mounts.
      let views = queryClient.getQueryData<components['schemas']['SavedViewRecord'][]>(queryKeys.views(activeServiceId))
      if (!views) {
        views = await unwrapClient(
          client.GET("/api/views/{service_id}", {
            params: { path: { service_id: activeServiceId } }
          }),
        )
      }
      const view = views?.find((v) => v.id === id)
      if (view) {
        // filters_json carries TWO shapes for backward compatibility:
        //   - Legacy (older saved views): a bare FilterPill[] array.
        //   - Current: { filters: FilterPill[], _view_extras: {...} }
        //     where extras carries edgeOnly / compareMode / relativeRange.
        // Detect by Array.isArray and apply each branch's recovery.
        let parsed: ReturnType<typeof JSON.parse>
        try {
          parsed = JSON.parse(view.filters_json)
        } catch {
          void setUrlParams({ view: null })
          return
        }
        const isLegacy = Array.isArray(parsed)
        const pills = isLegacy ? parsed : parsed?.filters
        const extras = isLegacy ? null : parsed?._view_extras

        // Read latest toggle state after any async fetch (falling back to
        // hook-subscribed state when useFilterStore is mocked in unit tests).
        const liveState =
          typeof useFilterStore.getState === 'function' ? useFilterStore.getState() : null
        const liveEdgeOnly = liveState ? liveState.edgeOnly : currentEdgeOnly
        const liveCompareMode = liveState ? liveState.compareMode : currentCompareMode

        // Edge-only toggle — flip if it differs from current.
        if (extras?.edgeOnly != null && extras.edgeOnly !== liveEdgeOnly) {
          toggleEdgeOnly()
        }

        // Compare-mode toggle + range MUST run BEFORE setRelativeRange / setRange
        // because toggleCompareMode() snaps startTime/endTime and clears relativeRange.
        if (extras?.compareMode != null && extras.compareMode !== liveCompareMode) {
          toggleCompareMode()
        }
        if (extras?.compareStartTime && extras?.compareEndTime) {
          setCompareRange(extras.compareStartTime, extras.compareEndTime)
        }

        // Range — prefer relativeRange when extras carry one (rolling window
        // re-derived from now), else fall back to the saved absolute range.
        if (view.start_time && view.end_time) {
          if (extras?.relativeRange) {
            // setRelativeRange records the label so the URL-sync round-trips
            // as ?range=<label> and reload re-derives [now-duration, now].
            setRelativeRange(extras.relativeRange, view.start_time, view.end_time)
          } else {
            setRange(view.start_time, view.end_time)
          }
        }

        clearFilters()
        if (Array.isArray(pills)) {
          pills.forEach((f: FilterPill) => addFilter(f.column, f.value, f.mode))
        }

        void setUrlParams({ view: null })
      }
    }

    if (viewId) {
      void loadView(viewId).catch(() => {})
      return
    }

    if (urlMetric) {
      setMetric?.(urlMetric)
      void setUrlParams({ metric: null })
    }
    // addFilter/setRange/setMetric/clearFilters are Zustand store methods —
    // their identities are stable across renders (Zustand returns the same
    // function refs), so listing them in deps documents the dependency
    // without re-firing the effect. The exhaustive-deps lint is happy and
    // a future reader sees the full data flow.
  }, [activeServiceId, addFilter, setRange, setRelativeRange, toggleEdgeOnly, toggleCompareMode, setCompareRange, setMetric, clearFilters, currentEdgeOnly, currentCompareMode, queryClient, viewId, urlMetric, setUrlParams])
}
