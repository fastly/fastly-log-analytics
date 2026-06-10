import { useEffect } from 'react'
import { useFilterStore } from '@/stores/filterStore'
import { useReportConfig } from './useReportConfig'
import { useActiveService } from './useActiveService'
import { client } from '@/lib/api'
import { useQueryClient } from '@tanstack/react-query'

export function useUrlFilterSync() {
  const { addFilter, clearFilters, setRange } = useFilterStore()
  const { activeServiceId } = useActiveService()
  const queryClient = useQueryClient()
  const { setMetric } = useReportConfig({
    defaultMetric: 'requests',
    defaultInterval: '1 minute',
    defaultTrend: 'off'
  })

  // Parse URL parameters on mount or when service changes
  useEffect(() => {
    if (typeof window === 'undefined' || !activeServiceId) return

    const params = new URLSearchParams(window.location.search)
    let updated = false

    const hasFilterParams = Array.from(params.keys()).some(k => k.startsWith('filter_'))
    const hasRangeParams = params.get('start_time') && params.get('end_time')
    const hasMetricParam = params.get('metric')
    const viewId = params.get('view')

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
        if (view.start_time && view.end_time) {
          setRange(view.start_time, view.end_time)
        }
        const parsedFilters = JSON.parse(view.filters_json)
        clearFilters()
        parsedFilters.forEach((f: any) => addFilter(f.column, f.value, f.mode))

        const url = new URL(window.location.href)
        url.searchParams.delete('view')
        window.history.replaceState({}, '', url.toString())
      }
    }

    if (viewId) {
      loadView(viewId)
      return
    }

    if (hasFilterParams || hasRangeParams || hasMetricParam) {
      clearFilters()
    }

    // Support the standardized ?filter_col=val format
    params.forEach((value, key) => {
      if (key.startsWith('filter_')) {
        const col = key.substring(7)
        addFilter(col, value, 'include')
        updated = true
      }
    })

    // Support start_time/end_time
    const urlStart = params.get('start_time')
    const urlEnd = params.get('end_time')
    if (urlStart && urlEnd) {
      setRange(urlStart, urlEnd)
      updated = true
    }

    // Support metric
    const urlMetric = params.get('metric')
    if (urlMetric) {
      setMetric(urlMetric)
      updated = true
    }

    if (updated) {
      const url = new URL(window.location.href)
      Array.from(url.searchParams.keys()).forEach(key => {
        if (key.startsWith('filter_')) url.searchParams.delete(key)
      })
      url.searchParams.delete('start_time')
      url.searchParams.delete('end_time')
      url.searchParams.delete('metric')
      window.history.replaceState({}, '', url.toString())
    }
  }, [activeServiceId, addFilter, setRange, setMetric, clearFilters])
}
