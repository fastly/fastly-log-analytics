import { subHours } from 'date-fns'
import { useFilterStore } from '@/stores/filterStore'
import type { FiltersPayload } from '@/types/api'

export function buildServiceHref(path: string, serviceId: string | null | undefined): string {
  return serviceId ? `${path}${path.includes('?') ? '&' : '?'}service=${encodeURIComponent(serviceId)}` : path
}

export function buildStreamDetailUrl(token: string, serviceId?: string | null): string {
  const base = `/sessions/stream?token=${encodeURIComponent(token)}`
  return serviceId ? `${base}&service=${encodeURIComponent(serviceId)}` : base
}

export function buildSessionDashboardUrl(
  serviceId: string | null | undefined,
  filterKey: string,
  filterValue: string | null | undefined,
  sessionStart: string | null | undefined,
  sessionEnd: string | null | undefined,
): string {
  const params = new URLSearchParams()
  if (serviceId != null) params.set('service', serviceId)
  if (filterValue != null) params.set(`filter_${filterKey}`, filterValue)
  if (sessionStart != null) params.set('start_time', sessionStart)
  if (sessionEnd != null) params.set('end_time', sessionEnd)
  return `/dashboard?${params.toString()}`
}

export const DEFAULT_RANGE_TOKEN = '24h'

export interface RangeWireArgs {
  relativeRange: string | null
  isAutoRange: boolean
  startTime: string | null
  endTime: string | null
  anchor: string
}

export interface RangeWire {
  rangeToken: string | null
  rangeKey: string
  rangeBody:
    | { range_token: string; anchor: string; start_time: string | null; end_time: string | null }
    | { start_time: string | null; end_time: string | null }
}

export function resolveRangeWire({ relativeRange, isAutoRange, startTime, endTime, anchor }: RangeWireArgs): RangeWire {
  const rangeToken = relativeRange ?? (isAutoRange ? DEFAULT_RANGE_TOKEN : null)
  return rangeToken !== null
    ? { rangeToken, rangeKey: rangeToken, rangeBody: { range_token: rangeToken, anchor, start_time: startTime, end_time: endTime } }
    : { rangeToken: null, rangeKey: `abs:${startTime ?? ''}|${endTime ?? ''}`, rangeBody: { start_time: startTime, end_time: endTime } }
}

export const ANCHOR_QUANTUM_SECONDS = 60

export function quantizeAnchor(iso?: string | null, now: Date = new Date()): string {
  const parsed = iso ? Date.parse(iso) : NaN
  const epoch = Math.floor((Number.isFinite(parsed) ? parsed : now.getTime()) / 1000)
  return new Date((epoch - (epoch % ANCHOR_QUANTUM_SECONDS)) * 1000).toISOString().replace(/\.\d{3}Z$/, 'Z')
}

let hydrated = false

export function rangeLabelToHours(label: string): number | null {
  const m = /^(\d+)([mhd])$/.exec(label)
  if (!m) return null
  const n = parseInt(m[1], 10)
  return !Number.isFinite(n) || n <= 0 ? null : m[2] === 'm' ? n / 60 : m[2] === 'd' ? n * 24 : n
}

export function applyUrlFiltersJsonToStore(
  qsFilters: string | null | undefined,
  store: Pick<ReturnType<typeof useFilterStore.getState>, 'clearFilters' | 'addFilter'> = useFilterStore.getState(),
): boolean {
  if (!qsFilters) return false
  try {
    const parsed = JSON.parse(qsFilters) as FiltersPayload
    if (parsed && typeof parsed === 'object') {
      store.clearFilters()
      for (const [rawCol, spec] of Object.entries(parsed)) {
        if (!spec || !Array.isArray(spec.values)) continue
        const col = rawCol.replace(/_\d+$/, '')
        for (const v of spec.values) store.addFilter(col, String(v), spec.mode === 'exclude' ? 'exclude' : 'include')
      }
      return true
    }
  } catch {
    /* ignore */
  }
  return false
}

export function hydrateFilterStoreFromUrl(): void {
  if (hydrated || typeof window === 'undefined') return
  hydrated = true
  const params = new URLSearchParams(window.location.search)
  const qsRange = params.get('range')
  const qsStart = params.get('start_time')
  const qsEnd = params.get('end_time')
  const qsFilters = params.get('filters')
  const store = useFilterStore.getState()
  const rangeHours = qsRange ? rangeLabelToHours(qsRange) : null
  let touchedRange = false
  let touchedFilters = false
  if (qsRange && rangeHours !== null) {
    const now = new Date()
    store.setRelativeRange(qsRange, subHours(now, rangeHours).toISOString(), now.toISOString())
    touchedRange = true
  } else if (qsStart && qsEnd) {
    store.setRange(qsStart, qsEnd)
    touchedRange = true
  }
  touchedFilters = applyUrlFiltersJsonToStore(qsFilters, store)
  if (!touchedFilters) {
    let cleared = false
    params.forEach((value, key) => {
      if (!key.startsWith('filter_')) return
      if (!cleared) {
        store.clearFilters()
        cleared = touchedFilters = true
      }
      store.addFilter(key.substring('filter_'.length).replace(/_\d+$/, ''), value, 'include')
    })
  }
  if (touchedRange || touchedFilters) {
    const url = new URL(window.location.href)
    for (const k of ['range', 'start_time', 'end_time', 'filters']) url.searchParams.delete(k)
    Array.from(url.searchParams.keys()).forEach((key) => {
      if (key.startsWith('filter_')) url.searchParams.delete(key)
    })
    window.history.replaceState({}, '', url.toString())
  }
}

export function _resetUrlHydrationFlag(): void {
  hydrated = false
}

export function normalizeTabKey(tab: string | null | undefined): string {
  if (!tab) return 'all'
  const lower = tab.toLowerCase().trim()
  return lower === 'performance' ? 'edge' : lower === 'routing' ? 'network' : lower
}
