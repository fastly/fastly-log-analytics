import { create } from 'zustand'
import { FilterPill, FilterMode } from '@/types/filters'
import { subDays, formatISO } from 'date-fns'

interface FilterState {
  startTime: string
  endTime: string
  filters: FilterPill[]
  edgeOnly: boolean
  isAutoRange: boolean
  // When a quick-preset pill is active, holds its label ("24h", "3d", ...).
  // Null means custom range (datetime inputs, chart zoom, saved view) or
  // implicit default. The URL-sync hook persists this as ?range=<label> and
  // re-derives [now-duration, now] on hydrate so reloads track the rolling
  // window instead of pinning the absolute timestamps from the click moment.
  relativeRange: string | null
  compareMode: boolean
  compareStartTime: string | null
  compareEndTime: string | null
  setRange: (start: string, end: string) => void
  setRelativeRange: (range: string, start: string, end: string) => void
  addFilter: (column: string, value: string, mode: FilterMode) => void
  removeFilter: (id: string) => void
  toggleFilterMode: (id: string) => void
  toggleCompareMode: () => void
  setCompareRange: (start: string | null, end: string | null) => void
  toggleEdgeOnly: () => void
  clearFilters: () => void
  resetAll: () => void
  resetRange: () => void
}

export const useFilterStore = create<FilterState>((set) => ({
  // Default to last 24h.
  startTime: formatISO(subDays(new Date(), 1)),
  endTime: formatISO(new Date()),
  filters: [],
  edgeOnly: false,
  isAutoRange: true, // Start with auto-range enabled for first data discovery
  relativeRange: null,
  compareMode: false,
  compareStartTime: null,
  compareEndTime: null,

  // Explicit absolute-range selection (custom datetime, chart zoom, saved
  // view). Clears relativeRange — this range no longer corresponds to a
  // rolling preset. Early-bail when the range hasn't actually changed and
  // we're already in absolute mode — a re-emit forces subscribers to
  // re-render (and useQuery to re-fetch with a new key tuple) for no
  // observable change.
  setRange: (startTime, endTime) => set((state) => {
    if (state.startTime === startTime && state.endTime === endTime && !state.isAutoRange && state.relativeRange === null) {
      return state
    }
    return { startTime, endTime, isAutoRange: false, relativeRange: null }
  }),

  // Preset pill click. Records the label so the URL persists as
  // ?range=<label> and reload re-derives [now-duration, now].
  setRelativeRange: (relativeRange, startTime, endTime) =>
    set({ startTime, endTime, isAutoRange: false, relativeRange }),

  resetRange: () => set({ isAutoRange: true, relativeRange: null }),

  toggleCompareMode: () => set((state) => {
    const nextMode = !state.compareMode
    if (nextMode) {
      // Default to matching the duration of the current selection, placed immediately prior.
      // We snap to minutes to ensure consistency with the UI inputs and prevent precision mismatches
      // that can cause chart rendering delays until manual "Apply" is hit.
      const s = new Date(state.startTime)
      const e = new Date(state.endTime)
      s.setSeconds(0, 0)
      e.setSeconds(0, 0)

      const diff = e.getTime() - s.getTime()
      const compEnd = new Date(s.getTime())
      const compStart = new Date(compEnd.getTime() - diff)
      return {
        startTime: s.toISOString(),
        endTime: e.toISOString(),
        compareMode: nextMode,
        compareStartTime: compStart.toISOString(),
        compareEndTime: compEnd.toISOString()
      }
    }
    return { compareMode: nextMode }
  }),

  setCompareRange: (startTime, endTime) => set({ compareStartTime: startTime, compareEndTime: endTime }),

  addFilter: (column, value, mode) => set((state) => {
    // Reject column names matching /_\d+$/. buildFiltersPayload uses
    // `_<n>` as a dedup suffix when the same column needs both include
    // and exclude buckets, and hydrateFilterStoreFromUrl (lib/urlFilterHydration.ts) strips that suffix on
    // URL hydration. A column literally ending in `_<digit>` (e.g.
    // `response_1`) would be silently corrupted on round-trip. The
    // field catalog (source schema) is the source of truth for column
    // names; any future field naming convention must avoid the collision.
    if (/_\d+$/.test(column)) {
      console.warn(
        `[filterStore] addFilter: dropping column "${column}" — column names ending in _<digit> ` +
        `collide with the buildFiltersPayload dedup suffix scheme.`
      )
      return state
    }

    // If exact filter already exists, don't duplicate
    const exists = state.filters.find(f => f.column === column && f.value === value)
    if (exists) return state

    return {
      filters: [
        ...state.filters,
        {
          id: crypto.randomUUID(),
          column,
          value,
          mode
        }
      ]
    }
  }),

  removeFilter: (id) => set((state) => ({
    filters: state.filters.filter(f => f.id !== id)
  })),

  toggleFilterMode: (id) => set((state) => ({
    filters: state.filters.map(f =>
      f.id === id
        ? { ...f, mode: f.mode === 'include' ? 'exclude' : 'include' }
        : f
    )
  })),

  toggleEdgeOnly: () => set((state) => ({ edgeOnly: !state.edgeOnly })),

  clearFilters: () => set({ filters: [] }),

  resetAll: () => {
    const now = new Date()
    set({
      filters: [],
      isAutoRange: true,
      relativeRange: null,
      compareMode: false,
      compareStartTime: null,
      compareEndTime: null,
      startTime: formatISO(subDays(now, 1)),
      endTime: formatISO(now),
    })
  },
}))
