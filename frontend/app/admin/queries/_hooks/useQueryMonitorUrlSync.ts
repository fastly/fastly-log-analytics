'use client'

import * as React from 'react'

import type { AttributionKind, DbFilter, ViewMode } from '../_types'

/**
 * Two-way sync between the page's filter state and the URL query string.
 *
 * Mirrors the pattern in `frontend/hooks/useFilterUrlSync.ts`:
 * - One-shot hydration from `window.location.search` on first mount.
 * - On every state change after that, `window.history.replaceState` writes
 *   the URL silently (no Next router refresh, which would refetch the page
 *   and remount the subtree).
 * - Default values are stripped so a clean view yields a clean URL.
 *
 * Shareability is the main motivation: an ops link like
 * `…/admin/queries?q=COMPACT&kind=cron&group=run` reproduces the exact
 * filter state for whoever opens it.
 */
export type QueryMonitorUrlState = {
  search: string
  kindFilter: AttributionKind | 'all'
  dbFilter: DbFilter
  viewMode: ViewMode
  slowThresholdMs: number
  groupByRun: boolean
}

export type QueryMonitorUrlSetters = {
  setSearch: (v: string) => void
  setKindFilter: (v: AttributionKind | 'all') => void
  setDbFilter: (v: DbFilter) => void
  setViewMode: (v: ViewMode) => void
  setSlowThresholdMs: (n: number) => void
  setGroupByRun: (v: boolean) => void
}

export function useQueryMonitorUrlSync(
  state: QueryMonitorUrlState,
  setters: QueryMonitorUrlSetters,
  defaultSlowMs: number,
): void {
  const hydratedRef = React.useRef(false)

  // Hydrate from URL on first mount. The dependency array is intentionally
  // empty — re-running this on setter identity changes would clobber
  // user-driven state updates with the (now-stale) URL on every render.
  React.useEffect(() => {
    if (hydratedRef.current) return
    if (typeof window === 'undefined') return
    const p = new URLSearchParams(window.location.search)
    const q = p.get('q')
    const kind = p.get('kind')
    const view = p.get('view')
    const slow = p.get('slow')
    const group = p.get('group')
    const db = p.get('db')
    if (q !== null) setters.setSearch(q)
    if (kind === 'analyst' || kind === 'admin' || kind === 'cron' || kind === 'system') {
      setters.setKindFilter(kind)
    }
    if (view === 'live' || view === 'past' || view === 'all') setters.setViewMode(view as ViewMode)
    if (slow !== null) {
      const n = parseInt(slow, 10)
      if (Number.isFinite(n) && n > 0) setters.setSlowThresholdMs(n)
    }
    if (group === 'run') setters.setGroupByRun(true)
    if (db === 'DuckDB' || db === 'SQLite') setters.setDbFilter(db)
    hydratedRef.current = true
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // Write state → URL on every change after hydration. Each param is
  // omitted when at its default so the URL stays clean for the default
  // view.
  React.useEffect(() => {
    if (!hydratedRef.current) return
    if (typeof window === 'undefined') return
    const url = new URL(window.location.href)
    const set = (key: string, value: string | null) => {
      if (value === null) url.searchParams.delete(key)
      else url.searchParams.set(key, value)
    }
    set('q', state.search || null)
    set('kind', state.kindFilter !== 'all' ? state.kindFilter : null)
    set('view', state.viewMode !== 'all' ? state.viewMode : null)
    set('slow', state.slowThresholdMs !== defaultSlowMs ? String(state.slowThresholdMs) : null)
    set('group', state.groupByRun ? 'run' : null)
    set('db', state.dbFilter !== 'all' ? state.dbFilter : null)
    window.history.replaceState({}, '', url.toString())
  }, [
    state.search,
    state.kindFilter,
    state.viewMode,
    state.slowThresholdMs,
    state.groupByRun,
    state.dbFilter,
    defaultSlowMs,
  ])
}
