// Client-only module: reads window.location.search. Importing from a
// Server Component or build-time path is safe (the function early-returns
// when window is undefined) but the import IS treated as client code by
// the bundler.
import { useFilterStore } from '@/stores/filterStore'
import type { FiltersPayload } from '@/types/filters'

let hydrated = false

function rangeLabelToHours(label: string): number | null {
  const m = /^(\d+)([hd])$/.exec(label)
  if (!m) return null
  const n = parseInt(m[1], 10)
  if (!Number.isFinite(n) || n <= 0) return null
  return m[2] === 'd' ? n * 24 : n
}

// Synchronously hydrate `filterStore` from window.location.search.
// Idempotent — a module-level flag means re-calls are no-ops.
//
// Called from <UrlFilterHydrator> inside QueryProvider via a useState
// lazy initializer, so the store reflects URL params BEFORE any page-
// level hook reads from it on first paint. Without this, the URL→store
// sync lives in useFilterUrlSync's useEffect — which fires AFTER first
// render — so the client's first React Query keys use store DEFAULTS
// instead of URL params, causing any SSR'd cache keyed on URL params to
// miss (and the cache hit only fires on the subsequent re-render).
//
// All three URL filter formats are handled here:
//   - ``?range=24h`` (relative range, rolling)
//   - ``?start_time=…&end_time=…`` (absolute range, saved views)
//   - ``?filters={…}`` (modern JSON payload)
//   - ``?filter_<col>=<val>`` (legacy short form, dashboard CTA links)
//
// After applying, the URL params are STRIPPED from window.history so
// the per-page ``useUrlFilterSync`` effect (which fires AFTER mount)
// doesn't re-apply them. Without the strip, that effect's
// ``clearFilters()`` + re-add re-rendered the store and forced
// every consumer's first React Query to re-key — the cancelled-
// /api/sessions-POST symptom the perf audit chased.
export function hydrateFilterStoreFromUrl(): void {
  if (hydrated) return
  if (typeof window === 'undefined') return
  hydrated = true

  const params = new URLSearchParams(window.location.search)
  const qsRange = params.get('range')
  const qsStart = params.get('start_time')
  const qsEnd = params.get('end_time')
  const qsFilters = params.get('filters')

  const store = useFilterStore.getState()

  // ?range= wins over ?start_time/?end_time so a bookmarked "rolling
  // last 24h" stays rolling. Absolute timestamps are only honored when
  // no relative range is present (saved views, chart-zoom links).
  const rangeHours = qsRange ? rangeLabelToHours(qsRange) : null
  let touchedRange = false
  if (qsRange && rangeHours !== null) {
    const now = new Date()
    const start = new Date(now.getTime() - rangeHours * 3600 * 1000).toISOString()
    store.setRelativeRange(qsRange, start, now.toISOString())
    touchedRange = true
  } else if (qsStart && qsEnd) {
    store.setRange(qsStart, qsEnd)
    touchedRange = true
  }

  let touchedFilters = false
  if (qsFilters) {
    try {
      const parsed = JSON.parse(qsFilters) as FiltersPayload
      if (parsed && typeof parsed === 'object') {
        store.clearFilters()
        touchedFilters = true
        for (const [rawCol, spec] of Object.entries(parsed)) {
          if (!spec || !Array.isArray(spec.values)) continue
          // Strip the `_<n>` dedupe suffix the payload format adds
          // when the same column has both include + exclude buckets.
          const col = rawCol.replace(/_\d+$/, '')
          for (const v of spec.values) {
            store.addFilter(col, String(v), spec.mode === 'exclude' ? 'exclude' : 'include')
          }
        }
      }
    } catch {
      // Malformed ?filters= — ignore silently rather than break the page.
    }
  }

  // Legacy ?filter_<col>=<val> short form — what the dashboard's
  // "See Raw Logs" CTA and a handful of chart drill-downs still emit.
  // Only apply when the modern ``?filters=`` JSON form WASN'T present
  // (the two formats are mutually exclusive on a given navigation).
  if (!touchedFilters) {
    let cleared = false
    params.forEach((value, key) => {
      if (!key.startsWith('filter_')) return
      if (!cleared) {
        store.clearFilters()
        cleared = true
        touchedFilters = true
      }
      const col = key.substring('filter_'.length).replace(/_\d+$/, '')
      store.addFilter(col, value, 'include')
    })
  }

  if (touchedRange || touchedFilters) {
    // Strip the consumed params from the URL so the post-mount
    // useUrlFilterSync effect doesn't re-apply them on top of the
    // store state we just populated.
    const url = new URL(window.location.href)
    url.searchParams.delete('range')
    url.searchParams.delete('start_time')
    url.searchParams.delete('end_time')
    url.searchParams.delete('filters')
    Array.from(url.searchParams.keys()).forEach((key) => {
      if (key.startsWith('filter_')) url.searchParams.delete(key)
    })
    window.history.replaceState({}, '', url.toString())
  }
}

// Test helper — resets the module-level guard so unit tests can
// re-trigger hydration after stubbing window.location.
export function _resetUrlHydrationFlag(): void {
  hydrated = false
}
