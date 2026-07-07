import type { components } from '@/types/api.generated'

type FilterSpec = components["schemas"]["FilterSpec"]
export type FilterMode = FilterSpec["mode"]
export type FiltersPayload = Record<string, FilterSpec>

export interface FilterPill {
  id: string
  column: string
  value: string
  mode: FilterMode
}

/**
 * Build a FiltersPayload from an array of FilterPills.
 *
 * Dedup scheme: when the same column needs both an include AND an exclude
 * bucket, the second bucket gets a `_<n>` suffix (`country`, `country_1`).
 * hydrateFilterStoreFromUrl (lib/urlFilterHydration.ts) strips this suffix
 * on URL hydration, and the backend
 * (backend/repositories/utils/filters.py) strips it when building WHERE
 * clauses. As a consequence, column names literally ending in `_<digit>`
 * would be corrupted on round-trip. filterStore.addFilter guards entry —
 * any future field naming convention must avoid the collision.
 */
export function buildFiltersPayload(filters: FilterPill[]): FiltersPayload {
  const payload: FiltersPayload = {}

  filters.forEach(f => {
    let index = 0;
    let targetKey: string | null = null;

    while (true) {
      const currentKey = index === 0 ? f.column : `${f.column}_${index}`;
      if (!payload[currentKey]) {
        targetKey = currentKey;
        break;
      } else if (payload[currentKey].mode === f.mode) {
        targetKey = currentKey;
        break;
      }
      index++;
    }

    if (!payload[targetKey]) {
      payload[targetKey] = { mode: f.mode, values: [] };
    }
    payload[targetKey].values.push(f.value);
  })

  return payload
}
