import type { components } from '@/types/api.generated'

export type FilterMode = 'include' | 'exclude'

export type FilterSpec = components["schemas"]["FilterSpec"]
export type FiltersPayload = Record<string, FilterSpec>

export interface FilterPill {
  id: string
  column: string
  value: string
  mode: FilterMode
}

export interface DateRange {
  from: string
  to: string
}

/** Build a FiltersPayload from an array of FilterPills. */
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
