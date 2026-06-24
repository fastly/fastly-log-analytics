'use client'

import { FilterChipRow } from './FilterChipRow'

import type { DbFilter } from '../_types'

/** DB-engine filter chip row (All / DuckDB / SQLite). Controlled — the
 *  parent owns the selection state so the filter can apply page-wide and
 *  the value persists to the URL alongside the other filters. */
export function DbFilterChips({
  value,
  onChange,
}: {
  value: DbFilter
  onChange: (v: DbFilter) => void
}) {
  const opts: { value: DbFilter; label: string }[] = [
    { value: 'all', label: 'All DBs' },
    { value: 'DuckDB', label: 'DuckDB' },
    { value: 'SQLite', label: 'SQLite' },
  ]
  return <FilterChipRow value={value} onChange={onChange} options={opts} />
}
