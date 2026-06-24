'use client'

import { FilterChipRow } from './FilterChipRow'

import type { AttributionKind } from '../_types'

/** Kind-filter chip row (All / Analyst / Admin / Cron / System). Controlled
 *  — the parent owns the selection state so the value can be shared with
 *  search filtering and persisted to URL state in a future iteration. */
export function FilterChips({
  value,
  onChange,
}: {
  value: AttributionKind | 'all'
  onChange: (v: AttributionKind | 'all') => void
}) {
  const opts: (AttributionKind | 'all')[] = ['all', 'analyst', 'admin', 'cron', 'system']
  return (
    <FilterChipRow
      value={value}
      onChange={onChange}
      options={opts.map((o) => ({ value: o, label: o }))}
    />
  )
}
