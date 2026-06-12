'use client'

import { Button } from '@/components/ui/button'

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
  return (
    <div className="flex items-center gap-1">
      {opts.map((opt) => (
        <Button
          key={opt.value}
          variant={value === opt.value ? 'default' : 'outline'}
          size="sm"
          className="h-7 px-2 text-xs"
          onClick={() => onChange(opt.value)}
        >
          {opt.label}
        </Button>
      ))}
    </div>
  )
}
