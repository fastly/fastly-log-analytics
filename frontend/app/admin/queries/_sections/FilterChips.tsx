'use client'

import { Button } from '@/components/ui/button'

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
    <div className="flex items-center gap-1">
      {opts.map((opt) => (
        <Button
          key={opt}
          variant={value === opt ? 'default' : 'outline'}
          size="sm"
          className="h-7 px-2 text-xs capitalize"
          onClick={() => onChange(opt)}
        >
          {opt}
        </Button>
      ))}
    </div>
  )
}
