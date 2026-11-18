'use client'

import { Info } from 'lucide-react'
import { useTimezone } from '@/hooks/usePersistentPreferences'
import { cn, formatDate } from '@/lib/utils'
import type { NetworkRollupMeta } from '@/lib/cloud-engine/network-rollup'

export type { NetworkRollupMeta }

/** The `rollup` block Cloud adds to /network responses it answered from summaries; absent on raw reads. */
export function readRollupMeta(data: unknown): NetworkRollupMeta | undefined {
  return (data as { rollup?: NetworkRollupMeta } | null | undefined)?.rollup
}

/** One page's summary reads combined: the narrower window and every marked field, or whichever read has a block. */
export function mergeRollupMeta(a: NetworkRollupMeta | undefined, b: NetworkRollupMeta | undefined): NetworkRollupMeta | undefined {
  if (!a || !b) return a ?? b
  return {
    data_from: a.data_from > b.data_from ? a.data_from : b.data_from,
    data_through: a.data_through < b.data_through ? a.data_through : b.data_through,
    approximate: [...a.approximate, ...b.approximate],
    lower_bound: [...a.lower_bound, ...b.lower_bound],
  }
}

export const approxMark = (meta: NetworkRollupMeta | undefined, field: string) => meta?.approximate.includes(field) ? '≈' : ''
export const boundMark = (meta: NetworkRollupMeta | undefined, field: string) => meta?.lower_bound.includes(field) ? '≥' : ''

export function RollupDataThrough({ meta }: { meta: NetworkRollupMeta }) {
  const timezone = useTimezone()
  return (
    <div role="note" className="flex items-center gap-2 rounded-md border bg-muted/30 px-3 py-1.5 text-xs text-muted-foreground">
      <Info className="h-3.5 w-3.5 shrink-0" />
      <span title={formatDate(meta.data_through, timezone, 'yyyy-MM-dd HH:mm:ss')}>
        Data through {formatDate(meta.data_through, timezone, 'HH:mm')}.
        Values marked ≈ are estimated from summaries and ≥ are lower bounds.
      </span>
    </div>
  )
}

/**
 * Holds the note's line(s) from first paint wherever the page can be answered
 * from summaries, so the note landing (or a raw answer leaving it out) never
 * moves the content below. Two lines until the note fits on one.
 */
export function RollupDataThroughSlot({ reserve, meta, className }: {
  reserve: boolean
  meta: NetworkRollupMeta | undefined
  className?: string
}) {
  if (!reserve) return null
  return (
    <div data-testid="rollup-data-through-slot" className={cn('min-h-[2.875rem] lg:min-h-[1.875rem]', className)}>
      {meta && <RollupDataThrough meta={meta} />}
    </div>
  )
}
