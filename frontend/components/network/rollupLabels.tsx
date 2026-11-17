'use client'

import { Info } from 'lucide-react'
import { useTimezone } from '@/hooks/usePersistentPreferences'
import { formatDate } from '@/lib/utils'
import type { NetworkRollupMeta } from '@/lib/cloud-engine/network-rollup'

export type { NetworkRollupMeta }

/** The `rollup` block Cloud adds to /network responses it answered from summaries; absent on raw reads. */
export function readRollupMeta(data: unknown): NetworkRollupMeta | undefined {
  return (data as { rollup?: NetworkRollupMeta } | null | undefined)?.rollup
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
