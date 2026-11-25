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
  const title = formatDate(meta.data_through, timezone, 'yyyy-MM-dd HH:mm:ss')
  return (
    <div role="note" className="flex flex-wrap items-center gap-x-1.5 gap-y-0.5 rounded-md border bg-muted/30 px-3 py-1.5 text-xs text-muted-foreground">
      <Info className="h-3.5 w-3.5 shrink-0" />
      <span title={title}>Data through {formatDate(meta.data_through, timezone, 'HH:mm')}.</span>
      <span>Values marked ≈ are estimated from summaries</span>
      <span>and ≥ are lower bounds.</span>
    </div>
  )
}

/**
 * Holds the note's line(s) from first paint wherever the page can be answered
 * from summaries, so the note landing (or a raw answer leaving it out) never
 * moves the content below. Two lines until the note fits on one.
 */
export function RollupDataThroughSlot({ reserve, meta, rawWindow, className }: {
  reserve: boolean
  meta: NetworkRollupMeta | undefined
  rawWindow?: RawWindow | undefined
  className?: string
}) {
  if (!reserve && !meta && !rawWindow) return null
  return (
    <div data-testid="rollup-data-through-slot" className={cn('min-h-[2.875rem] lg:min-h-[2.25rem]', className)}>
      {meta ? <RollupDataThrough meta={meta} /> : rawWindow ? <RawWindowNote window={rawWindow} /> : null}
    </div>
  )
}

/** The newest window Cloud narrowed an over-budget raw read to; absent when the whole range was read. */
export type RawWindow = { data_from: string; data_through: string }

export function readRawWindow(data: unknown): RawWindow | undefined {
  return (data as { raw_window?: RawWindow } | null | undefined)?.raw_window
}

export function RawWindowNote({ window, className }: { window: RawWindow | undefined; className?: string }) {
  const timezone = useTimezone()
  if (!window) return null
  const title = `${formatDate(window.data_from, timezone, 'yyyy-MM-dd HH:mm:ss')} – ${
    formatDate(window.data_through, timezone, 'yyyy-MM-dd HH:mm:ss')}`
  return (
    <div role="note" data-testid="raw-window-note"
      className={cn('flex flex-wrap items-center gap-x-1.5 gap-y-0.5 rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-1.5 text-xs', className)}>
      <Info className="h-3.5 w-3.5 shrink-0" />
      <span title={title}>
        Showing {formatDate(window.data_from, timezone, 'HH:mm')}–{formatDate(window.data_through, timezone, 'HH:mm')} only:
      </span>
      <span>the whole range needs more log data</span>
      <span>than the browser scan budget allows.</span>
      <span>Values are exact for this window;</span>
      <span>choose a shorter range to see all of it.</span>
    </div>
  )
}
