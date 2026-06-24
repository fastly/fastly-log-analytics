'use client'

import { RefreshCw } from 'lucide-react'

/** Tiny "Live / Paused / Error" indicator placed next to the section
 *  title. Stateless — the parent passes the four relevant flags. */
export function PollingIndicator({
  visible,
  isFetching,
  isError,
  paused = false,
}: {
  visible: boolean
  isFetching: boolean
  isError: boolean
  /** True when the admin has manually paused polling via the Pause
   *  button. Distinct from `!visible` (tab hidden) so the message can
   *  reflect the action they actually took. */
  paused?: boolean
}) {
  if (isError) return <span className="text-xs text-red-500 ml-2">Error — retrying</span>
  if (paused) return <span className="text-xs text-amber-600 ml-2 font-medium">Paused</span>
  if (!visible) return <span className="text-xs text-muted-foreground ml-2">Paused (tab hidden)</span>
  return (
    <span className="flex items-center gap-1 text-xs text-muted-foreground ml-2">
      <RefreshCw className={`h-3 w-3 ${isFetching ? 'animate-spin' : 'opacity-50'}`} />
      Live
    </span>
  )
}
