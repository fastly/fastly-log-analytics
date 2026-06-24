'use client'

import { AlertTriangle, Info } from 'lucide-react'
import { useDataWindowOverlap } from '@/hooks/useDataWindowOverlap'
import { formatRelative, toUTCDate } from '@/lib/date'

// Surface when the selected time range falls entirely outside the
// retained log extents. Every empty analytics card would otherwise just
// say "No logs found for this period" with no signal that the picked
// window is the cause — operators stare at blank charts and refresh.
//
// Read-only by design: NEVER offers an auto-snap action. Filter changes
// affect every queryKey downstream and a one-click "snap to data" would
// silently swap the URL-shared range. The banner names the retained
// extents so the operator can hand-pick a valid window via the existing
// quick-presets, datetime inputs, or Reset.
export function DataWindowBanner() {
  const { status, earliestLogAt, latestLogAt } = useDataWindowOverlap()

  if (status === 'ok' || status === 'unknown') return null

  let icon: React.ReactNode
  let title: string
  let body: React.ReactNode

  if (status === 'no-data') {
    icon = <Info className="h-4 w-4 shrink-0" aria-hidden="true" />
    title = 'No logs ingested yet'
    body = (
      <>
        This service hasn&apos;t recorded any logs in the local store yet.
        Empty charts below are expected — they&apos;ll populate once the
        first sync completes.
      </>
    )
  } else {
    icon = <AlertTriangle className="h-4 w-4 shrink-0 text-amber-600 dark:text-amber-400" aria-hidden="true" />
    title = status === 'before-earliest'
      ? 'Selected range is before the retained data window'
      : 'Selected range is after the retained data window'
    body = (
      <>
        Charts will be empty because no logs exist for this range. Retained
        data:{' '}
        <strong className="font-medium">
          {fmtExtent(earliestLogAt)} → {fmtExtent(latestLogAt)}
        </strong>
        . Use a quick-preset or Reset above to return to a window with data.
      </>
    )
  }

  return (
    <div
      role="status"
      aria-live="polite"
      className="border-b border-amber-500/30 bg-amber-500/10 px-4 py-2 text-xs text-amber-900 dark:text-amber-100"
      data-testid="data-window-banner"
    >
      <div className="flex items-start gap-2 max-w-7xl">
        {icon}
        <div className="flex-1 min-w-0">
          <span className="font-medium">{title}.</span>{' '}
          <span className="text-amber-900/80 dark:text-amber-100/80">{body}</span>
        </div>
      </div>
    </div>
  )
}

function fmtExtent(value: string | null): string {
  if (!value) return '—'
  try {
    const d = toUTCDate(value)
    return `${d.toISOString().slice(0, 16).replace('T', ' ')}Z (${formatRelative(value)})`
  } catch {
    return value
  }
}
