'use client'

import React from 'react'
import Link from 'next/link'
import { AlertTriangle, CheckCircle2, Loader2, RefreshCw } from 'lucide-react'
import { buildServiceHref } from '@/lib/navigation'
import { formatDate } from '@/lib/date'
import { Button } from '@/components/ui/button'
import { usePolicyActions, useSyncStatus } from './policyApi'

/**
 * Whether the saved policies are live at the blocker. "Not yet live" means the saved revision is
 * ahead of the one the blocker accepted, so edits exist only in this app until a retry succeeds.
 *
 * `compact` renders nothing while everything is in sync (the Monitor view only wants to hear bad news).
 */
export function PolicySyncNotice({
  activeServiceId,
  timezone,
  compact = false,
}: {
  activeServiceId: string | null
  timezone: string
  compact?: boolean
}) {
  const status = useSyncStatus(activeServiceId)
  const { retry } = usePolicyActions()
  const [busy, setBusy] = React.useState(false)
  const data = status.data
  if (!data) return null

  const sync = async () => {
    setBusy(true)
    try {
      await retry()
    } finally {
      setBusy(false)
    }
  }

  if (!data.configured) {
    return (
      <p role="status" className="rounded-md border bg-muted/40 px-3 py-2 text-xs text-muted-foreground">
        No blocklist endpoint is configured, so policies are saved but not enforced. Set one in{' '}
        <Link className="underline" href={buildServiceHref('/admin', activeServiceId)}>
          Admin → Overall Settings
        </Link>
        .
      </p>
    )
  }

  if (data.in_sync) {
    if (compact) return null
    return (
      <div role="status" className="flex flex-wrap items-center gap-3 rounded-md border bg-muted/40 px-3 py-2 text-xs">
        <span className="inline-flex items-center gap-1.5">
          <CheckCircle2 className="h-3.5 w-3.5 text-emerald-700 dark:text-emerald-500" aria-hidden="true" />
          Live: the blocker has revision {data.published_rev}
          {data.last_ok_at ? ` (synced ${formatDate(data.last_ok_at, timezone, 'yyyy-MM-dd HH:mm:ss')})` : ''}. It can take a
          few seconds to reach every edge.
        </span>
        <Button variant="outline" size="sm" className="ml-auto h-7 text-xs" onClick={() => void sync()} disabled={busy}>
          {busy ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <RefreshCw className="h-3 w-3 mr-1" />}
          {busy ? 'Syncing…' : 'Sync now'}
        </Button>
      </div>
    )
  }

  return (
    <div
      role="alert"
      className="flex flex-wrap items-center gap-3 rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-xs"
    >
      <span className="inline-flex min-w-0 items-center gap-1.5 text-amber-900 dark:text-amber-300">
        <AlertTriangle className="h-3.5 w-3.5 shrink-0" aria-hidden="true" />
        <span>
          <strong>Not yet live.</strong> Saved revision {data.rev} has not reached the blocker (it has {data.published_rev}).
          {data.last_error ? ` Last error: ${data.last_error}` : ''}
        </span>
      </span>
      <Button variant="outline" size="sm" className="ml-auto h-7 text-xs" onClick={() => void sync()} disabled={busy}>
        {busy ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <RefreshCw className="h-3 w-3 mr-1" />}
        {busy ? 'Retrying…' : 'Retry'}
      </Button>
    </div>
  )
}
