'use client'

import * as React from 'react'
import { useRouter } from 'next/navigation'

import { client } from '@/lib/api'

interface Options {
  enabled: boolean
}

type ShareStatus = {
  sharing_active: boolean
  public_url: string | null
}

const POLL_MS = 15_000

export function useShareStatusBanner({ enabled }: Options) {
  const router = useRouter()
  const [status, setStatus] = React.useState<ShareStatus | null>(null)

  React.useEffect(() => {
    if (!enabled) return
    let cancelled = false
    const tick = async () => {
      try {
        // Use the lean /api/admin/share/banner endpoint (~80B) instead of
        // /status (~11KB). The banner only needs sharing_active +
        // public_url; the full status response carries services / invites
        // / sessions / audit_logs / telemetry that the banner never reads
        // and the poll runs every 15s on every page with AppLayout.
        const { data, response } = await client.GET('/api/admin/share/banner' as any, {})
        if (cancelled) return
        if (!response.ok) return
        const body = data as any
        setStatus({
          sharing_active: !!body?.sharing_active,
          public_url: body?.public_url ?? null,
        })
      } catch {
        /* swallow — banner is non-essential UX */
      }
    }
    tick()
    const id = setInterval(tick, POLL_MS)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [enabled])

  const node = enabled && status?.sharing_active ? (
    <button
      type="button"
      onClick={() => router.push('/admin/share')}
      className="w-full bg-amber-500/90 hover:bg-amber-500 text-amber-50 text-xs font-semibold py-1.5 text-center shadow shrink-0"
      data-testid="share-active-banner"
    >
      ⚠️ Dashboard sharing is ACTIVE
      {status.public_url ? ` — ${status.public_url}` : ''} (click to manage)
    </button>
  ) : null

  return {
    node,
    sharingActive: !!status?.sharing_active,
    openDashboard: () => router.push('/admin/share'),
  }
}
