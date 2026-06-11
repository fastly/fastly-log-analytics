'use client'

import * as React from 'react'
import { useRouter } from 'next/navigation'
import { useQueryClient } from '@tanstack/react-query'

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
  const queryClient = useQueryClient()
  // Seed initial state from bootstrap's share_banner field if it has
  // landed already (perf audit Phase D-3). Skips the first poll +
  // RTT on cold load. Polling still runs on the 15s cadence below
  // for ongoing updates (admin starting/stopping sharing should
  // reflect in the banner within ~15s, regardless of how stale the
  // bootstrap entry is).
  const bootstrapData = queryClient.getQueryData(['bootstrap']) as any
  const seeded: ShareStatus | null = bootstrapData?.share_banner
    ? {
        sharing_active: !!bootstrapData.share_banner.sharing_active,
        public_url: bootstrapData.share_banner.public_url ?? null,
      }
    : null
  const [status, setStatus] = React.useState<ShareStatus | null>(seeded)

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
    // Don't fire the immediate poll if we already seeded from
    // bootstrap — bootstrap's share_banner is at most as stale as
    // bootstrap's 5-min cache. The 15-s setInterval below picks up
    // changes within one poll window either way.
    if (seeded === null) {
      tick()
    }
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
      // M-13 (a11y): bg-amber-500 + text-amber-50 measured below WCAG AA
      // contrast (~3.1 ratio) on the 12px banner text — the single largest
      // contributor to the audit's 299-node admin color-contrast count.
      // Darken to amber-700 background with white text → contrast jumps to
      // ~7.0 and the banner reads as a serious operator warning, which is
      // its actual purpose (the safety affordance reminding the admin
      // that data is exposed to external analysts).
      className="w-full bg-amber-700 hover:bg-amber-800 text-white text-xs font-semibold py-1.5 text-center shadow shrink-0"
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
