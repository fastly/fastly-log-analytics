'use client'

import * as React from 'react'
import { useRouter } from 'next/navigation'
import { useQuery, useQueryClient } from '@tanstack/react-query'

import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import { queryKeys } from '@/lib/query-keys'
import { useServiceStore } from '@/stores/serviceStore'
import { useSessionRoleStore } from '@/stores/sessionRoleStore'
import { useIsAnalyst } from '@/hooks/useIsAnalyst'
import { buildServiceHref } from '@/lib/utils'
import type { BootstrapResponse, ShareBanner } from '@/types/api'

interface Options {
  enabled: boolean
}

const POLL_MS = 15_000

function toBanner(raw: { [key: string]: unknown } | null | undefined): ShareBanner | null {
  if (!raw) return null
  return {
    sharing_active: !!raw.sharing_active,
    public_url: typeof raw.public_url === 'string' ? raw.public_url : null,
  }
}

function subscribeVisibility(onChange: () => void) {
  document.addEventListener('visibilitychange', onChange)
  return () => document.removeEventListener('visibilitychange', onChange)
}

export function useShareStatusBanner({ enabled }: Options) {
  const router = useRouter()
  const queryClient = useQueryClient()
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const isRemoteAnalyst = useSessionRoleStore((s) => s.isRemoteAnalyst)
  const isAnalyst = useIsAnalyst()
  // Background tabs don't poll (AGENTS.md: refetchIntervalInBackground=false).
  const hidden = React.useSyncExternalStore(
    subscribeVisibility,
    () => document.hidden,
    () => false,
  )

  const readBootstrapBanner = () =>
    toBanner(queryClient.getQueryData<BootstrapResponse>(queryKeys.bootstrap())?.share_banner)

  // Seed from bootstrap's share_banner (perf audit Phase D-3): initialData +
  // a staleTime equal to the poll cadence means mount does NOT fetch when the
  // seed is present; the 15s refetchInterval picks up admin start/stop within
  // one window regardless of how stale the bootstrap entry is.
  const seeded = readBootstrapBanner()
  const seededAtRenderRef = React.useRef(seeded !== null)

  const { data: status } = useQuery<ShareBanner | null>({
    queryKey: queryKeys.shareBanner(),
    queryFn: async ({ signal }) => {
      if (queryClient.getQueryData<BootstrapResponse>(queryKeys.bootstrap())?.settings?.is_remote_analyst) {
        return null
      }
      // Bootstrap can resolve between first render and the mount-time fetch;
      // re-read it once so that window doesn't cost a round trip either.
      if (!seededAtRenderRef.current) {
        seededAtRenderRef.current = true
        const late = readBootstrapBanner()
        if (late) return late
      }
      // Lean /api/admin/share/banner (~80B) instead of /status (~11KB): the
      // banner only needs sharing_active + public_url and this polls every
      // 15s on every page with AppLayout.
      const data = await unwrapClient(client.GET('/api/admin/share/banner', { signal }))
      return toBanner(data)
    },
    initialData: seeded ?? undefined,
    staleTime: POLL_MS,
    refetchInterval: POLL_MS,
    refetchIntervalInBackground: false,
    retry: false,
    enabled: enabled && !hidden && !isRemoteAnalyst && !isAnalyst,
  })

  const node = enabled && status?.sharing_active ? (
    <button
      type="button"
      onClick={() => router.push(buildServiceHref('/admin/share', activeServiceId))}
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
    openDashboard: () => router.push(buildServiceHref('/admin/share', activeServiceId)),
  }
}
