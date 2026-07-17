'use client'

import * as React from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, ShieldAlert } from 'lucide-react'
import dynamic from 'next/dynamic'

import { Alert, AlertDescription } from '@/components/ui/alert'
import { BackToAdminLink } from '@/components/BackToAdminLink'
import { PageHeader } from '@/components/ui/page-header'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { InvitationsPanel } from '@/components/share-dashboard/InvitationsPanel'
import { SharingControlPanel } from '@/components/share-dashboard/SharingControlPanel'
import type { ShareStatus } from '@/components/share-dashboard/utils'
import { FormSkeleton } from '@/components/skeletons/PageSkeleton'
import { client, extractApiError } from '@/lib/api'

// InvitationsPanel is the default tab on cold load — static-importing it
// keeps the initial paint off the dynamic-chunk fetch path.
// SessionsPanel + AuditLogPanel stay lazy because they only mount on
// tab click; ``ssr: false`` keeps their hooks off the SSR pass.
const SessionsPanel = dynamic(
  () => import('@/components/share-dashboard/SessionsPanel').then(m => m.SessionsPanel),
  { ssr: false },
)
const AuditLogPanel = dynamic(
  () => import('@/components/share-dashboard/AuditLogPanel').then(m => m.AuditLogPanel),
  { ssr: false },
)

// Shared query key so the hover-prefetch on the Admin → Share Dashboard
// link (in /admin/page.tsx) populates the same React Query cache entry
// the page reads on mount. Resulting UX: by the time the operator
// clicks Share Dashboard, the status payload is already in cache —
// page paints real content immediately instead of skeleton-then-swap.
export const SHARE_STATUS_QUERY_KEY = ['admin', 'share', 'status'] as const
// Companion key for the lean /live poll — keeps its own cache slot so
// the mount-time /status query isn't refetched on the 10-s tick.
export const SHARE_LIVE_QUERY_KEY = ['admin', 'share', 'live'] as const

type ShareLiveFields = Partial<Pick<
  ShareStatus,
  'sharing_active' | 'public_url' | 'active_session_count' | 'rate_limits' | 'telemetry'
>>

export default function ShareDashboardPage() {
  const queryClient = useQueryClient()
  const [actionError, setActionError] = React.useState('')
  const [activeTab, setActiveTab] = React.useState('invites')
  const [auditEmailFilter, setAuditEmailFilter] = React.useState('')

  // /status carries the full mount-time payload — services, invites,
  // sessions, audit logs (~11 KB). Mutations refetch this. No
  // refetchInterval; the 10-s live tick rides on /live below.
  const { data: status, error: statusError, refetch } = useQuery({
    queryKey: SHARE_STATUS_QUERY_KEY,
    queryFn: async ({ signal }) => {
      const { data, response } = await client.GET('/api/admin/share/status' as any, { signal, })
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as ShareStatus
    },
    // 30s staleTime so the hover-prefetch from the /admin PageHeader chip
    // is reused on click even when the user lingers on hover. Mutations
    // call refresh() to bust this when invites/sessions change.
    staleTime: 30_000,
  })

  // /live carries only the fields that actually change in real time
  // (sharing_active, public_url, active_session_count, rate_limits,
  // telemetry). ~1.5 KB instead of ~11 KB. Freshness via the admin event
  // stream's `share` channel (see note below); this query is a one-shot
  // snapshot on mount plus a 5-min safety net for silently-dropped streams.
  const { data: live } = useQuery({
    queryKey: SHARE_LIVE_QUERY_KEY,
    queryFn: async ({ signal }) => {
      const { data, response } = await client.GET('/api/admin/share/live' as any, { signal })
      if (!response.ok) throw new Error(`live ${response.status}`)
      return data as ShareLiveFields
    },
    staleTime: 10_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })

  // SSE push of /live changes rides the multiplexed admin event stream's
  // `share` channel (SyncStatusBadge adds it on /admin/share), which writes
  // this same ['admin','share','live'] cache key — one shared connection
  // instead of a second dedicated stream over the H1 admin tunnel. The
  // useQuery above keeps the one-shot mount snapshot + 5-min safety net.
  const refresh = React.useCallback(async () => {
    await refetch()
    // Also invalidate /live so the operator sees their just-revoked
    // session disappear without waiting for the 10-s tick.
    queryClient.invalidateQueries({ queryKey: SHARE_LIVE_QUERY_KEY })
  }, [refetch, queryClient])
  const statusErrorMsg = statusError
    ? extractApiError(statusError as any) || (statusError as Error).message || 'unable to load status'
    : ''
  // Merge /live fields into /status so SharingControlPanel (which reads
  // active_session_count + rate_limits + telemetry) sees fresh data on
  // every 10-s tick without us touching the full /status payload.
  const statusForPanels: ShareStatus | null = status
    ? { ...status, ...(live ?? {}) }
    : null

  return (
    <div className="space-y-6">
      <PageHeader
        title="Remote Dashboard Sharing"
        description="Start the share tunnel, manage analyst invitations, monitor live sessions, and review the audit trail."
        icon={ShieldAlert}
      >
        <BackToAdminLink />
      </PageHeader>

      <Alert className="border-amber-300 bg-amber-50 text-amber-900 [&>svg]:text-amber-900">
        <AlertTriangle className="h-4 w-4" />
        <AlertDescription>
          <span className="font-semibold">Secure your server before sharing.</span>{' '}
          Remote sharing exposes this dashboard to invited analysts over the public internet.
          Before enabling the tunnel, confirm the host has an up-to-date OS, firewall rules
          restricting inbound access to the share port, and (recommended) only allows the
          tunnel through a reverse proxy you control. Each invited analyst gets read-only
          access scoped to the services you grant — but the underlying server is yours to
          harden.
        </AlertDescription>
      </Alert>

      {statusErrorMsg && (
        <Alert variant="destructive">
          <AlertDescription>{statusErrorMsg}</AlertDescription>
        </Alert>
      )}
      {actionError && (
        <Alert variant="destructive">
          <AlertDescription>{actionError}</AlertDescription>
        </Alert>
      )}

      {statusForPanels === null && !statusErrorMsg ? (
        // First-render skeleton: React Query yields ``undefined`` (which
        // we coerce to null) until the initial /api/admin/share/status
        // fetch returns. Pre-fix the page showed an empty
        // SharingControlPanel + tabs with no data — looked broken until
        // ~300ms later when status arrived. With the hover-prefetch on
        // the Admin → Share Dashboard link, this skeleton usually does
        // not appear at all (cache hit), but it's a clean fallback on
        // cold navigations.
        <FormSkeleton />
      ) : (
        <>
          <SharingControlPanel status={statusForPanels} onRefresh={refresh} onError={setActionError} />

          <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
            <TabsList>
              <TabsTrigger value="invites">Invitations</TabsTrigger>
              <TabsTrigger value="sessions">Sessions</TabsTrigger>
              <TabsTrigger value="audit">Audit</TabsTrigger>
            </TabsList>

            {/* Gate each panel on activeTab so the tab-bundled JS chunks
               * stay out of the initial parse. ``auditEmailFilter`` is
               * already lifted to the parent for the invites→audit jump,
               * so AuditLogPanel's filter state survives the unmount.
               * Other per-panel state (table sort, pagination cursor)
               * resets on tab switch — acceptable tradeoff for the
               * cold-load bundle saving. */}
            <TabsContent value="invites" className="pt-4">
              {activeTab === 'invites' && (
                <InvitationsPanel
                  status={statusForPanels}
                  onRefresh={refresh}
                  onError={setActionError}
                  onViewAuditLogs={(email) => {
                    setAuditEmailFilter(email)
                    setActiveTab('audit')
                  }}
                />
              )}
            </TabsContent>

            <TabsContent value="sessions" className="pt-4">
              {activeTab === 'sessions' && (
                <SessionsPanel status={statusForPanels} onRefresh={refresh} onError={setActionError} />
              )}
            </TabsContent>

            <TabsContent value="audit" className="pt-4">
              {activeTab === 'audit' && (
                <AuditLogPanel
                  status={statusForPanels}
                  onError={setActionError}
                  initialEmailFilter={auditEmailFilter}
                  onClearInitialFilter={() => setAuditEmailFilter('')}
                />
              )}
            </TabsContent>
          </Tabs>
        </>
      )}
    </div>
  )
}
