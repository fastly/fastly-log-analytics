'use client'

import * as React from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, ArrowLeft, ShieldAlert } from 'lucide-react'
import Link from 'next/link'

import { Alert, AlertDescription } from '@/components/ui/alert'
import { buttonVariants } from '@/components/ui/button'
import { PageHeader } from '@/components/ui/page-header'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { AuditLogPanel } from '@/components/share-dashboard/AuditLogPanel'
import { InvitationsPanel } from '@/components/share-dashboard/InvitationsPanel'
import { SessionsPanel } from '@/components/share-dashboard/SessionsPanel'
import { SharingControlPanel } from '@/components/share-dashboard/SharingControlPanel'
import type { ShareStatus } from '@/components/share-dashboard/utils'
import { FormSkeleton } from '@/components/skeletons/PageSkeleton'
import { client, extractApiError } from '@/lib/api'

// Shared query key so the hover-prefetch on the Admin → Share Dashboard
// link (in /admin/page.tsx) populates the same React Query cache entry
// the page reads on mount. Resulting UX: by the time the operator
// clicks Share Dashboard, the status payload is already in cache —
// page paints real content immediately instead of skeleton-then-swap.
export const SHARE_STATUS_QUERY_KEY = ['admin', 'share', 'status'] as const

export default function ShareDashboardPage() {
  const queryClient = useQueryClient()
  const [actionError, setActionError] = React.useState('')
  const [activeTab, setActiveTab] = React.useState('invites')
  const [auditEmailFilter, setAuditEmailFilter] = React.useState('')

  // React Query handles the polling, cache, and prefetch interop. The
  // refetchInterval matches the previous setInterval(refresh, 10_000)
  // cadence so live updates while the operator is on this page still
  // refresh at the same rate.
  const { data: status, error: statusError, refetch } = useQuery({
    queryKey: SHARE_STATUS_QUERY_KEY,
    queryFn: async ({ signal }) => {
      const { data, response } = await client.GET('/api/admin/share/status' as any, { signal, })
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as ShareStatus
    },
    refetchInterval: 10_000,
    // 30s staleTime so the hover-prefetch from the /admin PageHeader chip
    // is reused on click even when the user lingers on hover. Live
    // polling still ticks at 10s while the page is open; staleTime only
    // affects the initial mount-time decision to refetch vs. use cache.
    staleTime: 30_000,
  })
  const refresh = React.useCallback(async () => {
    await refetch()
    queryClient.invalidateQueries({ queryKey: SHARE_STATUS_QUERY_KEY })
  }, [refetch, queryClient])
  const statusErrorMsg = statusError
    ? extractApiError(statusError as any) || (statusError as Error).message || 'unable to load status'
    : ''
  // React Query yields ``undefined`` until the first fetch resolves; the
  // child panels' props are typed ``ShareStatus | null``, so coerce.
  const statusForPanels: ShareStatus | null = status ?? null

  return (
    <div className="space-y-6">
      <PageHeader
        title="Remote Dashboard Sharing"
        description="Start the share tunnel, manage analyst invitations, monitor live sessions, and review the audit trail."
        icon={ShieldAlert}
      >
        <Link href="/admin" prefetch={true} className={buttonVariants({ variant: 'outline', size: 'sm' })}>
          <ArrowLeft className="h-4 w-4 mr-1" />
          Back to Admin
        </Link>
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

            <TabsContent value="invites" className="pt-4">
              <InvitationsPanel
                status={statusForPanels}
                onRefresh={refresh}
                onError={setActionError}
                onViewAuditLogs={(email) => {
                  setAuditEmailFilter(email)
                  setActiveTab('audit')
                }}
              />
            </TabsContent>

            <TabsContent value="sessions" className="pt-4">
              <SessionsPanel status={statusForPanels} onRefresh={refresh} onError={setActionError} />
            </TabsContent>

            <TabsContent value="audit" className="pt-4">
              <AuditLogPanel
                status={statusForPanels}
                onError={setActionError}
                initialEmailFilter={auditEmailFilter}
                onClearInitialFilter={() => setAuditEmailFilter('')}
              />
            </TabsContent>
          </Tabs>
        </>
      )}
    </div>
  )
}
