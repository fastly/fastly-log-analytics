"use client"

import * as React from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import { useSearchParams } from "next/navigation"
import { useQueryState } from "nuqs"
import { AlertTriangle, RefreshCw, ShieldAlert } from "lucide-react"

import { Alert, AlertDescription } from "@/components/ui/alert"
import { BackToAdminLink } from "@/components/ReportLayout"
import { Button } from "@/components/ui/button"
import { PageHeader } from "@/components/ui/confirm-dialog"
import { DeployRemoteFrontendDialog, TeardownRemoteFrontendDialog } from "@/components/DeployRemoteFrontendDialog/DeployRemoteFrontendDialog"
import { Skeleton } from "@/components/ui/skeleton"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { client, extractApiError, queryKeys, unwrapClient } from "@/lib/api"
import { cn } from "@/lib/utils"
import Loading from "./loading"
import { InvitationsPanel } from "./InvitationsPanel"
import { AuditLogPanel, SessionsPanel, SharingControlPanel } from "./SharingControlPanel"
import { normalizeShareTab, shareTabParser, type ShareStatus } from "./types"

export * from "./types"
export * from "./SharingControlPanel"
export * from "./CreateInviteDialog"
export * from "./InviteAnalystDialog"
export * from "./InvitationsPanel"

const SHARE_STATUS_QUERY_KEY = queryKeys.admin.shareStatus()
const SHARE_LIVE_QUERY_KEY = queryKeys.admin.shareLive()

type ShareLiveFields = Partial<
  Pick<ShareStatus, 'sharing_active' | 'public_url' | 'active_session_count' | 'rate_limits' | 'telemetry'>
>

function ShareDashboardPageContent() {
  const queryClient = useQueryClient()
  const searchParams = useSearchParams()
  const activeServiceId = searchParams?.get('service')

  const [actionError, setActionError] = React.useState('')
  const [activeTab, setActiveTab] = useQueryState('tab', shareTabParser)
  const [auditEmailFilter, setAuditEmailFilter] = React.useState('')

  const handleTabChange = React.useCallback(
    (nextTab: string) => { void setActiveTab(normalizeShareTab(nextTab)) },
    [setActiveTab],
  )

  const {
    data: status,
    error: statusError,
    isLoading: statusLoading,
    isFetching: statusFetching,
    refetch,
  } = useQuery({
    queryKey: SHARE_STATUS_QUERY_KEY,
    queryFn: async ({ signal }) => (await unwrapClient(client.GET('/api/admin/share/status', { signal }))) as ShareStatus,
    staleTime: 30_000,
  })

  const { data: live } = useQuery({
    queryKey: SHARE_LIVE_QUERY_KEY,
    queryFn: async ({ signal }) => (await unwrapClient(client.GET('/api/admin/share/live', { signal }))) as ShareLiveFields,
    staleTime: 10_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })

  const refresh = React.useCallback(async () => {
    await refetch()
    queryClient.invalidateQueries({ queryKey: SHARE_LIVE_QUERY_KEY })
  }, [refetch, queryClient])

  const statusErrorMsg = statusError
    ? extractApiError(statusError) || (statusError as Error).message || 'unable to load status'
    : ''

  const statusForPanels: ShareStatus | null = status ? { ...status, ...(live ?? {}) } : null
  const isInitialLoading = statusForPanels === null && !statusErrorMsg && statusLoading
  const isRefreshing = statusFetching && !isInitialLoading
  const activeService = status?.services?.find((s) => s.service_id === activeServiceId)

  return (
    <div className="space-y-6" data-testid="admin-share-root">
      <PageHeader
        title="Remote Dashboard Sharing"
        description="Start the share tunnel, manage analyst invitations, monitor live sessions, and review the audit trail."
        icon={ShieldAlert}
      >
        {activeService && activeService.remote_frontend_deployed ? (
          <TeardownRemoteFrontendDialog
            serviceId={activeService.service_id}
            domainName={activeService.sharing_domain || ''}
            onSuccess={refresh}
          />
        ) : (
          <DeployRemoteFrontendDialog />
        )}
        <Button variant="outline" size="sm" onClick={refresh} disabled={isRefreshing} data-testid="admin-share-refresh-btn">
          <RefreshCw className={cn('h-4 w-4 mr-1', isRefreshing && 'animate-spin')} />
          Refresh
        </Button>
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
        <Alert variant="destructive" data-testid="admin-share-status-error">
          <AlertDescription>{statusErrorMsg}</AlertDescription>
        </Alert>
      )}
      {actionError && (
        <Alert variant="destructive" data-testid="admin-share-action-error">
          <AlertDescription>{actionError}</AlertDescription>
        </Alert>
      )}

      <div
        className={cn(
          'min-h-[220px] [contain-intrinsic-size:auto_220px] [content-visibility:auto] transition-opacity',
          isRefreshing && 'opacity-40 pointer-events-none',
        )}
        data-testid="admin-share-control-reservation"
      >
        {statusForPanels === null && !statusErrorMsg ? (
          <div className="space-y-3" aria-busy="true">
            <Skeleton className="h-[220px] w-full rounded-xl" />
          </div>
        ) : (
          <SharingControlPanel status={statusForPanels} onRefresh={refresh} onError={setActionError} />
        )}
      </div>

      <Tabs value={activeTab} onValueChange={handleTabChange} className="w-full" data-testid="admin-share-tabs">
        <TabsList>
          <TabsTrigger value="invites" data-testid="admin-share-tab-invites">Invitations</TabsTrigger>
          <TabsTrigger value="sessions" data-testid="admin-share-tab-sessions">Sessions</TabsTrigger>
          <TabsTrigger value="audit" data-testid="admin-share-tab-audit">Audit</TabsTrigger>
        </TabsList>

        <div
          className={cn(
            'min-h-[360px] [contain-intrinsic-size:auto_360px] [content-visibility:auto] transition-opacity',
            isRefreshing && 'opacity-40 pointer-events-none',
          )}
          data-testid="admin-share-panel-reservation"
        >
          {statusForPanels === null && !statusErrorMsg ? (
            <div className="pt-4 space-y-3" aria-busy="true">
              <Skeleton className="h-[360px] w-full rounded-xl" />
            </div>
          ) : (
            <>
              <TabsContent value="invites" className="pt-4">
                {activeTab === 'invites' && (
                  <InvitationsPanel
                    status={statusForPanels}
                    onRefresh={refresh}
                    onError={setActionError}
                    onViewAuditLogs={(email) => {
                      setAuditEmailFilter(email)
                      handleTabChange('audit')
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
            </>
          )}
        </div>
      </Tabs>
    </div>
  )
}

export default function ShareDashboardPage() {
  return (
    <React.Suspense fallback={<Loading />}>
      <ShareDashboardPageContent />
    </React.Suspense>
  )
}
