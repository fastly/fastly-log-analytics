'use client'

import * as React from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, RefreshCw, ShieldAlert } from 'lucide-react'
import dynamic from 'next/dynamic'
import { useSearchParams } from 'next/navigation'
import { parseAsStringLiteral, useQueryState } from 'nuqs'

import { Alert, AlertDescription } from '@/components/ui/alert'
import { BackToAdminLink } from '@/components/BackToAdminLink'
import { Button } from '@/components/ui/button'
import { DeployRemoteFrontendDialog, TeardownRemoteFrontendDialog } from '@/components/DeployRemoteFrontendDialog/DeployRemoteFrontendDialog'
import { PageHeader } from '@/components/ui/page-header'
import { Skeleton } from '@/components/ui/skeleton'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { InvitationsPanel } from '@/components/share-dashboard/InvitationsPanel'
import { SharingControlPanel } from '@/components/share-dashboard/SharingControlPanel'
import type { ShareStatus } from '@/components/share-dashboard/sortable'
import { client, extractApiError } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import { queryKeys } from '@/lib/query-keys'
import { cn } from '@/lib/utils'
import Loading from './loading'

const SHARE_TABS = ['invites', 'sessions', 'audit'] as const
type ShareTab = (typeof SHARE_TABS)[number]
const VALID_SHARE_TABS: ReadonlySet<string> = new Set(SHARE_TABS)

// `?tab=` is the shareable deep-link; the default tab is omitted from the URL.
const shareTabParser = parseAsStringLiteral(SHARE_TABS)
  .withDefault('invites')
  .withOptions({ clearOnDefault: true, history: 'replace', shallow: true })

// InvitationsPanel is the default tab on cold load — static-importing it
// keeps the initial paint off the dynamic-chunk fetch path.
// SessionsPanel + AuditLogPanel stay lazy because they only mount on
// tab click; ``ssr: false`` keeps their hooks off the SSR pass.
const SessionsPanel = dynamic(
  () => import('@/components/share-dashboard/SharingControlPanel').then((m) => m.SessionsPanel),
  {
    ssr: false,
    loading: () => (
      <div className="space-y-3" aria-busy="true">
        <Skeleton className="h-[360px] w-full rounded-xl" />
      </div>
    ),
  },
)
const AuditLogPanel = dynamic(
  () => import('@/components/share-dashboard/AuditLogPanel').then((m) => m.AuditLogPanel),
  {
    ssr: false,
    loading: () => (
      <div className="space-y-3" aria-busy="true">
        <Skeleton className="h-[360px] w-full rounded-xl" />
      </div>
    ),
  },
)

// Shared query key so the hover-prefetch on the Admin → Share Dashboard
// link (in /admin/page.tsx) populates the same React Query cache entry
// the page reads on mount. Resulting UX: by the time the operator
// clicks Share Dashboard, the status payload is already in cache —
// page paints real content immediately instead of skeleton-then-swap.
const SHARE_STATUS_QUERY_KEY = queryKeys.admin.shareStatus()
// Companion key for the lean /live poll — keeps its own cache slot so
// the mount-time /status query isn't refetched on the 10-s tick.
const SHARE_LIVE_QUERY_KEY = queryKeys.admin.shareLive()

type ShareLiveFields = Partial<
  Pick<
    ShareStatus,
    'sharing_active' | 'public_url' | 'active_session_count' | 'rate_limits' | 'telemetry'
  >
>

function normalizeShareTab(raw: string | null | undefined): ShareTab {
  return raw && VALID_SHARE_TABS.has(raw) ? (raw as ShareTab) : 'invites'
}

function ShareDashboardPageContent() {
  const queryClient = useQueryClient()
  const searchParams = useSearchParams()
  const activeServiceId = searchParams?.get('service')

  const [actionError, setActionError] = React.useState('')
  const [activeTab, setActiveTab] = useQueryState('tab', shareTabParser)
  const [auditEmailFilter, setAuditEmailFilter] = React.useState('')

  const handleTabChange = React.useCallback(
    (nextTab: string) => {
      void setActiveTab(normalizeShareTab(nextTab))
    },
    [setActiveTab],
  )

  // /status carries the full mount-time payload — services, invites,
  // sessions, audit logs (~11 KB). Mutations refetch this. No
  // refetchInterval; the 10-s live tick rides on /live below.
  const {
    data: status,
    error: statusError,
    isLoading: statusLoading,
    isFetching: statusFetching,
    refetch,
  } = useQuery({
    queryKey: SHARE_STATUS_QUERY_KEY,
    queryFn: async ({ signal }) => {
      return (await unwrapClient(client.GET('/api/admin/share/status', { signal }))) as ShareStatus
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
      return (await unwrapClient(client.GET('/api/admin/share/live', { signal }))) as ShareLiveFields
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
    ? extractApiError(statusError) || (statusError as Error).message || 'unable to load status'
    : ''

  // Merge /live fields into /status so SharingControlPanel (which reads
  // active_session_count + rate_limits + telemetry) sees fresh data on
  // every 10-s tick without us touching the full /status payload.
  const statusForPanels: ShareStatus | null = status
    ? { ...status, ...(live ?? {}) }
    : null

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
        <Button
          variant="outline"
          size="sm"
          onClick={refresh}
          disabled={isRefreshing}
          data-testid="admin-share-refresh-btn"
        >
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
          <SharingControlPanel
            status={statusForPanels}
            onRefresh={refresh}
            onError={setActionError}
          />
        )}
      </div>

      <Tabs
        value={activeTab}
        onValueChange={handleTabChange}
        className="w-full"
        data-testid="admin-share-tabs"
      >
        <TabsList>
          <TabsTrigger value="invites" data-testid="admin-share-tab-invites">
            Invitations
          </TabsTrigger>
          <TabsTrigger value="sessions" data-testid="admin-share-tab-sessions">
            Sessions
          </TabsTrigger>
          <TabsTrigger value="audit" data-testid="admin-share-tab-audit">
            Audit
          </TabsTrigger>
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
                  <SessionsPanel
                    status={statusForPanels}
                    onRefresh={refresh}
                    onError={setActionError}
                  />
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
