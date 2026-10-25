'use client'

import * as React from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, Loader2, RefreshCw, ShieldAlert } from 'lucide-react'
import dynamic from 'next/dynamic'
import { useSearchParams } from 'next/navigation'
import { parseAsStringLiteral, useQueryState } from 'nuqs'

import { Alert, AlertDescription } from '@/components/ui/alert'
import { BackToAdminLink } from '@/components/ReportLayout'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { DeployRemoteFrontendDialog, TeardownRemoteFrontendDialog } from '@/components/DeployRemoteFrontendDialog/DeployRemoteFrontendDialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { PageHeader } from '@/components/ui/confirm-dialog'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Table,
  TableBody,
  TableCell,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { InvitationsPanel } from '@/components/share-dashboard/InvitationsPanel'
import {
  SharingControlPanel,
  SortableHead,
  useTableSort,
  type AuditLog,
  type ShareStatus,
  type SortAccessors,
} from '@/components/share-dashboard/SharingControlPanel'
import { client, extractApiError, unwrapClient } from '@/lib/api'
import { formatDeterministicUTC } from '@/lib/format'
import { queryKeys } from '@/lib/query-keys'
import { cn } from '@/lib/utils'
import Loading from './loading'

const SHARE_TABS = ['invites', 'sessions', 'audit'] as const
type ShareTab = (typeof SHARE_TABS)[number]
const VALID_SHARE_TABS: ReadonlySet<string> = new Set(SHARE_TABS)

const shareTabParser = parseAsStringLiteral(SHARE_TABS)
  .withDefault('invites')
  .withOptions({ clearOnDefault: true, history: 'replace', shallow: true })

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

const EVENT_TYPE_FILTER_OPTIONS = [
  'ALL',
  'LOGIN_SUCCESS',
  'LOGIN_FAIL',
  'LOCKOUT',
  'INVITE_CREATE',
  'INVITE_REVOKE',
  'INVITE_DELETE',
  'INVITE_CLAIMED',
  'SESSION_BOOT',
  'SESSION_TIMEOUT',
  'TOS_ACCEPTED',
  'TUNNEL_START',
  'TUNNEL_RESUMED',
  'SHARE_START',
  'SHARE_STOP',
  'PANIC_TRIGGERED',
  'BACKUP_EXPORTED',
  'BACKUP_IMPORTED',
  'CLAIM_FAIL',
  'OAUTH_AUTH_INIT',
  'OAUTH_CALLBACK_FAIL',
  'OAUTH_VERIFY_FAIL',
  'OAUTH_INVITE_NOT_FOUND',
  'LOGIN_SUCCESS_OAUTH',
]

type AuditFilters = {
  event_type: string
  email: string
  since: string
  until: string
}

interface AuditLogPanelProps {
  status: ShareStatus | null
  onError: (msg: string) => void
  initialEmailFilter?: string
  onClearInitialFilter?: () => void
}

export function AuditLogPanel({ status, onError, initialEmailFilter, onClearInitialFilter }: AuditLogPanelProps) {
  const [filters, setFilters] = React.useState<AuditFilters>({
    event_type: 'ALL',
    email: initialEmailFilter || '',
    since: '',
    until: '',
  })
  const [appliedFilters, setAppliedFilters] = React.useState<AuditFilters | null>(() =>
    initialEmailFilter
      ? { event_type: 'ALL', email: initialEmailFilter, since: '', until: '' }
      : null,
  )

  const [seenInitialEmail, setSeenInitialEmail] = React.useState(initialEmailFilter)
  if (initialEmailFilter !== seenInitialEmail) {
    setSeenInitialEmail(initialEmailFilter)
    setFilters((f) => ({ ...f, email: initialEmailFilter || '' }))
    setAppliedFilters(
      initialEmailFilter
        ? { event_type: 'ALL', email: initialEmailFilter, since: '', until: '' }
        : null,
    )
  }

  const auditQuery = useQuery({
    queryKey: ['share-audit-logs', appliedFilters],
    enabled: appliedFilters !== null,
    retry: false,
    queryFn: async () => {
      onError('')
      try {
        const params: Record<string, string | number> = { limit: 200 }
        if (appliedFilters?.event_type && appliedFilters.event_type !== 'ALL') {
          params.event_type = appliedFilters.event_type
        }
        if (appliedFilters?.email) params.email = appliedFilters.email
        if (appliedFilters?.since) params.since = appliedFilters.since
        if (appliedFilters?.until) params.until = appliedFilters.until
        const data = await unwrapClient(
          client.GET('/api/admin/share/audit-logs', {
            params: { query: params },
          }),
        )
        return data?.audit_logs || []
      } catch (e) {
        onError(extractApiError(e))
        throw e
      }
    },
  })

  const applyFilters = () => {
    setAppliedFilters({ ...filters })
    if (
      appliedFilters &&
      appliedFilters.event_type === filters.event_type &&
      appliedFilters.email === filters.email &&
      appliedFilters.since === filters.since &&
      appliedFilters.until === filters.until
    ) {
      auditQuery.refetch()
    }
  }

  const handleClear = () => {
    setFilters({ event_type: 'ALL', email: '', since: '', until: '' })
    setAppliedFilters(null)
    if (onClearInitialFilter) onClearInitialFilter()
  }

  const filteredLogs = appliedFilters !== null ? (auditQuery.data ?? null) : null
  const loading = auditQuery.isFetching
  const rows = filteredLogs ?? status?.audit_logs ?? []

  const auditAccessors = React.useMemo<SortAccessors<AuditLog>>(
    () => ({
      timestamp: (r) => r.timestamp ?? null,
      event_type: (r) => r.event_type ?? '',
      email: (r) => (r.email ?? '').toLowerCase(),
      ip_address: (r) => r.ip_address ?? '',
      details: (r) => r.details ?? '',
    }),
    [],
  )
  const { sorted, sortKey, sortDir, toggle } = useTableSort(rows, auditAccessors, {
    defaultKey: 'timestamp',
    defaultDir: 'desc',
  })

  return (
    <div className="space-y-3" data-testid="admin-share-audit-panel">
      <section className="rounded-lg border bg-card p-3 space-y-2">
        <h4 className="text-sm font-semibold">Filter audit log</h4>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-2">
          <div className="space-y-1">
            <Label htmlFor="audit-event-type" className="text-[10px]">Event type</Label>
            <Select
              value={filters.event_type}
              onValueChange={(v) => setFilters((f) => ({ ...f, event_type: v ?? 'ALL' }))}
            >
              <SelectTrigger id="audit-event-type">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {EVENT_TYPE_FILTER_OPTIONS.map((opt) => (
                  <SelectItem key={opt} value={opt}>
                    {opt}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <Label htmlFor="audit-email" className="text-[10px]">Email contains</Label>
            <Input
              id="audit-email"
              value={filters.email}
              onChange={(e) => setFilters((f) => ({ ...f, email: e.target.value }))}
              placeholder="alice@…"
            />
          </div>
          <div className="space-y-1">
            <Label htmlFor="audit-since" className="text-[10px]">Since (ISO-Z)</Label>
            <Input
              id="audit-since"
              value={filters.since}
              onChange={(e) => setFilters((f) => ({ ...f, since: e.target.value }))}
              placeholder="2026-05-25T00:00:00Z"
            />
          </div>
          <div className="space-y-1">
            <Label htmlFor="audit-until" className="text-[10px]">Until (ISO-Z)</Label>
            <Input
              id="audit-until"
              value={filters.until}
              onChange={(e) => setFilters((f) => ({ ...f, until: e.target.value }))}
              placeholder="2026-05-27T23:59:59Z"
            />
          </div>
        </div>
        <div className="flex justify-end gap-2">
          <Button
            size="sm"
            variant="ghost"
            onClick={handleClear}
            data-testid="admin-share-audit-clear-btn"
          >
            Clear
          </Button>
          <Button
            size="sm"
            onClick={applyFilters}
            disabled={loading}
            data-testid="admin-share-audit-apply-btn"
          >
            {loading && <Loader2 className="h-3 w-3 mr-1 animate-spin" />}
            Apply
          </Button>
        </div>
      </section>
      <section className="rounded-lg border bg-card p-4 max-h-[480px] overflow-y-auto">
        <h4 className="text-sm font-semibold mb-2">
          {filteredLogs ? 'Filtered audit events' : 'Recent audit events'}
        </h4>
        <Table className="font-mono text-[11px]">
          <TableHeader>
            <TableRow>
              <SortableHead label="Time" sortKey="timestamp" activeKey={sortKey} dir={sortDir} onSort={toggle} className="w-[180px]" />
              <SortableHead label="Event" sortKey="event_type" activeKey={sortKey} dir={sortDir} onSort={toggle} />
              <SortableHead label="Email" sortKey="email" activeKey={sortKey} dir={sortDir} onSort={toggle} />
              <SortableHead label="IP" sortKey="ip_address" activeKey={sortKey} dir={sortDir} onSort={toggle} />
              <SortableHead label="Details" sortKey="details" activeKey={sortKey} dir={sortDir} onSort={toggle} />
            </TableRow>
          </TableHeader>
          <TableBody>
            {sorted.map((row: AuditLog, i: number) => (
              <TableRow key={`${row.id || i}`}>
                <TableCell className="text-muted-foreground align-top whitespace-nowrap">
                  {formatDeterministicUTC(row.timestamp)}
                </TableCell>
                <TableCell className="align-top">
                  <Badge variant="outline" className="text-[10px]">
                    {row.event_type}
                  </Badge>
                </TableCell>
                <TableCell className="align-top">{row.email || '—'}</TableCell>
                <TableCell className="align-top whitespace-nowrap">{row.ip_address || '—'}</TableCell>
                <TableCell className="text-muted-foreground align-top whitespace-normal break-words max-w-[420px]">
                  {row.details}
                </TableCell>
              </TableRow>
            ))}
            {!rows.length && (
              <TableRow>
                <TableCell colSpan={5} className="text-center text-xs text-muted-foreground">
                  No audit events yet.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </section>
    </div>
  )
}

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
