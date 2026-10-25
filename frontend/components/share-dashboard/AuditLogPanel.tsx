'use client'

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'
import { Loader2 } from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Table,
  TableBody,
  TableCell,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { client, extractApiError } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'

import { SortableHead, useTableSort, type AuditLog, type ShareStatus, type SortAccessors } from './sortable'
import { formatDeterministicUTC } from '@/lib/date'

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
  // OAuth / OIDC analyst login events (design §4). Present under "ALL" without
  // these, but the filter dropdown must offer them explicitly.
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

  // The email filter follows the upstream `initialEmailFilter` (adjusted during render when it
  // changes); the query runs automatically when `appliedFilters` is non-null.
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
    if (onClearInitialFilter) {
      onClearInitialFilter()
    }
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
