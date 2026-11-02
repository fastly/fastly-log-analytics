"use client"

import * as React from "react"
import { useQuery } from "@tanstack/react-query"
import { flexRender, getCoreRowModel, useReactTable, type ColumnDef } from "@tanstack/react-table"
import { ArrowDown, ArrowUp, ArrowUpDown, Loader2, Lock } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { client, extractApiError, unwrapClient } from "@/lib/api"
import { cn, formatDeterministicUTC } from "@/lib/utils"
import {
  EVENT_TYPE_FILTER_OPTIONS,
  useShareMutation,
  useTableSort,
  type AuditFilters,
  type AuditLog,
  type ShareSession,
  type ShareStatus,
  type SortAccessors,
  type SortableHeadProps,
} from "./types"

export function SortableHead({ label, sortKey, activeKey, dir, onSort, className, ...thProps }: SortableHeadProps) {
  const active = activeKey === sortKey
  const ariaSort = active ? (dir === 'asc' ? 'ascending' : 'descending') : 'none'
  return (
    <TableHead aria-sort={ariaSort} className={cn('p-0', className)} {...thProps}>
      <button
        type="button"
        onClick={() => onSort(sortKey)}
        className="group/sort flex w-full items-center gap-1 px-2 py-2 text-left font-medium hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring rounded-sm"
      >
        <span className="truncate">{label}</span>
        <span className="ml-auto flex items-center shrink-0">
          {active ? (
            dir === 'asc' ? <ArrowUp className="h-3.5 w-3.5" /> : <ArrowDown className="h-3.5 w-3.5" />
          ) : (
            <ArrowUpDown className="h-3.5 w-3.5 opacity-0 group-hover/sort:opacity-50 transition-opacity" />
          )}
        </span>
      </button>
    </TableHead>
  )
}

export interface SessionsPanelProps {
  status: ShareStatus | null
  onRefresh: () => Promise<void> | void
  onError: (msg: string) => void
}

export function SessionsPanel({ status, onRefresh, onError }: SessionsPanelProps) {
  const { busy, run } = useShareMutation(onError, onRefresh)

  const lockoutsByIp = React.useMemo(
    () => new Map((status?.rate_limits?.lockouts || []).map((l) => [l.ip, l])),
    [status?.rate_limits?.lockouts],
  )
  const failuresByIp = React.useMemo(
    () => new Map((status?.rate_limits?.failures || []).map((f) => [f.ip, f])),
    [status?.rate_limits?.failures],
  )

  const handleBootSession = (sid: string) =>
    run(() => client.POST('/api/admin/share/sessions/{session_id}/boot', { params: { path: { session_id: sid } } }))

  const sessions = status?.sessions || []

  const accessors = React.useMemo<SortAccessors<ShareSession>>(
    () => ({
      analyst: (s) => (s.name || s.email || '').toLowerCase(),
      signin: (s) => s.auth_method || 'passcode',
      ip: (s) => s.ip_address || '',
      last_active_time: (s) => s.last_active_time ?? null,
    }),
    [],
  )
  const { sorted, sortKey, sortDir, toggle } = useTableSort(sessions, accessors, {
    defaultKey: 'last_active_time',
    defaultDir: 'desc',
  })

  const columns = React.useMemo<ColumnDef<ShareSession>[]>(
    () => [
      {
        id: 'analyst',
        cell: ({ row }) => {
          const s = row.original
          return (
            <>
              {s.name || s.email}
              <div className="text-[10px] text-muted-foreground">{s.email}</div>
            </>
          )
        },
      },
      {
        id: 'signin',
        cell: ({ row }) => {
          const s = row.original
          return s.auth_method === 'oauth' ? (
            <Badge variant="outline" className="text-[10px] font-normal">
              SSO{s.oauth_provider ? ` · ${s.oauth_provider}` : ''}
            </Badge>
          ) : (
            <span className="text-[10px] text-muted-foreground">Passcode</span>
          )
        },
      },
      {
        id: 'ip',
        cell: ({ row }) => {
          const s = row.original
          return (
            <div className="flex items-center gap-1 flex-wrap">
              <span>{s.ip_address}</span>
              {lockoutsByIp.has(s.ip_address) && (
                <Badge variant="destructive" className="text-[10px]">
                  <Lock className="h-2 w-2 mr-0.5" />
                  locked {lockoutsByIp.get(s.ip_address)?.remaining_s}s
                </Badge>
              )}
              {!lockoutsByIp.has(s.ip_address) && failuresByIp.has(s.ip_address) && (
                <Badge variant="outline" className="text-[10px] text-amber-700 dark:text-amber-400">
                  {failuresByIp.get(s.ip_address)?.count} fail
                </Badge>
              )}
            </div>
          )
        },
      },
      { id: 'last_active_time', cell: ({ row }) => formatDeterministicUTC(row.original.last_active_time) },
      {
        id: 'action',
        cell: ({ row }) => (
          <Button size="sm" variant="ghost" onClick={() => handleBootSession(row.original.session_id)} disabled={busy}>
            Boot
          </Button>
        ),
      },
    ],
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [lockoutsByIp, failuresByIp, busy],
  )

  const table = useReactTable({ data: sorted, columns, getCoreRowModel: getCoreRowModel() })

  return (
    <section className="rounded-lg border bg-card p-4 space-y-3" data-testid="admin-share-sessions-panel">
      <h4 className="text-sm font-semibold">Active sessions</h4>
      <Table>
        <TableHeader>
          <TableRow>
            <SortableHead label="Analyst" sortKey="analyst" activeKey={sortKey} dir={sortDir} onSort={toggle} />
            <SortableHead label="Sign-in" sortKey="signin" activeKey={sortKey} dir={sortDir} onSort={toggle} />
            <SortableHead label="IP" sortKey="ip" activeKey={sortKey} dir={sortDir} onSort={toggle} />
            <SortableHead label="Last activity" sortKey="last_active_time" activeKey={sortKey} dir={sortDir} onSort={toggle} />
            <TableHead className="text-right">Action</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {table.getRowModel().rows.map((row) => (
            <TableRow key={row.original.session_id}>
              {row.getVisibleCells().map((cell) => {
                let cellClass = 'text-xs'
                if (cell.column.id === 'ip') {
                  cellClass = 'text-xs font-mono'
                } else if (cell.column.id === 'action') {
                  cellClass = 'text-right'
                }
                return (
                  <TableCell key={cell.id} className={cellClass}>
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </TableCell>
                )
              })}
            </TableRow>
          ))}
          {!sessions.length && (
            <TableRow>
              <TableCell colSpan={5} className="text-center text-xs text-muted-foreground">No active sessions.</TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </section>
  )
}

export interface AuditLogPanelProps {
  status: ShareStatus | null
  onError: (msg: string) => void
  initialEmailFilter?: string
  onClearInitialFilter?: () => void
}

export function AuditLogPanel({ status, onError, initialEmailFilter, onClearInitialFilter }: AuditLogPanelProps) {
  const [filters, setFilters] = React.useState<AuditFilters>({
    event_type: 'ALL', email: initialEmailFilter || '', since: '', until: '',
  })
  const [appliedFilters, setAppliedFilters] = React.useState<AuditFilters | null>(() =>
    initialEmailFilter ? { event_type: 'ALL', email: initialEmailFilter, since: '', until: '' } : null,
  )

  const [seenInitialEmail, setSeenInitialEmail] = React.useState(initialEmailFilter)
  if (initialEmailFilter !== seenInitialEmail) {
    setSeenInitialEmail(initialEmailFilter)
    setFilters((f) => ({ ...f, email: initialEmailFilter || '' }))
    setAppliedFilters(initialEmailFilter ? { event_type: 'ALL', email: initialEmailFilter, since: '', until: '' } : null)
  }

  const auditQuery = useQuery({
    queryKey: ['share-audit-logs', appliedFilters],
    enabled: appliedFilters !== null,
    retry: false,
    queryFn: async () => {
      onError('')
      try {
        const params: Record<string, string | number> = { limit: 200 }
        if (appliedFilters?.event_type && appliedFilters.event_type !== 'ALL') params.event_type = appliedFilters.event_type
        if (appliedFilters?.email) params.email = appliedFilters.email
        if (appliedFilters?.since) params.since = appliedFilters.since
        if (appliedFilters?.until) params.until = appliedFilters.until
        const data = await unwrapClient(client.GET('/api/admin/share/audit-logs', { params: { query: params } }))
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

  const auditColumns = React.useMemo<ColumnDef<AuditLog>[]>(
    () => [
      { id: 'timestamp', cell: ({ row }) => formatDeterministicUTC(row.original.timestamp) },
      { id: 'event_type', cell: ({ row }) => <Badge variant="outline" className="text-[10px]">{row.original.event_type}</Badge> },
      { id: 'email', cell: ({ row }) => row.original.email || '—' },
      { id: 'ip_address', cell: ({ row }) => row.original.ip_address || '—' },
      { id: 'details', cell: ({ row }) => row.original.details },
    ],
    [],
  )
  const auditTable = useReactTable({ data: sorted, columns: auditColumns, getCoreRowModel: getCoreRowModel() })

  return (
    <div className="space-y-3" data-testid="admin-share-audit-panel">
      <section className="rounded-lg border bg-card p-3 space-y-2">
        <h4 className="text-sm font-semibold">Filter audit log</h4>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-2">
          <div className="space-y-1">
            <Label htmlFor="audit-event-type" className="text-[10px]">Event type</Label>
            <Select value={filters.event_type} onValueChange={(v) => setFilters((f) => ({ ...f, event_type: v ?? 'ALL' }))}>
              <SelectTrigger id="audit-event-type"><SelectValue /></SelectTrigger>
              <SelectContent>
                {EVENT_TYPE_FILTER_OPTIONS.map((opt) => (
                  <SelectItem key={opt} value={opt}>{opt}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <Label htmlFor="audit-email" className="text-[10px]">Email contains</Label>
            <Input id="audit-email" value={filters.email} onChange={(e) => setFilters((f) => ({ ...f, email: e.target.value }))} placeholder="alice@…" />
          </div>
          <div className="space-y-1">
            <Label htmlFor="audit-since" className="text-[10px]">Since (ISO-Z)</Label>
            <Input id="audit-since" value={filters.since} onChange={(e) => setFilters((f) => ({ ...f, since: e.target.value }))} placeholder="2026-05-25T00:00:00Z" />
          </div>
          <div className="space-y-1">
            <Label htmlFor="audit-until" className="text-[10px]">Until (ISO-Z)</Label>
            <Input id="audit-until" value={filters.until} onChange={(e) => setFilters((f) => ({ ...f, until: e.target.value }))} placeholder="2026-05-27T23:59:59Z" />
          </div>
        </div>
        <div className="flex justify-end gap-2">
          <Button size="sm" variant="ghost" onClick={handleClear} data-testid="admin-share-audit-clear-btn">Clear</Button>
          <Button size="sm" onClick={applyFilters} disabled={loading} data-testid="admin-share-audit-apply-btn">
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
            {auditTable.getRowModel().rows.map((row, i: number) => (
              <TableRow key={`${row.original.id || i}`}>
                {row.getVisibleCells().map((cell) => {
                  let cellClass = 'align-top'
                  if (cell.column.id === 'timestamp') {
                    cellClass = 'text-muted-foreground align-top whitespace-nowrap'
                  } else if (cell.column.id === 'ip_address') {
                    cellClass = 'align-top whitespace-nowrap'
                  } else if (cell.column.id === 'details') {
                    cellClass = 'text-muted-foreground align-top whitespace-normal break-words max-w-[420px]'
                  }
                  return (
                    <TableCell key={cell.id} className={cellClass}>
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </TableCell>
                  )
                })}
              </TableRow>
            ))}
            {!rows.length && (
              <TableRow>
                <TableCell colSpan={5} className="text-center text-xs text-muted-foreground">No audit events yet.</TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </section>
    </div>
  )
}
