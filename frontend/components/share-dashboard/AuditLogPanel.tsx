'use client'

import * as React from 'react'
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

import { SortableHead, useTableSort, type SortAccessors } from './sortable'
import { formatStamp, type AuditLog, type ShareStatus } from './utils'

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
  const [filteredLogs, setFilteredLogs] = React.useState<AuditLog[] | null>(null)
  const [loading, setLoading] = React.useState(false)

  React.useEffect(() => {
    if (initialEmailFilter) {
      setFilters((f) => ({ ...f, email: initialEmailFilter }))
      const autoApply = async () => {
        setLoading(true)
        onError('')
        try {
          const params: Record<string, string | number> = { limit: 200, email: initialEmailFilter }
          const { data, response } = await client.GET('/api/admin/share/audit-logs' as any, {
            params: { query: params },
          } as any)
          if (!response.ok) throw new Error(`status ${response.status}`)
          setFilteredLogs((data as any).audit_logs || [])
        } catch (e: any) {
          onError(extractApiError(e))
        } finally {
          setLoading(false)
        }
      }
      autoApply()
    } else {
      setFilters((f) => ({ ...f, email: '' }))
      setFilteredLogs(null)
    }
  }, [initialEmailFilter, onError])

  const applyFilters = async () => {
    setLoading(true)
    onError('')
    try {
      const params: Record<string, string | number> = { limit: 200 }
      if (filters.event_type && filters.event_type !== 'ALL') {
        params.event_type = filters.event_type
      }
      if (filters.email) params.email = filters.email
      if (filters.since) params.since = filters.since
      if (filters.until) params.until = filters.until
      const { data, response } = await client.GET('/api/admin/share/audit-logs' as any, {
        params: { query: params },
      } as any)
      if (!response.ok) throw new Error(`status ${response.status}`)
      setFilteredLogs((data as any).audit_logs || [])
    } catch (e: any) {
      onError(extractApiError(e))
    } finally {
      setLoading(false)
    }
  }

  const handleClear = () => {
    setFilters({ event_type: 'ALL', email: '', since: '', until: '' })
    setFilteredLogs(null)
    if (onClearInitialFilter) {
      onClearInitialFilter()
    }
  }

  const rows = filteredLogs ?? status?.audit_logs ?? []

  const auditAccessors = React.useMemo<SortAccessors<AuditLog>>(
    () => ({
      timestamp: (r) => r.timestamp ?? null,
      event_type: (r) => r.event_type ?? '',
      email: (r) => (r.email ?? '').toLowerCase(),
      details: (r) => r.details ?? '',
    }),
    [],
  )
  const { sorted, sortKey, sortDir, toggle } = useTableSort(rows, auditAccessors, {
    defaultKey: 'timestamp',
    defaultDir: 'desc',
  })

  return (
    <div className="space-y-3">
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
          <Button size="sm" variant="ghost" onClick={handleClear}>
            Clear
          </Button>
          <Button size="sm" onClick={applyFilters} disabled={loading}>
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
              <SortableHead label="Details" sortKey="details" activeKey={sortKey} dir={sortDir} onSort={toggle} />
            </TableRow>
          </TableHeader>
          <TableBody>
            {sorted.map((row: AuditLog, i: number) => (
              <TableRow key={`${row.id || i}`}>
                <TableCell className="text-muted-foreground align-top whitespace-nowrap">
                  {formatStamp(row.timestamp)}
                </TableCell>
                <TableCell className="align-top">
                  <Badge variant="outline" className="text-[10px]">
                    {row.event_type}
                  </Badge>
                </TableCell>
                <TableCell className="align-top">{row.email || '—'}</TableCell>
                <TableCell className="text-muted-foreground align-top whitespace-normal break-words max-w-[420px]">
                  {row.details}
                </TableCell>
              </TableRow>
            ))}
            {!rows.length && (
              <TableRow>
                <TableCell colSpan={4} className="text-center text-xs text-muted-foreground">
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
