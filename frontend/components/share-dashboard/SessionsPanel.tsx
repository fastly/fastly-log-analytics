'use client'

import * as React from 'react'
import { Lock } from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { client, extractApiError } from '@/lib/api'

import { formatStamp, type RateLimitFailure, type RateLimitLockout, type ShareStatus } from './utils'

interface SessionsPanelProps {
  status: ShareStatus | null
  onRefresh: () => Promise<void> | void
  onError: (msg: string) => void
}

export function SessionsPanel({ status, onRefresh, onError }: SessionsPanelProps) {
  const [busy, setBusy] = React.useState(false)

  const lockoutsByIp = React.useMemo(() => {
    const map = new Map<string, RateLimitLockout>()
    for (const l of status?.rate_limits?.lockouts || []) map.set(l.ip, l)
    return map
  }, [status?.rate_limits?.lockouts])

  const failuresByIp = React.useMemo(() => {
    const map = new Map<string, RateLimitFailure>()
    for (const f of status?.rate_limits?.failures || []) map.set(f.ip, f)
    return map
  }, [status?.rate_limits?.failures])

  const handleBootSession = async (sid: string) => {
    setBusy(true)
    try {
      await client.POST('/api/admin/share/sessions/{session_id}/boot' as any, {
        params: { path: { session_id: sid } },
      } as any)
      await onRefresh()
    } catch (e: any) {
      onError(extractApiError(e))
    } finally {
      setBusy(false)
    }
  }

  const sessions = status?.sessions || []

  return (
    <section className="rounded-lg border bg-card p-4 space-y-3">
      <h4 className="text-sm font-semibold">Active sessions</h4>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Analyst</TableHead>
            <TableHead>IP</TableHead>
            <TableHead>Last activity</TableHead>
            <TableHead className="text-right">Action</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {sessions.map((s: any) => (
            <TableRow key={s.session_id}>
              <TableCell className="text-xs">
                {s.name || s.email}
                <div className="text-[10px] text-muted-foreground">{s.email}</div>
              </TableCell>
              <TableCell className="text-xs font-mono">
                <div className="flex items-center gap-1 flex-wrap">
                  <span>{s.ip_address}</span>
                  {lockoutsByIp.has(s.ip_address) && (
                    <Badge variant="destructive" className="text-[10px]">
                      <Lock className="h-2 w-2 mr-0.5" />
                      locked {lockoutsByIp.get(s.ip_address)?.remaining_s}s
                    </Badge>
                  )}
                  {!lockoutsByIp.has(s.ip_address) && failuresByIp.has(s.ip_address) && (
                    <Badge
                      variant="outline"
                      className="text-[10px] text-amber-700 dark:text-amber-400"
                    >
                      {failuresByIp.get(s.ip_address)?.count} fail
                    </Badge>
                  )}
                </div>
              </TableCell>
              <TableCell className="text-xs">{formatStamp(s.last_active_time)}</TableCell>
              <TableCell className="text-right">
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={() => handleBootSession(s.session_id)}
                  disabled={busy}
                >
                  Boot
                </Button>
              </TableCell>
            </TableRow>
          ))}
          {!sessions.length && (
            <TableRow>
              <TableCell colSpan={4} className="text-center text-xs text-muted-foreground">
                No active sessions.
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </section>
  )
}
