'use client'

import * as React from 'react'
import { useQuery } from '@tanstack/react-query'
import { ScrollText, Info } from 'lucide-react'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { AuditLogHelp } from '@/components/SessionScoring/help-content'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { client } from '@/lib/api'
import { formatTimeAgo } from '@/lib/date'

interface AuditLogTabProps {
  serviceId: string
}

interface AuditRow {
  id: number | string
  timestamp: string
  service_id: string
  action: string
  actor: string | null
  details: unknown
}

interface AuditResponse {
  audit: AuditRow[]
  limit?: number
}

const AUDIT_LIMIT = 200

function detailsToString(details: unknown): string {
  if (details == null) return ''
  if (typeof details === 'string') return details
  try {
    return JSON.stringify(details)
  } catch {
    return String(details)
  }
}

function truncate(s: string, max: number) {
  if (s.length <= max) return s
  return s.slice(0, max) + '…'
}

export function AuditLogTab({ serviceId }: AuditLogTabProps) {
  const query = useQuery<AuditResponse>({
    queryKey: ['scoring-audit', serviceId, AUDIT_LIMIT],
    queryFn: async () => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/audit' as any,
        {
          params: {
            path: { service_id: serviceId },
            query: { limit: AUDIT_LIMIT },
          },
        } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as AuditResponse
    },
    staleTime: 30_000,
  })

  if (query.isError) {
    return (
      <AnalyticsCard
        title="Audit log"
        icon={<ScrollText className="h-4 w-4" />}
        helpContent={<AuditLogHelp />}
        helpTitle="About Scoring Audit Log"
      >
        <div className="flex flex-col items-start gap-3">
          <div className="flex items-center gap-2 text-destructive">
            <Info className="h-4 w-4" />
            <span className="text-sm font-medium">Failed to load audit log</span>
          </div>
          <p className="text-xs text-muted-foreground">
            {(query.error as any)?.message || 'Unknown error'}
          </p>
          <Button size="sm" variant="outline" onClick={() => query.refetch()}>
            Retry
          </Button>
        </div>
      </AnalyticsCard>
    )
  }

  const rows = query.data?.audit ?? []

  return (
    <AnalyticsCard
      title="Audit log"
      icon={<ScrollText className="h-4 w-4" />}
      description="Recent operator actions on this service's scoring config (most-recent first)."
      helpContent={<AuditLogHelp />}
      helpTitle="About Scoring Audit Log"
      isLoading={query.isLoading}
      isFetching={query.isFetching}
      contentClassName="p-0"
    >
      {query.isLoading ? (
        <div className="p-4 space-y-2">
          {Array.from({ length: 3 }).map((_, i) => (
            <Skeleton key={i} className="h-10 w-full" />
          ))}
        </div>
      ) : (
        <div className="max-h-[520px] overflow-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-[140px]">Timestamp</TableHead>
                <TableHead className="w-[200px]">Action</TableHead>
                <TableHead className="w-[160px]">Actor</TableHead>
                <TableHead>Details</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.length === 0 && (
                <TableRow>
                  <TableCell
                    colSpan={4}
                    className="text-center text-xs text-muted-foreground py-6"
                  >
                    No audit events yet. Mutations to scoring (enable, retrain,
                    rotate key, etc.) will appear here.
                  </TableCell>
                </TableRow>
              )}
              {rows.map((r) => {
                const details = detailsToString(r.details)
                return (
                  <TableRow key={String(r.id)}>
                    <TableCell
                      className="text-xs whitespace-nowrap text-muted-foreground"
                      title={r.timestamp}
                    >
                      {formatTimeAgo(r.timestamp) || r.timestamp}
                    </TableCell>
                    <TableCell>
                      <Badge variant="secondary" className="font-mono text-[10px]">
                        {r.action}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-xs font-mono">
                      {r.actor || '—'}
                    </TableCell>
                    <TableCell
                      className="text-xs font-mono max-w-[480px] truncate"
                      title={details || undefined}
                    >
                      {details ? truncate(details, 60) : '—'}
                    </TableCell>
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>
        </div>
      )}
    </AnalyticsCard>
  )
}
