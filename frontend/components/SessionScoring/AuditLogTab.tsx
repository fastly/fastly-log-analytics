'use client'

import * as React from 'react'
import { ScrollText, Info } from 'lucide-react'
import { useScoringQuery } from './useScorerTimeseries'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { CardErrorState } from '@/components/CardErrorState'
import { AuditLogHelp } from '@/components/SessionScoring/help-content'
import { Badge } from '@/components/ui/badge'
import type { ColumnDef } from '@tanstack/react-table'
import { DataTableReadonly } from '@/components/DataTable'
import { client } from '@/lib/api'
import { formatTimeAgo } from '@/lib/date'
import type { components } from '@/types/api.generated'
import { SkeletonGrid } from '@/components/ui/skeleton'

interface AuditLogTabProps {
  serviceId: string
}

type AuditRow = components['schemas']['ScoringAuditRow']

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

const AUDIT_COLUMNS: ColumnDef<AuditRow>[] = [
  {
    id: 'timestamp',
    header: 'Timestamp',
    accessorFn: (r) => r.timestamp ?? '',
    size: 140,
    enableSorting: false,
    cell: ({ row }) => (
      <span
        className="text-xs whitespace-nowrap text-muted-foreground"
        title={row.original.timestamp ?? undefined}
      >
        {formatTimeAgo(row.original.timestamp ?? '') || row.original.timestamp}
      </span>
    ),
  },
  {
    id: 'action',
    header: 'Action',
    accessorFn: (r) => r.action,
    size: 200,
    enableSorting: false,
    cell: ({ row }) => (
      <Badge variant="secondary" className="font-mono text-[10px]">
        {row.original.action}
      </Badge>
    ),
  },
  {
    id: 'actor',
    header: 'Actor',
    accessorFn: (r) => r.actor ?? '',
    size: 160,
    enableSorting: false,
    cell: ({ row }) => <span className="text-xs font-mono">{row.original.actor || '—'}</span>,
  },
  {
    id: 'details',
    header: 'Details',
    accessorFn: (r) => detailsToString(r.details),
    size: 480,
    enableSorting: false,
    cell: ({ row }) => {
      const details = detailsToString(row.original.details)
      return (
        <span className="text-xs font-mono" title={details || undefined}>
          {details ? truncate(details, 60) : '—'}
        </span>
      )
    },
  },
]

export function AuditLogTab({ serviceId }: AuditLogTabProps) {
  const query = useScoringQuery(
    ['scoring-audit', serviceId, AUDIT_LIMIT],
    () =>
      client.GET('/api/services/{service_id}/scoring/audit', {
        params: {
          path: { service_id: serviceId },
          query: { limit: AUDIT_LIMIT },
        },
      }),
    { staleTime: 30_000 },
  )

  if (query.isError) {
    return (
      <AnalyticsCard
        title="Audit log"
        icon={<ScrollText className="h-4 w-4" />}
        helpContent={<AuditLogHelp />}
        helpTitle="About Scoring Audit Log"
      >
        <CardErrorState
          icon={<Info className="h-4 w-4" />}
          title="Failed to load audit log"
          message={query.error?.message || 'Unknown error'}
          onRetry={() => query.refetch()}
          variant="stacked"
        />
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
          <SkeletonGrid count={3} height="2.5rem" className="rounded-md" />
        </div>
      ) : (
        <DataTableReadonly
          columns={AUDIT_COLUMNS}
          data={rows}
          hideToolbar
          showPagination={rows.length > 50}
          tableCaption="Scoring audit log"
          emptyMessage="No audit events yet. Mutations to scoring (enable, retrain, rotate key, etc.) will appear here."
        />
      )}
    </AnalyticsCard>
  )
}
