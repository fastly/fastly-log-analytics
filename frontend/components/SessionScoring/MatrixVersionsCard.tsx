'use client'

import * as React from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { History, Info, RotateCcw, ScrollText } from 'lucide-react'
import { invalidateServiceScoringQueries, useScoringQuery } from './useScorerTimeseries'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { CardErrorState } from '@/components/AnalyticsCard'
import { AuditLogHelp, MatrixHistoryHelp } from '@/components/SessionScoring/help-content'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { CopyButton } from '@/components/ui/copy-button'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import type { ColumnDef } from '@tanstack/react-table'
import { DataTableReadonly } from '@/components/DataTable'
import { client, extractApiError } from '@/lib/api'
import type { components } from '@/types/api.generated'

interface MatrixVersionsCardProps {
  serviceId: string
}

type RestoreResponse = components['schemas']['ScoringMatrixRestoreResponse']
type MatrixVersion = components['schemas']['ScoringMatrixVersion']
type AuditRow = components['schemas']['ScoringAuditRow']

import { formatBytes } from '@/lib/format'
import { formatDeterministicUTC, formatTimeAgo } from '@/lib/date'
import { SkeletonGrid } from '@/components/ui/skeleton'

export function MatrixVersionsCard({ serviceId }: MatrixVersionsCardProps) {
  const qc = useQueryClient()
  const [pendingVersion, setPendingVersion] = React.useState<string | null>(null)
  const [restoreResult, setRestoreResult] = React.useState<RestoreResponse | null>(null)

  const { data, isLoading, isFetching, isError, error } = useScoringQuery(
    ['scoring-matrix-versions', serviceId],
    () =>
      client.GET('/api/services/{service_id}/scoring/matrix-versions', {
        params: { path: { service_id: serviceId } },
      }),
  )

  const restore = useMutation({
    mutationFn: async (version: string): Promise<RestoreResponse> => {
      const { data, error: errBody, response } = await client.POST(
        '/api/services/{service_id}/scoring/matrix-versions/{version}/restore',
        {
          params: {
            path: { service_id: serviceId, version },
            query: { confirm: true },
          },
        },
      )
      if (!response.ok || !data) {
        throw new Error(errBody ? extractApiError(errBody) : `status ${response.status}`)
      }
      return data
    },
    onSuccess: (res) => {
      setRestoreResult(res)
      setPendingVersion(null)
      // Refresh AUC / status / versions list — every card keyed on
      // 'scoring-*' for this service should refetch.
      invalidateServiceScoringQueries(qc, serviceId)
    },
  })

  const currentVersion = data?.current_version ?? null
  const restorePending = restore.isPending

  const columns = React.useMemo<ColumnDef<MatrixVersion>[]>(
    () => [
      {
        id: 'version',
        header: 'Version',
        accessorFn: (v) => v.version ?? '',
        size: 260,
        enableSorting: false,
        cell: ({ row }) => (
          <div className="flex items-center gap-2 font-mono text-xs">
            <span>{row.original.version}</span>
            {currentVersion === row.original.version && (
              <Badge variant="default" className="text-[10px]">current</Badge>
            )}
          </div>
        ),
      },
      {
        id: 'last_modified',
        header: 'Last modified',
        accessorFn: (v) => v.last_modified ?? '',
        size: 200,
        enableSorting: false,
        cell: ({ row }) => (
          <span className="text-xs text-muted-foreground">
            {formatDeterministicUTC(row.original.last_modified)}
          </span>
        ),
      },
      {
        id: 'size_bytes',
        header: 'Size',
        accessorFn: (v) => v.size_bytes ?? 0,
        size: 110,
        enableSorting: false,
        cell: ({ row }) => (
          <span className="text-xs font-mono tabular-nums">{formatBytes(row.original.size_bytes ?? 0)}</span>
        ),
      },
      {
        id: 'actions',
        header: 'Actions',
        size: 130,
        enableSorting: false,
        cell: ({ row }) => (
          <div className="flex justify-end">
            <Button
              variant="outline"
              size="sm"
              className="h-7 text-xs text-destructive hover:bg-destructive/10 hover:text-destructive border-destructive/30"
              disabled={currentVersion === row.original.version || restorePending}
              onClick={() => setPendingVersion(row.original.version ?? null)}
            >
              <RotateCcw className="h-3 w-3 mr-1" />
              Restore
            </Button>
          </div>
        ),
      },
    ],
    [currentVersion, restorePending],
  )

  if (isError) {
    return (
      <CardErrorState
        icon={<Info className="h-4 w-4" />}
        title="Failed to load matrix versions"
        message={(error as Error)?.message || 'Unknown error'}
      />
    )
  }

  const versions = data?.versions ?? []

  return (
    <AnalyticsCard
      title="Matrix history"
      icon={<History className="h-4 w-4" />}
      description="Restore a prior trained matrix. Edge Wasm keeps the embedded matrix until you re-run deploy_wasm.sh."
      helpContent={<MatrixHistoryHelp />}
      helpTitle="About Matrix History"
      isLoading={isLoading}
      isFetching={isFetching}
      contentClassName="p-0"
    >
      {isLoading ? (
        <div className="p-4 space-y-2">
          <SkeletonGrid count={3} height="2.5rem" className="rounded-md" />
        </div>
      ) : versions.length === 0 ? (
        <div className="p-6 text-center text-sm text-muted-foreground">
          No prior matrix versions saved yet. Versions accumulate as you retrain.
        </div>
      ) : (
        <DataTableReadonly
          columns={columns}
          data={versions}
          hideToolbar
          showPagination={versions.length > 50}
          tableCaption="Matrix versions"
        />
      )}

      {restoreResult && (
        <div className="border-t p-4 space-y-2">
          <div className="text-xs font-semibold">
            Restored matrix version{' '}
            <span className="font-mono">{restoreResult.restored_version}</span>
          </div>
          <div className="rounded-md border bg-muted overflow-hidden">
            <div className="px-3 py-1.5 border-b bg-muted/50 flex items-center justify-between">
              <span className="text-[10px] font-mono text-muted-foreground">
                deploy hint
              </span>
              <CopyButton
                text={restoreResult.deploy_hint ?? ''}
                ariaLabel="Copy deploy command"
                copiedAriaLabel="Copied deploy command"
                title="Copy to clipboard"
                className="h-6 w-6 hover:bg-muted-foreground/10"
                timeoutMs={1500}
              />
            </div>
            <pre className="p-3 text-[11px] font-mono text-muted-foreground whitespace-pre-wrap leading-relaxed">
              {restoreResult.deploy_hint}
            </pre>
          </div>
        </div>
      )}

      <ConfirmDialog
        open={pendingVersion !== null}
        onOpenChange={(open) => {
          if (!open) setPendingVersion(null)
        }}
        title={`Restore matrix version ${pendingVersion ?? ''}?`}
        description={
          <div className="space-y-2 text-sm">
            <p>This will:</p>
            <ul className="list-disc list-inside space-y-1 text-xs">
              <li>
                Snapshot the current matrix.json to{' '}
                <span className="font-mono">pre-restore-{`{epoch_ms}`}</span> (reversible).
              </li>
              <li>
                Make the chosen version live for the admin AUC immediately.
              </li>
              <li>
                <strong>EDGE behavior:</strong> the Wasm at the edge keeps the
                embedded matrix until <span className="font-mono">deploy_wasm.sh</span>{' '}
                re-runs.
              </li>
            </ul>
            {restore.isError && (
              <p className="text-destructive text-xs">
                {(restore.error as Error).message}
              </p>
            )}
          </div>
        }
        confirmLabel="Restore version"
        cancelLabel="Cancel"
        isDangerous
        isPending={restore.isPending}
        onConfirm={() => {
          if (pendingVersion) restore.mutate(pendingVersion)
        }}
      />
    </AnalyticsCard>
  )
}

interface AuditLogTabProps {
  serviceId: string
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
