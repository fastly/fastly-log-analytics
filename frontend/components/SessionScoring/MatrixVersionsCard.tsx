'use client'

import * as React from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { History, Info, RotateCcw } from 'lucide-react'
import { invalidateServiceScoringQueries, useScoringQuery } from './useScoringQuery'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { MatrixHistoryHelp } from '@/components/SessionScoring/help-content'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { CopyButton } from '@/components/ui/copy-button'
import { Card, CardContent } from '@/components/ui/card'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import type { ColumnDef } from '@tanstack/react-table'
import { DataTableReadonly } from '@/components/DataTable/DataTableReadonly'
import { client, extractApiError } from '@/lib/api'
import type { components } from '@/types/api.generated'

interface MatrixVersionsCardProps {
  serviceId: string
}

type RestoreResponse = components['schemas']['ScoringMatrixRestoreResponse']
type MatrixVersion = components['schemas']['ScoringMatrixVersion']

import { formatBytes } from '@/lib/format'
import { formatDeterministicUTC } from '@/lib/date'
import { SkeletonGrid } from '@/components/ui/skeleton-grid'

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
      <Card className="border-destructive/20 bg-destructive/5">
        <CardContent className="pt-6">
          <div className="flex items-center gap-2 text-destructive">
            <Info className="h-4 w-4" />
            <span className="text-sm font-medium">Failed to load matrix versions</span>
          </div>
          <p className="text-xs text-muted-foreground mt-1">
            {(error as Error)?.message || 'Unknown error'}
          </p>
        </CardContent>
      </Card>
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
