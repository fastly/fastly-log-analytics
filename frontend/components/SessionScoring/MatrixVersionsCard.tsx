'use client'

import React from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { ColumnDef } from '@tanstack/react-table'
import { History, Info, RotateCcw } from 'lucide-react'
import { AnalyticsCard, CardErrorState } from '@/components/AnalyticsCard'
import { DataTableReadonly } from '@/components/DataTable'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { SkeletonGrid } from '@/components/ui/skeleton'
import { client, unwrapClient } from '@/lib/api'
import { formatBytes, formatDeterministicUTC } from '@/lib/utils'
import type { components } from '@/types/api'
import { MatrixHistoryHelp } from './help-content'
import { invalidateServiceScoringQueries, useScoringQuery } from './ScoringAdminPanels'
import {
  DeployHintBox,
  RetrainButton,
  RotateKeyButton,
  AuditLogTab,
} from './ScoringOperationsAndAudit'

export { DeployHintBox, RetrainButton, RotateKeyButton, AuditLogTab }

type RestoreResponse = components['schemas']['ScoringMatrixRestoreResponse']
type MatrixVersion = components['schemas']['ScoringMatrixVersion']

export function MatrixVersionsCard({ serviceId }: { serviceId: string }) {
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
    mutationFn: (version: string): Promise<RestoreResponse> =>
      unwrapClient(
        client.POST('/api/services/{service_id}/scoring/matrix-versions/{version}/restore', {
          params: { path: { service_id: serviceId, version }, query: { confirm: true } },
        }),
      ),
    onSuccess: (res) => {
      setRestoreResult(res)
      setPendingVersion(null)
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
              <Badge variant="default" className="text-[10px]">
                current
              </Badge>
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
          <span className="text-xs text-muted-foreground">{formatDeterministicUTC(row.original.last_modified)}</span>
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
              <RotateCcw className="h-3 w-3 mr-1" /> Restore
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
            Restored matrix version <span className="font-mono">{restoreResult.restored_version}</span>
          </div>
          <DeployHintBox
            title="deploy hint"
            hint={restoreResult.deploy_hint ?? ''}
            ariaLabel="Copy deploy command"
            copiedAriaLabel="Copied deploy command"
            buttonTitle="Copy to clipboard"
            timeoutMs={1500}
          />
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
                Snapshot the current matrix.json to <span className="font-mono">pre-restore-{`{epoch_ms}`}</span>{' '}
                (reversible).
              </li>
              <li>Make the chosen version live for the admin AUC immediately.</li>
              <li>
                <strong>EDGE behavior:</strong> the Wasm at the edge keeps the embedded matrix until{' '}
                <span className="font-mono">deploy_wasm.sh</span> re-runs.
              </li>
            </ul>
            {restore.isError && <p className="text-destructive text-xs">{(restore.error as Error).message}</p>}
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
