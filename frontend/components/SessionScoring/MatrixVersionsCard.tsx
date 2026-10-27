'use client'

import React from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { ColumnDef } from '@tanstack/react-table'
import { Brain, History, Info, KeyRound, Loader2, RotateCcw, ScrollText } from 'lucide-react'
import { AnalyticsCard, CardErrorState } from '@/components/AnalyticsCard'
import { DataTableReadonly } from '@/components/DataTable'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { ConfirmDialog, CopyButton } from '@/components/ui/confirm-dialog'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { SkeletonGrid } from '@/components/ui/skeleton'
import { useAutoFlash, useOpenGeneration } from '@/hooks/useSSE'
import { client, unwrapClient } from '@/lib/api'
import { formatBytes, formatCount, formatDeterministicUTC, formatTimeAgo } from '@/lib/utils'
import type { components } from '@/types/api'
import { AuditLogHelp, MatrixHistoryHelp } from './help-content'
import { invalidateServiceScoringQueries, scoringConfigCompositeKey, useScoringQuery } from './ScoringAdminPanels'

type RetrainResult = components['schemas']['ScoringRetrainResponse']
type RotateKeyResponse = components['schemas']['ScoringRotateKeyResponse']
type RestoreResponse = components['schemas']['ScoringMatrixRestoreResponse']
type MatrixVersion = components['schemas']['ScoringMatrixVersion']
type AuditRow = components['schemas']['ScoringAuditRow']

export function DeployHintBox({
  title,
  hint,
  ariaLabel,
  copiedAriaLabel,
  buttonTitle,
  timeoutMs,
}: {
  title: string
  hint: string
  ariaLabel: string
  copiedAriaLabel: string
  buttonTitle?: string
  timeoutMs?: number
}) {
  return (
    <div className="rounded-md border bg-muted overflow-hidden">
      <div className="px-3 py-1.5 border-b bg-muted/50 flex items-center justify-between">
        <span className="text-[10px] font-mono text-muted-foreground">{title}</span>
        <CopyButton
          text={hint}
          ariaLabel={ariaLabel}
          copiedAriaLabel={copiedAriaLabel}
          title={buttonTitle}
          className="h-6 w-6 hover:bg-muted-foreground/10"
          timeoutMs={timeoutMs}
        />
      </div>
      <pre className="p-3 text-[11px] font-mono text-muted-foreground leading-relaxed whitespace-pre-wrap break-all">
        {hint}
      </pre>
    </div>
  )
}

export function RetrainButton({ serviceId }: { serviceId: string }) {
  const [open, setOpen] = React.useState(false)
  const generation = useOpenGeneration(open)
  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger
        render={
          <Button variant="outline" size="sm">
            <Brain className="h-4 w-4 mr-1" />
            Retrain matrix
          </Button>
        }
      />
      <RetrainDialogBody key={generation} serviceId={serviceId} onClose={() => setOpen(false)} />
    </Dialog>
  )
}

function RetrainDialogBody({ serviceId, onClose }: { serviceId: string; onClose: () => void }) {
  const queryClient = useQueryClient()
  const [sinceDays, setSinceDays] = React.useState(7)
  const [result, setResult] = React.useState<RetrainResult | null>(null)
  const mutation = useMutation({
    mutationFn: (): Promise<RetrainResult> =>
      unwrapClient(
        client.POST('/api/services/{service_id}/scoring/retrain', {
          params: { path: { service_id: serviceId }, query: { since_days: sinceDays } },
        }),
      ),
    onSuccess: (data) => {
      setResult(data)
      invalidateServiceScoringQueries(queryClient, serviceId)
    },
  })

  return (
    <DialogContent className="max-w-2xl">
      <DialogHeader>
        <DialogTitle className="flex items-center gap-2">
          <Brain className="h-4 w-4" />
          Retrain scoring matrix
        </DialogTitle>
        <DialogDescription>
          Builds a fresh transition matrix from the last N days of DuckDB traffic and evaluates AUC against your
          labels. Saves matrix.json + publishes to FOS so every backend host sees the new matrix. The Wasm rebuild +
          Compute deploy is a separate step (requires Fastly CLI on your local box).
        </DialogDescription>
      </DialogHeader>

      {!result && (
        <div className="space-y-4 py-2">
          <div className="space-y-1.5">
            <Label htmlFor="since-days" className="text-xs">
              Training window (days)
            </Label>
            <Input
              id="since-days"
              type="number"
              min={1}
              max={90}
              value={sinceDays}
              onChange={(e) => setSinceDays(Number(e.target.value) || 7)}
              className="text-sm"
              disabled={mutation.isPending}
            />
            <p className="text-[11px] text-muted-foreground">
              7 days is the default; bump to 30 once you have steady traffic. Larger windows take longer but build a
              more stable matrix.
            </p>
          </div>
          {mutation.isError && <p className="text-sm text-destructive">{(mutation.error as Error).message}</p>}
        </div>
      )}

      {result && (
        <div className="space-y-3 py-2 text-sm">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
            {[
              { label: 'Matrix version', value: result.matrix_version, mono: true },
              { label: 'Sessions trained', value: formatCount(result.sessions_trained_on ?? 0) },
              { label: 'Transitions', value: formatCount(result.transitions ?? 0) },
              { label: 'Vocab size', value: formatCount(result.vocab_size ?? 0) },
            ].map((m) => (
              <div key={m.label} className="p-2 border rounded-md">
                <div className="text-[10px] uppercase tracking-wider text-muted-foreground">{m.label}</div>
                <div className={`text-sm ${m.mono ? 'font-mono' : ''} font-semibold`}>{m.value}</div>
              </div>
            ))}
          </div>
          {result.rejected && (
            <p className="text-xs text-muted-foreground">
              Kept {formatCount(result.rejected.kept ?? 0)} sessions · dropped{' '}
              {formatCount(result.rejected.too_few_events ?? 0)} too-short,{' '}
              {formatCount(result.rejected.too_fast ?? 0)} too-fast ·{' '}
              {formatCount(result.rejected.routes_seen ?? 0)} routes seen
            </p>
          )}
          {result.auc_against_labels ? (
            <div className="p-3 border rounded-md bg-muted/40">
              <div className="text-xs uppercase text-muted-foreground mb-1">AUC against your labels</div>
              <div className="flex items-baseline gap-2">
                <span
                  className={`text-2xl font-mono font-semibold tabular-nums ${
                    result.auc_against_labels.passed ? 'text-emerald-600' : 'text-amber-600'
                  }`}
                >
                  {(result.auc_against_labels.auc ?? 0).toFixed(3)}
                </span>
                <span className="text-sm text-muted-foreground">
                  {result.auc_against_labels.passed ? 'PASS' : 'BELOW THRESHOLD'} (threshold{' '}
                  {(result.auc_against_labels.threshold ?? 0).toFixed(2)})
                </span>
              </div>
              <p className="text-[11px] text-muted-foreground mt-1">
                n={result.auc_against_labels.n_good} good / {result.auc_against_labels.n_bad} bad — minimum{' '}
                {result.default_min_auc} to pass.
              </p>
            </div>
          ) : (
            <p className="text-xs text-muted-foreground italic">
              AUC not computed — need at least 3 good + 3 bad labels to evaluate.
            </p>
          )}
          <div className="space-y-1 text-[11px] text-muted-foreground">
            <p>{result.local_matrix_saved ? '✓' : '✗'} matrix.json saved locally</p>
            <p>{result.fos_matrix_published ? '✓' : '✗'} matrix published to FOS</p>
          </div>
          {result.deploy_hint && (
            <details className="text-xs">
              <summary className="cursor-pointer text-muted-foreground hover:text-foreground mb-2">
                Deploy the new matrix to Fastly Compute
              </summary>
              <DeployHintBox
                title="deploy command"
                hint={result.deploy_hint}
                ariaLabel="Copy deploy hint"
                copiedAriaLabel="Copied deploy hint"
              />
            </details>
          )}
        </div>
      )}

      <DialogFooter>
        {!result ? (
          <>
            <Button variant="outline" onClick={onClose} disabled={mutation.isPending}>
              Cancel
            </Button>
            <Button onClick={() => mutation.mutate()} disabled={mutation.isPending}>
              {mutation.isPending ? (
                <>
                  <Loader2 className="h-4 w-4 mr-1 animate-spin" />
                  Training…
                </>
              ) : (
                <>
                  <Brain className="h-4 w-4 mr-1" />
                  Start retrain
                </>
              )}
            </Button>
          </>
        ) : (
          <Button onClick={onClose}>Close</Button>
        )}
      </DialogFooter>
    </DialogContent>
  )
}

export function RotateKeyButton({ serviceId }: { serviceId: string }) {
  const qc = useQueryClient()
  const [open, setOpen] = React.useState(false)
  const { value: status, flash: flashStatus, set: setStatus, clear: clearStatus } = useAutoFlash<{
    kind: 'success' | 'error'
    message: string
  }>(4000)

  const mutation = useMutation({
    mutationFn: (): Promise<RotateKeyResponse> =>
      unwrapClient(
        client.POST('/api/services/{service_id}/scoring/rotate-key', {
          params: { path: { service_id: serviceId } },
        }),
      ),
    onSuccess: () => {
      flashStatus({ kind: 'success', message: 'Key rotated; previous slot updated' })
      setOpen(false)
      qc.invalidateQueries({ queryKey: ['scoring-status', serviceId] })
      qc.invalidateQueries({ queryKey: scoringConfigCompositeKey(serviceId) })
      qc.invalidateQueries({ queryKey: ['scoring-audit', serviceId] })
    },
    onError: (err: unknown) => {
      setStatus({ kind: 'error', message: err instanceof Error ? err.message : 'Failed to rotate key' })
    },
  })

  return (
    <div className="inline-flex flex-col items-end gap-1">
      <Button
        variant="destructive"
        size="sm"
        onClick={() => {
          clearStatus()
          setOpen(true)
        }}
        disabled={mutation.isPending}
        title="Rotate the AES key used to encrypt scoring cookies"
      >
        {mutation.isPending ? (
          <Loader2 className="h-4 w-4 mr-1 animate-spin" />
        ) : (
          <KeyRound className="h-4 w-4 mr-1" />
        )}
        Rotate AES key
      </Button>
      {status && (
        <p
          className={`text-[11px] ${status.kind === 'success' ? 'text-emerald-600' : 'text-destructive'}`}
          role="status"
        >
          {status.message}
        </p>
      )}
      <ConfirmDialog
        open={open}
        onOpenChange={(o) => {
          if (!mutation.isPending) setOpen(o)
        }}
        isDangerous
        isPending={mutation.isPending}
        title="Rotate AES cookie key?"
        description={
          <div className="space-y-2 text-sm">
            <p>
              Generates a new AES key and promotes it to the <strong>current</strong> slot. The old current key moves
              to <strong>previous</strong> and is still accepted for one grace cycle.
            </p>
            <ul className="list-disc list-inside space-y-1 text-xs">
              <li>In-flight sessions keep working — their cookies decode under the previous-slot key.</li>
              <li>New sessions get cookies encrypted with the new current key.</li>
              <li>
                <strong>Warning:</strong> rotating twice in quick succession discards the original key entirely.
                Cookies issued under it will be treated as <span className="font-mono">tampered</span> at the edge.
              </li>
            </ul>
          </div>
        }
        confirmLabel="Rotate key"
        cancelLabel="Cancel"
        onConfirm={() => mutation.mutate()}
      />
    </div>
  )
}

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

const AUDIT_COLUMNS: ColumnDef<AuditRow>[] = [
  {
    id: 'timestamp',
    header: 'Timestamp',
    accessorFn: (r) => r.timestamp ?? '',
    size: 140,
    enableSorting: false,
    cell: ({ row }) => (
      <span className="text-xs whitespace-nowrap text-muted-foreground" title={row.original.timestamp ?? undefined}>
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
          {details ? (details.length <= 60 ? details : `${details.slice(0, 60)}…`) : '—'}
        </span>
      )
    },
  },
]

export function AuditLogTab({ serviceId }: { serviceId: string }) {
  const query = useScoringQuery(
    ['scoring-audit', serviceId, AUDIT_LIMIT],
    () =>
      client.GET('/api/services/{service_id}/scoring/audit', {
        params: { path: { service_id: serviceId }, query: { limit: AUDIT_LIMIT } },
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
