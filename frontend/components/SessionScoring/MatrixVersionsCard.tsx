'use client'

import * as React from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Copy, History, Info, RotateCcw } from 'lucide-react'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { MatrixHistoryHelp } from '@/components/SessionScoring/help-content'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
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

interface MatrixVersionsCardProps {
  serviceId: string
}

interface MatrixVersion {
  version: string
  last_modified: string | null
  size_bytes: number
  key?: string
}

interface MatrixVersionsResponse {
  versions: MatrixVersion[]
  current_version: string | null
}

interface RestoreResponse {
  ok: boolean
  restored_version: string
  restored_at: string
  deploy_hint: string
}

function formatBytes(bytes: number): string {
  if (!bytes) return '0 B'
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`
}

function formatTimestamp(iso: string | null): string {
  if (!iso) return '—'
  try {
    return new Date(iso).toLocaleString()
  } catch {
    return iso
  }
}

export function MatrixVersionsCard({ serviceId }: MatrixVersionsCardProps) {
  const qc = useQueryClient()
  const [pendingVersion, setPendingVersion] = React.useState<string | null>(null)
  const [restoreResult, setRestoreResult] = React.useState<RestoreResponse | null>(null)
  const [copied, setCopied] = React.useState(false)

  const { data, isLoading, isFetching, isError, error } = useQuery({
    queryKey: ['scoring-matrix-versions', serviceId],
    queryFn: async () => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/matrix-versions' as any,
        {
          params: { path: { service_id: serviceId } },
        } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as MatrixVersionsResponse
    },
  })

  const restore = useMutation({
    mutationFn: async (version: string): Promise<RestoreResponse> => {
      const { data, response } = await client.POST(
        '/api/services/{service_id}/scoring/matrix-versions/{version}/restore' as any,
        {
          params: {
            path: { service_id: serviceId, version },
            query: { confirm: true },
          },
        } as any,
      )
      if (!response.ok) {
        const msg = (data as any)?.detail?.error ?? `status ${response.status}`
        throw new Error(msg)
      }
      return data as RestoreResponse
    },
    onSuccess: (res) => {
      setRestoreResult(res)
      setPendingVersion(null)
      // Refresh AUC / status / versions list — every card keyed on
      // 'scoring-*' for this service should refetch.
      qc.invalidateQueries({
        predicate: (q) =>
          Array.isArray(q.queryKey) &&
          typeof q.queryKey[0] === 'string' &&
          (q.queryKey[0] as string).startsWith('scoring-') &&
          q.queryKey[1] === serviceId,
      })
    },
  })

  const onCopyHint = () => {
    if (!restoreResult?.deploy_hint) return
    navigator.clipboard.writeText(restoreResult.deploy_hint)
    setCopied(true)
    window.setTimeout(() => setCopied(false), 1500)
  }

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
  const currentVersion = data?.current_version ?? null

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
          {Array.from({ length: 3 }).map((_, i) => (
            <Skeleton key={i} className="h-10 w-full" />
          ))}
        </div>
      ) : versions.length === 0 ? (
        <div className="p-6 text-center text-sm text-muted-foreground">
          No prior matrix versions saved yet. Versions accumulate as you retrain.
        </div>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Version</TableHead>
              <TableHead>Last modified</TableHead>
              <TableHead>Size</TableHead>
              <TableHead className="text-right">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {versions.map((v) => {
              const isCurrent = currentVersion === v.version
              return (
                <TableRow key={v.version}>
                  <TableCell className="font-mono text-xs">
                    <div className="flex items-center gap-2">
                      <span>{v.version}</span>
                      {isCurrent && (
                        <Badge variant="default" className="text-[10px]">current</Badge>
                      )}
                    </div>
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {formatTimestamp(v.last_modified)}
                  </TableCell>
                  <TableCell className="text-xs font-mono tabular-nums">
                    {formatBytes(v.size_bytes)}
                  </TableCell>
                  <TableCell className="text-right">
                    <Button
                      variant="outline"
                      size="sm"
                      className="h-7 text-xs text-destructive hover:bg-destructive/10 hover:text-destructive border-destructive/30"
                      disabled={isCurrent || restore.isPending}
                      onClick={() => setPendingVersion(v.version)}
                    >
                      <RotateCcw className="h-3 w-3 mr-1" />
                      Restore
                    </Button>
                  </TableCell>
                </TableRow>
              )
            })}
          </TableBody>
        </Table>
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
              <Button
                variant="ghost"
                size="icon"
                className="h-6 w-6 hover:bg-muted-foreground/10"
                onClick={onCopyHint}
                title={copied ? 'Copied!' : 'Copy to clipboard'}
              >
                <Copy className="h-3 w-3" />
              </Button>
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
