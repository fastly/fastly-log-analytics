'use client'

import React, { useState } from 'react'
import { useSearchParams } from 'next/navigation'
import { keepPreviousData, useMutation, useQuery } from '@tanstack/react-query'
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  Clock,
  Database,
  Info,
  Layers,
  Play,
  RefreshCw,
  RotateCcw,
  ShieldAlert,
} from 'lucide-react'

import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api'
import { formatElapsed } from '@/lib/format'
import { queryKeys } from '@/lib/query-keys'
import { useServiceStore } from '@/stores/serviceStore'
import type { components } from '@/types/api.generated'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'

type ClickHouseStatusResponse = components['schemas']['ClickHouseStatusResponse']
type ClickHouseReplayRequest = components['schemas']['ClickHouseReplayRequest']
type ClickHouseReplayResponse = components['schemas']['ClickHouseReplayResponse']

interface ClickHouseAdminClientProps {
  initialServiceId?: string
}

export function ClickHouseStatusNotice({
  enabled,
  health,
}: {
  enabled?: boolean
  health?: string
}) {
  const disabled = enabled === false
  const unavailable = enabled === true && health === 'unavailable'

  return (
    <div className="grid">
      <div
        data-testid="admin-clickhouse-disabled-banner"
        aria-hidden={!disabled}
        className={`col-start-1 row-start-1 rounded-md border border-blue-500/30 bg-blue-500/10 px-4 py-3 text-xs text-blue-700 dark:text-blue-300 flex items-start gap-2.5 ${disabled ? '' : 'invisible'}`}
      >
        <Info className="h-4 w-4 shrink-0 mt-0.5" />
        <div>
          <p className="font-semibold">ClickHouse Disabled</p>
          <p className="mt-0.5 text-muted-foreground">
            ClickHouse cluster connection is not currently enabled (<code>enabled: false</code>,{' '}
            <code>health: &quot;disabled&quot;</code>).
          </p>
        </div>
      </div>
      <div
        data-testid="admin-clickhouse-unavailable-banner"
        aria-hidden={!unavailable}
        className={`col-start-1 row-start-1 rounded-md border border-destructive/40 bg-destructive/10 px-4 py-3 text-xs text-destructive flex items-start gap-2.5 ${unavailable ? '' : 'invisible'}`}
      >
        <AlertTriangle className="h-4 w-4 shrink-0 mt-0.5" />
        <div>
          <p className="font-semibold">ClickHouse Cluster or Manifest Schema Unavailable (HTTP 503)</p>
          <p className="mt-0.5">
            The ClickHouse health check failed or the recorded <code>clickhouse_schema_state</code> version does
            not match the expected runtime schema version. Inspect cluster connectivity and schema initialization
            before executing replays.
          </p>
        </div>
      </div>
    </div>
  )
}

async function fetchClickHouseStatus(
  serviceId: string,
  signal?: AbortSignal,
): Promise<ClickHouseStatusResponse> {
  return unwrapClient(
    client.GET('/api/admin/clickhouse/status', {
      params: { query: { service_id: serviceId } },
      signal,
    }),
    'Failed to load ClickHouse status',
  )
}

function formatAgeSeconds(seconds: number | null | undefined): string {
  return formatElapsed(seconds, { fractionDigits: { seconds: 1, minutes: 1, hours: 1, days: 1 }, fallback: '—' })
}

function formatTimestamp(iso: string | null | undefined): string {
  if (!iso) return '—'
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  return d.toISOString().replace('T', ' ').replace(/\.\d+Z$/, ' UTC')
}

export default function ClickHouseAdminClient({ initialServiceId }: ClickHouseAdminClientProps) {
  const searchParams = useSearchParams()
  const urlServiceId = searchParams?.get('service') || searchParams?.get('service_id') || undefined
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const services = useServiceStore((s) => s.services)

  const resolvedServiceId =
    urlServiceId || initialServiceId || activeServiceId || services[0]?.id || ''

  const [replayMode, setReplayMode] = useState<'rebuild' | 'resume'>('rebuild')
  const [referenceValue, setReferenceValue] = useState<string>('')
  const [limitValue, setLimitValue] = useState<number>(100)
  const [dryRun, setDryRun] = useState<boolean>(true)
  const [replayError, setReplayError] = useState<string | null>(null)
  const [replayResult, setReplayResult] = useState<ClickHouseReplayResponse | null>(null)

  const statusQuery = useQuery({
    queryKey: queryKeys.admin.clickhouseStatus(resolvedServiceId || null),
    queryFn: ({ signal }) => fetchClickHouseStatus(resolvedServiceId, signal),
    enabled: Boolean(resolvedServiceId),
    placeholderData: keepPreviousData,
    staleTime: 15_000,
    retry: false,
  })

  const replayMutation = useMutation({
    mutationFn: async (payload: ClickHouseReplayRequest): Promise<ClickHouseReplayResponse> =>
      unwrapClient(
        client.POST('/api/admin/clickhouse/replay', {
          body: payload,
        }),
        'Replay failed',
      ),
    onSuccess: (data) => {
      setReplayError(null)
      setReplayResult(data)
      if (!data.dry_run) {
        void statusQuery.refetch()
      }
    },
    onError: (err: Error) => {
      setReplayResult(null)
      setReplayError(err.message)
    },
  })

  const status = statusQuery.data
  const isEnabled = status?.enabled === true
  const health = status?.health ?? 'loading'
  const isDimmed = statusQuery.isFetching && statusQuery.isPlaceholderData
  const counts = status?.publication_counts ?? null
  const activeGen = status?.active_generation ?? null

  const handleManualRefresh = () => {
    void statusQuery.refetch()
  }

  const handleReplaySubmit = (e: React.FormEvent) => {
    e.preventDefault()
    setReplayError(null)
    if (!resolvedServiceId) {
      setReplayError('Select a service before submitting a replay request.')
      return
    }
    const trimmedRef = referenceValue.trim()
    if (!trimmedRef) {
      setReplayError(
        replayMode === 'rebuild'
          ? 'Enter a dataset_id to rebuild.'
          : 'Enter a generation identifier to resume.',
      )
      return
    }
    if (!Number.isInteger(limitValue) || limitValue < 1 || limitValue > 100) {
      setReplayError('HTTP replay limit must be an integer between 1 and 100.')
      return
    }

    const payload: ClickHouseReplayRequest = {
      service_id: resolvedServiceId,
      dataset_id: replayMode === 'rebuild' ? trimmedRef : null,
      generation: replayMode === 'resume' ? trimmedRef : null,
      dry_run: dryRun,
      limit: limitValue,
    }
    replayMutation.mutate(payload)
  }

  const handlePrefillFromActiveGen = () => {
    if (!activeGen) return
    if (replayMode === 'rebuild') {
      setReferenceValue(activeGen.dataset_id)
    } else {
      setReferenceValue(activeGen.generation)
    }
  }

  return (
    <div className="space-y-6">
      {/* Toolbar */}
      <div
        data-testid="admin-clickhouse-toolbar"
        className="flex flex-wrap items-center justify-between gap-3 rounded-lg border bg-card px-4 py-3 min-h-[56px] [contain-intrinsic-size:auto_56px] [content-visibility:auto]"
      >
        <div className="flex flex-wrap items-center gap-3">
          <div className="flex items-center gap-2">
            <Database className="h-4 w-4 text-muted-foreground" />
            <span className="text-sm font-medium">Service:</span>
            <Badge variant="outline" data-testid="admin-clickhouse-service-badge" className="font-mono text-xs">
              {resolvedServiceId || 'No service selected'}
            </Badge>
          </div>

          <div className="flex items-center gap-2">
            <span className="text-sm text-muted-foreground">Cluster Health:</span>
            {health === 'ok' && (
              <Badge
                data-testid="admin-clickhouse-health-badge"
                className="bg-emerald-500/15 text-emerald-600 dark:text-emerald-400 border-emerald-500/30 gap-1"
              >
                <CheckCircle2 className="h-3.5 w-3.5" />
                ok
              </Badge>
            )}
            {health === 'disabled' && (
              <Badge
                variant="secondary"
                data-testid="admin-clickhouse-health-badge"
                className="gap-1"
              >
                <Info className="h-3.5 w-3.5" />
                disabled
              </Badge>
            )}
            {health === 'unavailable' && (
              <Badge
                variant="destructive"
                data-testid="admin-clickhouse-health-badge"
                className="gap-1"
              >
                <AlertTriangle className="h-3.5 w-3.5" />
                unavailable
              </Badge>
            )}
            {health === 'loading' && (
              <Badge variant="outline" data-testid="admin-clickhouse-health-badge">
                checking...
              </Badge>
            )}
          </div>
        </div>

        <div className="flex items-center gap-2">
          <Button
            type="button"
            variant="outline"
            size="sm"
            data-testid="admin-clickhouse-refresh-btn"
            onClick={handleManualRefresh}
            disabled={statusQuery.isFetching || !resolvedServiceId}
          >
            <RefreshCw className={`h-4 w-4 mr-1.5 ${statusQuery.isFetching ? 'animate-spin' : ''}`} />
            Refresh
          </Button>
        </div>
      </div>

      {statusQuery.isError && (
        <div
          role="alert"
          data-testid="admin-clickhouse-query-error"
          className="rounded-lg border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive flex items-center gap-2"
        >
          <ShieldAlert className="h-4 w-4 shrink-0" />
          <span>{(statusQuery.error as Error)?.message || 'Failed to load ClickHouse status'}</span>
        </div>
      )}

      {/* Cluster & Publication Vitals */}
      <Card
        data-testid="admin-clickhouse-vitals-reservation"
        className={`min-h-[220px] [contain-intrinsic-size:auto_220px] [content-visibility:auto] transition-opacity ${
          isDimmed ? 'opacity-40 pointer-events-none' : ''
        }`}
      >
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Activity className="h-4 w-4 text-primary" />
            Cluster &amp; Publication Vitals
          </CardTitle>
          <CardDescription>
            ClickHouse serving schema readiness and PostgreSQL manifest publication counters.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <ClickHouseStatusNotice enabled={status?.enabled} health={status?.health} />

          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <div className="rounded-md border bg-muted/20 p-3">
              <div className="text-xs text-muted-foreground">Engine Mode</div>
              <div className="mt-1 text-lg font-semibold" data-testid="admin-clickhouse-vital-enabled">
                {status ? (status.enabled ? 'High-Scale (Enabled)' : 'Disabled') : '—'}
              </div>
              <div className="text-xs text-muted-foreground mt-0.5">
                Schema Version: <span className="font-mono">{status?.schema_version ?? '—'}</span>
              </div>
            </div>

            <div className="rounded-md border bg-muted/20 p-3">
              <div className="text-xs text-muted-foreground">Published / Pending</div>
              <div className="mt-1 text-lg font-semibold font-mono" data-testid="admin-clickhouse-vital-counts">
                {counts ? `${counts.published ?? 0} / ${counts.pending ?? 0}` : '—'}
              </div>
              <div className="text-xs text-muted-foreground mt-0.5">
                Claimed: <span className="font-mono">{counts?.claimed ?? '—'}</span> · Failed:{' '}
                <span className="font-mono">{counts?.failed ?? '—'}</span>
              </div>
            </div>

            <div className="rounded-md border bg-muted/20 p-3">
              <div className="text-xs text-muted-foreground">Oldest Pending Age</div>
              <div className="mt-1 text-lg font-semibold font-mono" data-testid="admin-clickhouse-vital-pending-age">
                {formatAgeSeconds(status?.oldest_pending_age_seconds)}
              </div>
              <div className="text-xs text-muted-foreground mt-0.5 truncate" title={status?.last_published_at ?? ''}>
                Last Published: {formatTimestamp(status?.last_published_at)}
              </div>
            </div>

            <div className="rounded-md border bg-muted/20 p-3">
              <div className="text-xs text-muted-foreground">Expired Datasets</div>
              <div className="mt-1 text-lg font-semibold font-mono" data-testid="admin-clickhouse-vital-expired-count">
                {status?.expired_dataset_count != null ? status.expired_dataset_count : '—'}
              </div>
              <div className="text-xs text-muted-foreground mt-0.5">Bounded retention window</div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Active Generation Card */}
      <Card
        data-testid="admin-clickhouse-generation-reservation"
        className={`min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto] transition-opacity ${
          isDimmed ? 'opacity-40 pointer-events-none' : ''
        }`}
      >
        <CardHeader className="pb-3 flex flex-row items-center justify-between">
          <div>
            <CardTitle className="text-base flex items-center gap-2">
              <Layers className="h-4 w-4 text-primary" />
              Active Generation
            </CardTitle>
            <CardDescription>
              Currently activated ClickHouse publication generation and dataset coverage window for this service.
            </CardDescription>
          </div>
          {activeGen && (
            <Button
              type="button"
              variant="outline"
              size="sm"
              data-testid="admin-clickhouse-prefill-btn"
              onClick={handlePrefillFromActiveGen}
            >
              Use in Replay
            </Button>
          )}
        </CardHeader>
        <CardContent>
          {activeGen ? (
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3" data-testid="admin-clickhouse-active-generation">
              <div className="rounded-md border bg-muted/20 p-3">
                <div className="text-xs text-muted-foreground">Generation ID</div>
                <div
                  className="mt-1 text-sm font-mono font-semibold truncate"
                  title={activeGen.generation}
                  data-testid="admin-clickhouse-gen-id"
                >
                  {activeGen.generation}
                </div>
                <div className="text-xs text-muted-foreground mt-1">
                  Target Matches:{' '}
                  <Badge variant={activeGen.target_matches ? 'default' : 'destructive'} className="text-[10px] px-1.5 py-0">
                    {activeGen.target_matches ? 'Yes' : 'No'}
                  </Badge>
                </div>
              </div>

              <div className="rounded-md border bg-muted/20 p-3">
                <div className="text-xs text-muted-foreground">Dataset ID</div>
                <div
                  className="mt-1 text-sm font-mono font-semibold truncate"
                  title={activeGen.dataset_id}
                  data-testid="admin-clickhouse-dataset-id"
                >
                  {activeGen.dataset_id}
                </div>
                <div className="text-xs text-muted-foreground mt-1">
                  Expired:{' '}
                  <Badge variant={activeGen.expired ? 'destructive' : 'secondary'} className="text-[10px] px-1.5 py-0">
                    {activeGen.expired ? 'Expired' : 'Active'}
                  </Badge>
                </div>
              </div>

              <div className="rounded-md border bg-muted/20 p-3">
                <div className="text-xs text-muted-foreground">Coverage Window</div>
                <div className="mt-1 text-xs font-mono" data-testid="admin-clickhouse-coverage-window">
                  <div>Start: {formatTimestamp(activeGen.coverage_start)}</div>
                  <div className="mt-0.5">End: {formatTimestamp(activeGen.coverage_end)}</div>
                </div>
              </div>

              <div className="rounded-md border bg-muted/20 p-3">
                <div className="text-xs text-muted-foreground flex items-center gap-1">
                  <Clock className="h-3 w-3" /> Coverage Age &amp; Expiry
                </div>
                <div className="mt-1 text-sm font-mono font-semibold" data-testid="admin-clickhouse-coverage-age">
                  {formatAgeSeconds(activeGen.coverage_age_seconds)}
                </div>
                <div className="text-xs text-muted-foreground mt-0.5 truncate" title={activeGen.expires_at}>
                  Expires: {formatTimestamp(activeGen.expires_at)}
                </div>
              </div>
            </div>
          ) : (
            <div
              data-testid="admin-clickhouse-no-active-generation"
              className="rounded-md border border-dashed bg-muted/10 p-6 text-center text-sm text-muted-foreground"
            >
              {isEnabled
                ? 'No active publication generation is currently registered for this service.'
                : 'Active generation metadata is not applicable while ClickHouse is disabled.'}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Bounded Replay Control Panel */}
      <Card
        data-testid="admin-clickhouse-replay-reservation"
        className="min-h-[300px] [contain-intrinsic-size:auto_300px] [content-visibility:auto]"
      >
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <RotateCcw className="h-4 w-4 text-primary" />
            Bounded Artifact Replay Control Panel
          </CardTitle>
          <CardDescription>
            Preview (dry-run) or execute a bounded replay of sealed FOS artifacts into ClickHouse (1..100 artifacts per
            HTTP request). Specify either a <code>dataset_id</code> for a full rebuild or a <code>generation</code> to
            resume unpublished batches.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <form onSubmit={handleReplaySubmit} className="space-y-4" data-testid="admin-clickhouse-replay-form">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              {/* Mode selector */}
              <div className="space-y-2">
                <Label className="text-xs font-medium">Replay Operation Mode</Label>
                <div className="flex gap-2">
                  <Button
                    type="button"
                    size="sm"
                    variant={replayMode === 'rebuild' ? 'default' : 'outline'}
                    data-testid="admin-clickhouse-mode-rebuild"
                    onClick={() => setReplayMode('rebuild')}
                    className="flex-1"
                  >
                    Dataset Rebuild
                  </Button>
                  <Button
                    type="button"
                    size="sm"
                    variant={replayMode === 'resume' ? 'default' : 'outline'}
                    data-testid="admin-clickhouse-mode-resume"
                    onClick={() => setReplayMode('resume')}
                    className="flex-1"
                  >
                    Generation Resume
                  </Button>
                </div>
              </div>

              {/* Reference identifier */}
              <div className="space-y-2">
                <Label htmlFor="clickhouse-replay-ref" className="text-xs font-medium">
                  {replayMode === 'rebuild' ? 'Dataset ID (dataset_id)' : 'Generation ID (generation)'}
                </Label>
                <Input
                  id="clickhouse-replay-ref"
                  data-testid="admin-clickhouse-reference-input"
                  placeholder={replayMode === 'rebuild' ? 'e.g. ds_20261020_00' : 'e.g. gen_20261020_01'}
                  value={referenceValue}
                  onChange={(e) => setReferenceValue(e.target.value)}
                  maxLength={128}
                  disabled={!isEnabled || replayMutation.isPending}
                  className="font-mono text-xs"
                />
              </div>

              {/* Batch limit (1..100) */}
              <div className="space-y-2">
                <Label htmlFor="clickhouse-replay-limit" className="text-xs font-medium">
                  Batch Limit (1..100)
                </Label>
                <Input
                  id="clickhouse-replay-limit"
                  type="number"
                  min={1}
                  max={100}
                  data-testid="admin-clickhouse-limit-input"
                  value={limitValue}
                  onChange={(e) => setLimitValue(Number(e.target.value))}
                  disabled={!isEnabled || replayMutation.isPending}
                  className="font-mono text-xs"
                />
              </div>
            </div>

            <div className="flex flex-wrap items-center justify-between gap-4 pt-1">
              <label className="flex items-center gap-2 text-sm cursor-pointer select-none">
                <input
                  type="checkbox"
                  data-testid="admin-clickhouse-dry-run-checkbox"
                  checked={dryRun}
                  onChange={(e) => setDryRun(e.target.checked)}
                  disabled={!isEnabled || replayMutation.isPending}
                  className="h-4 w-4 rounded border-input"
                />
                <span>
                  Dry Run Preview (<code>dry_run: {dryRun ? 'true' : 'false'}</code>) — validate manifest references
                  without mutating ClickHouse or reading FOS
                </span>
              </label>

              <Button
                type="submit"
                size="sm"
                data-testid="admin-clickhouse-replay-submit-btn"
                disabled={!isEnabled || !referenceValue.trim() || replayMutation.isPending}
              >
                <Play className="h-4 w-4 mr-1.5" />
                {replayMutation.isPending
                  ? 'Running...'
                  : dryRun
                    ? 'Preview Replay (Dry Run)'
                    : 'Execute Bounded Replay'}
              </Button>
            </div>

            {!isEnabled && (
              <p
                data-testid="admin-clickhouse-replay-disabled-hint"
                className="text-xs text-muted-foreground italic"
              >
                Replay controls are disabled because ClickHouse is not enabled on this deployment.
              </p>
            )}
          </form>

          {replayError && (
            <div
              role="alert"
              data-testid="admin-clickhouse-replay-error"
              className="rounded-md border border-destructive/40 bg-destructive/10 px-4 py-3 text-xs text-destructive flex items-center gap-2"
            >
              <AlertTriangle className="h-4 w-4 shrink-0" />
              <span>{replayError}</span>
            </div>
          )}

          {replayResult && (
            <div
              data-testid="admin-clickhouse-replay-outcome"
              className="rounded-md border bg-muted/20 p-4 space-y-2 text-xs"
            >
              <div className="flex items-center justify-between">
                <span className="font-semibold text-sm flex items-center gap-1.5">
                  <CheckCircle2 className="h-4 w-4 text-emerald-500" />
                  {replayResult.dry_run ? 'Dry-Run Replay Preview' : 'Replay Execution Completed'}
                </span>
                <Badge variant="outline" className="font-mono">
                  operation: {replayResult.operation}
                </Badge>
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3 pt-1 font-mono">
                <div>
                  <span className="text-muted-foreground">Dataset:</span> {replayResult.dataset_id}
                </div>
                <div>
                  <span className="text-muted-foreground">Generation:</span> {replayResult.generation ?? '—'}
                </div>
                <div>
                  <span className="text-muted-foreground">Planned / Total Artifacts:</span>{' '}
                  {replayResult.planned_artifacts} / {replayResult.artifact_count}
                </div>
                <div>
                  <span className="text-muted-foreground">Attempted / Published:</span> {replayResult.attempted} /{' '}
                  {replayResult.published} (Activated: {replayResult.activated ? 'Yes' : 'No'})
                </div>
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
