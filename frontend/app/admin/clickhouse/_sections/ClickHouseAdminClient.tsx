'use client'

import React, { Suspense, useState } from 'react'
import { notFound, useSearchParams } from 'next/navigation'
import { parseAsString, useQueryState } from 'nuqs'
import { keepPreviousData, useMutation, useQuery } from '@tanstack/react-query'
import {
  AlertTriangle,
  CheckCircle2,
  Database,
  Info,
  RefreshCw,
  ShieldAlert,
} from 'lucide-react'

import { client, queryKeys, unwrapClient } from '@/lib/api'
import { useServiceStore } from '@/stores/filterStore'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { ClickHouseReplayControlPanel } from './ClickHouseReplayControlPanel'
import {
  ClickHouseActiveGenerationCard,
  ClickHouseStatusNotice,
  ClickHouseVitalsCard,
  formatAgeSeconds,
  formatTimestamp,
  type ClickHouseReplayRequest,
  type ClickHouseReplayResponse,
  type ClickHouseStatusResponse,
} from './ClickHouseVitalsAndGeneration'

export {
  ClickHouseReplayControlPanel,
  ClickHouseActiveGenerationCard,
  ClickHouseStatusNotice,
  ClickHouseVitalsCard,
  formatAgeSeconds,
  formatTimestamp,
  type ClickHouseReplayRequest,
  type ClickHouseReplayResponse,
  type ClickHouseStatusResponse,
}

export interface ClickHouseAdminClientProps {
  initialServiceId?: string
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

export function ClickHouseAdminClient({ initialServiceId }: ClickHouseAdminClientProps) {
  const searchParams = useSearchParams()
  const [nuqsService] = useQueryState('service', parseAsString)
  const urlServiceId =
    nuqsService || searchParams?.get('service') || searchParams?.get('service_id') || undefined
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
      <div
        data-testid="admin-clickhouse-toolbar"
        className="flex flex-wrap items-center justify-between gap-3 rounded-lg border bg-card px-4 py-3 min-h-[56px] [contain-intrinsic-size:auto_56px] [content-visibility:auto]"
      >
        <div className="flex flex-wrap items-center gap-3">
          <div className="flex items-center gap-2">
            <Database className="h-4 w-4 text-muted-foreground" />
            <span className="text-sm font-medium">Service:</span>
            <Badge
              variant="outline"
              data-testid="admin-clickhouse-service-badge"
              className="font-mono text-xs"
            >
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
            <RefreshCw
              className={`h-4 w-4 mr-1.5 ${statusQuery.isFetching ? 'animate-spin' : ''}`}
            />
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
          <span>
            {(statusQuery.error as Error)?.message || 'Failed to load ClickHouse status'}
          </span>
        </div>
      )}

      <ClickHouseVitalsCard status={status} isDimmed={isDimmed} />

      <ClickHouseActiveGenerationCard
        status={status}
        isEnabled={isEnabled}
        isDimmed={isDimmed}
        onPrefillFromActiveGen={handlePrefillFromActiveGen}
      />

      <ClickHouseReplayControlPanel
        replayMode={replayMode}
        onReplayModeChange={setReplayMode}
        referenceValue={referenceValue}
        onReferenceValueChange={setReferenceValue}
        limitValue={limitValue}
        onLimitValueChange={setLimitValue}
        dryRun={dryRun}
        onDryRunChange={setDryRun}
        isEnabled={isEnabled}
        isPending={replayMutation.isPending}
        replayError={replayError}
        replayResult={replayResult}
        onSubmit={handleReplaySubmit}
      />
    </div>
  )
}

export function ClickHouseAdminPage() {
  if (
    process.env.DEPLOYMENT_MODE !== 'high_scale' &&
    process.env.HIGH_SCALE_ENABLED !== 'true'
  ) {
    notFound()
  }

  return (
    <div data-testid="admin-clickhouse-root" className="space-y-6">
      <div className="min-h-[60px]">
        <h1 className="text-2xl font-bold tracking-tight">ClickHouse Publication &amp; Replay</h1>
        <p className="text-sm text-muted-foreground mt-1">
          High-scale ClickHouse cluster readiness, active serving generation coverage, and bounded
          artifact replay controls.
        </p>
      </div>
      <Suspense
        fallback={
          <div className="space-y-6">
            <div className="h-[56px] rounded-lg border bg-card animate-pulse" />
            <div className="h-[220px] rounded-lg border bg-card animate-pulse" />
            <div className="h-[200px] rounded-lg border bg-card animate-pulse" />
            <div className="h-[300px] rounded-lg border bg-card animate-pulse" />
          </div>
        }
      >
        <ClickHouseAdminClient />
      </Suspense>
    </div>
  )
}

export default ClickHouseAdminPage
