'use client'

import React from 'react'
import {
  Activity,
  AlertTriangle,
  Clock,
  Info,
  Layers,
} from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { formatElapsed } from '@/lib/utils'
import type { components } from '@/types/api.generated'

export type ClickHouseStatusResponse = components['schemas']['ClickHouseStatusResponse']
export type ClickHouseReplayRequest = components['schemas']['ClickHouseReplayRequest']
export type ClickHouseReplayResponse = components['schemas']['ClickHouseReplayResponse']

export function formatAgeSeconds(seconds: number | null | undefined): string {
  return formatElapsed(seconds, {
    fractionDigits: { seconds: 1, minutes: 1, hours: 1, days: 1 },
    fallback: '—',
  })
}

export function formatTimestamp(iso: string | null | undefined): string {
  if (!iso) return '—'
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  return d.toISOString().replace('T', ' ').replace(/\.\d+Z$/, ' UTC')
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
          <p className="font-semibold">
            ClickHouse Cluster or Manifest Schema Unavailable (HTTP 503)
          </p>
          <p className="mt-0.5">
            The ClickHouse health check failed or the recorded <code>clickhouse_schema_state</code>{' '}
            version does not match the expected runtime schema version. Inspect cluster
            connectivity and schema initialization before executing replays.
          </p>
        </div>
      </div>
    </div>
  )
}

export function ClickHouseVitalsCard({
  status,
  isDimmed,
}: {
  status: ClickHouseStatusResponse | undefined
  isDimmed: boolean
}) {
  const counts = status?.publication_counts ?? null

  return (
    <Card
      data-testid="admin-clickhouse-vitals-reservation"
      className={`min-h-[300px] transition-opacity ${
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
            <div
              className="mt-1 text-lg font-semibold"
              data-testid="admin-clickhouse-vital-enabled"
            >
              {status ? (status.enabled ? 'High-Scale (Enabled)' : 'Disabled') : '—'}
            </div>
            <div className="text-xs text-muted-foreground mt-0.5">
              Schema Version: <span className="font-mono">{status?.schema_version ?? '—'}</span>
            </div>
          </div>

          <div className="rounded-md border bg-muted/20 p-3">
            <div className="text-xs text-muted-foreground">Published / Pending</div>
            <div
              className="mt-1 text-lg font-semibold font-mono"
              data-testid="admin-clickhouse-vital-counts"
            >
              {counts ? `${counts.published ?? 0} / ${counts.pending ?? 0}` : '—'}
            </div>
            <div className="text-xs text-muted-foreground mt-0.5">
              Claimed: <span className="font-mono">{counts?.claimed ?? '—'}</span> · Failed:{' '}
              <span className="font-mono">{counts?.failed ?? '—'}</span>
            </div>
          </div>

          <div className="rounded-md border bg-muted/20 p-3">
            <div className="text-xs text-muted-foreground">Oldest Pending Age</div>
            <div
              className="mt-1 text-lg font-semibold font-mono"
              data-testid="admin-clickhouse-vital-pending-age"
            >
              {formatAgeSeconds(status?.oldest_pending_age_seconds)}
            </div>
            <div
              className="text-xs text-muted-foreground mt-0.5 truncate"
              title={status?.last_published_at ?? ''}
            >
              Last Published: {formatTimestamp(status?.last_published_at)}
            </div>
          </div>

          <div className="rounded-md border bg-muted/20 p-3">
            <div className="text-xs text-muted-foreground">Expired Datasets</div>
            <div
              className="mt-1 text-lg font-semibold font-mono"
              data-testid="admin-clickhouse-vital-expired-count"
            >
              {status?.expired_dataset_count != null ? status.expired_dataset_count : '—'}
            </div>
            <div className="text-xs text-muted-foreground mt-0.5">Bounded retention window</div>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

export function ClickHouseActiveGenerationCard({
  status,
  isEnabled,
  isDimmed,
  onPrefillFromActiveGen,
}: {
  status: ClickHouseStatusResponse | undefined
  isEnabled: boolean
  isDimmed: boolean
  onPrefillFromActiveGen: () => void
}) {
  const activeGen = status?.active_generation ?? null

  return (
    <Card
      data-testid="admin-clickhouse-generation-reservation"
      className={`min-h-[212px] transition-opacity ${
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
            Currently activated ClickHouse publication generation and dataset coverage window for
            this service.
          </CardDescription>
        </div>
        {activeGen && (
          <Button
            type="button"
            variant="outline"
            size="sm"
            data-testid="admin-clickhouse-prefill-btn"
            onClick={onPrefillFromActiveGen}
          >
            Use in Replay
          </Button>
        )}
      </CardHeader>
      <CardContent>
        {activeGen ? (
          <div
            className="grid grid-cols-2 md:grid-cols-4 gap-3 min-h-[92px]"
            data-testid="admin-clickhouse-active-generation"
          >
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
                <Badge
                  variant={activeGen.target_matches ? 'default' : 'destructive'}
                  className="text-[10px] px-1.5 py-0"
                >
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
                <Badge
                  variant={activeGen.expired ? 'destructive' : 'secondary'}
                  className="text-[10px] px-1.5 py-0"
                >
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
              <div
                className="mt-1 text-sm font-mono font-semibold"
                data-testid="admin-clickhouse-coverage-age"
              >
                {formatAgeSeconds(activeGen.coverage_age_seconds)}
              </div>
              <div
                className="text-xs text-muted-foreground mt-0.5 truncate"
                title={activeGen.expires_at}
              >
                Expires: {formatTimestamp(activeGen.expires_at)}
              </div>
            </div>
          </div>
        ) : (
          <div
            data-testid="admin-clickhouse-no-active-generation"
            className="rounded-md border border-dashed bg-muted/10 p-6 min-h-[92px] flex items-center justify-center text-center text-sm text-muted-foreground"
          >
            {isEnabled
              ? 'No active publication generation is currently registered for this service.'
              : 'Active generation metadata is not applicable while ClickHouse is disabled.'}
          </div>
        )}
      </CardContent>
    </Card>
  )
}
