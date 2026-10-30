'use client'

import React from 'react'
import { AlertTriangle, CheckCircle2, Play, RotateCcw } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import type { ClickHouseReplayResponse } from './ClickHouseVitalsAndGeneration'

export function ClickHouseReplayControlPanel({
  replayMode,
  onReplayModeChange,
  referenceValue,
  onReferenceValueChange,
  limitValue,
  onLimitValueChange,
  dryRun,
  onDryRunChange,
  isEnabled,
  isPending,
  replayError,
  replayResult,
  onSubmit,
}: {
  replayMode: 'rebuild' | 'resume'
  onReplayModeChange: (mode: 'rebuild' | 'resume') => void
  referenceValue: string
  onReferenceValueChange: (value: string) => void
  limitValue: number
  onLimitValueChange: (value: number) => void
  dryRun: boolean
  onDryRunChange: (value: boolean) => void
  isEnabled: boolean
  isPending: boolean
  replayError: string | null
  replayResult: ClickHouseReplayResponse | null
  onSubmit: (e: React.FormEvent) => void
}) {
  return (
    <Card
      data-testid="admin-clickhouse-replay-reservation"
      className="min-h-[320px]"
    >
      <CardHeader className="pb-3">
        <CardTitle className="text-base flex items-center gap-2">
          <RotateCcw className="h-4 w-4 text-primary" />
          Bounded Artifact Replay Control Panel
        </CardTitle>
        <CardDescription>
          Preview (dry-run) or execute a bounded replay of sealed FOS artifacts into ClickHouse
          (1..100 artifacts per HTTP request). Specify either a <code>dataset_id</code> for a full
          rebuild or a <code>generation</code> to resume unpublished batches.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <form
          onSubmit={onSubmit}
          className="space-y-4"
          data-testid="admin-clickhouse-replay-form"
        >
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="space-y-2">
              <Label className="text-xs font-medium">Replay Operation Mode</Label>
              <div className="flex gap-2">
                <Button
                  type="button"
                  size="sm"
                  variant={replayMode === 'rebuild' ? 'default' : 'outline'}
                  data-testid="admin-clickhouse-mode-rebuild"
                  onClick={() => onReplayModeChange('rebuild')}
                  className="flex-1"
                >
                  Dataset Rebuild
                </Button>
                <Button
                  type="button"
                  size="sm"
                  variant={replayMode === 'resume' ? 'default' : 'outline'}
                  data-testid="admin-clickhouse-mode-resume"
                  onClick={() => onReplayModeChange('resume')}
                  className="flex-1"
                >
                  Generation Resume
                </Button>
              </div>
            </div>

            <div className="space-y-2">
              <Label htmlFor="clickhouse-replay-ref" className="text-xs font-medium">
                {replayMode === 'rebuild' ? 'Dataset ID (dataset_id)' : 'Generation ID (generation)'}
              </Label>
              <Input
                id="clickhouse-replay-ref"
                data-testid="admin-clickhouse-reference-input"
                placeholder={
                  replayMode === 'rebuild' ? 'e.g. ds_20261020_00' : 'e.g. gen_20261020_01'
                }
                value={referenceValue}
                onChange={(e) => onReferenceValueChange(e.target.value)}
                maxLength={128}
                disabled={!isEnabled || isPending}
                className="font-mono text-xs"
              />
            </div>

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
                onChange={(e) => onLimitValueChange(Number(e.target.value))}
                disabled={!isEnabled || isPending}
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
                onChange={(e) => onDryRunChange(e.target.checked)}
                disabled={!isEnabled || isPending}
                className="h-4 w-4 rounded border-input"
              />
              <span>
                Dry Run Preview (<code>dry_run: {dryRun ? 'true' : 'false'}</code>) — validate
                manifest references without mutating ClickHouse or reading FOS
              </span>
            </label>

            <Button
              type="submit"
              size="sm"
              data-testid="admin-clickhouse-replay-submit-btn"
              disabled={!isEnabled || !referenceValue.trim() || isPending}
            >
              <Play className="h-4 w-4 mr-1.5" />
              {isPending
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
                <span className="text-muted-foreground">Generation:</span>{' '}
                {replayResult.generation ?? '—'}
              </div>
              <div>
                <span className="text-muted-foreground">Planned / Total Artifacts:</span>{' '}
                {replayResult.planned_artifacts} / {replayResult.artifact_count}
              </div>
              <div>
                <span className="text-muted-foreground">Attempted / Published:</span>{' '}
                {replayResult.attempted} / {replayResult.published} (Activated:{' '}
                {replayResult.activated ? 'Yes' : 'No'})
              </div>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  )
}
