'use client'

import * as React from 'react'
import { Check, Loader2, ShieldCheck, ShieldOff } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { Slider } from '@/components/ui/slider'

interface StatusCodeMeta {
  current: number | null
  default: number
  effective: number
  min: number
  max: number
  is_default: boolean
}

interface ThresholdSliderControlsProps {
  thresholdRaw: number
  onThresholdRawChange: (value: number) => void
  committed: { threshold: number | null; set_at: string | null; enforced: boolean } | undefined
  enforce: { threshold: number | null; enforced: boolean } | undefined
  statusCode: StatusCodeMeta | undefined
  effectiveStatusCode: number
  isAlreadyCommitted: boolean
  isEnforcingThis: boolean
  commitPending: boolean
  enforcePending: boolean
  statusCodePending: boolean
  codeDraft: string
  onCodeDraftChange: (value: string) => void
  codeDraftValid: boolean
  codeDraftIsDirty: boolean
  codeDraftNum: number
  onCommitClick: () => void
  onEnforceClick: () => void
  onApplyStatusCode: () => void
  onResetStatusCode: () => void
}

/**
 * Threshold slider + the action buttons (commit / enforce / disable) +
 * the inline status-code editor. Pure presentational — parent owns all state
 * and supplies the click handlers.
 */
export function ThresholdSliderControls({
  thresholdRaw,
  onThresholdRawChange,
  committed,
  enforce,
  statusCode,
  effectiveStatusCode,
  isAlreadyCommitted,
  isEnforcingThis,
  commitPending,
  enforcePending,
  statusCodePending,
  codeDraft,
  onCodeDraftChange,
  codeDraftValid,
  codeDraftIsDirty,
  codeDraftNum,
  onCommitClick,
  onEnforceClick,
  onApplyStatusCode,
  onResetStatusCode,
}: ThresholdSliderControlsProps) {
  return (
    <div className="space-y-2">
      <div className="flex items-baseline justify-between">
        <label className="text-xs font-medium text-muted-foreground">
          Score threshold
          {committed?.threshold != null && (
            <span className="ml-2 text-[10px] text-muted-foreground">
              · committed: <span className="font-mono">{committed.threshold}</span>
            </span>
          )}
        </label>
        <div className="flex items-center gap-2">
          <span className="font-mono text-lg font-semibold tabular-nums">
            {thresholdRaw}
          </span>
          <Button
            variant={isAlreadyCommitted ? 'outline' : 'default'}
            size="sm"
            disabled={commitPending || isAlreadyCommitted}
            onClick={onCommitClick}
            className="h-7 text-xs"
            title="Persist this as your committed threshold (preview only — does NOT push to Compute)"
          >
            {commitPending ? (
              <Loader2 className="h-3 w-3 animate-spin mr-1" />
            ) : isAlreadyCommitted ? (
              <Check className="h-3 w-3 mr-1" />
            ) : null}
            {isAlreadyCommitted ? 'Committed' : 'Commit'}
          </Button>
          <Button
            variant={isEnforcingThis ? 'outline' : 'destructive'}
            size="sm"
            disabled={enforcePending}
            onClick={onEnforceClick}
            className="h-7 text-xs"
            title={
              isEnforcingThis
                ? 'Currently ENFORCING this threshold. Click to disable enforcement.'
                : `Push this threshold to Compute. Live requests with score >= threshold will be blocked (HTTP ${effectiveStatusCode}).`
            }
          >
            {enforcePending ? (
              <Loader2 className="h-3 w-3 animate-spin mr-1" />
            ) : isEnforcingThis ? (
              <ShieldOff className="h-3 w-3 mr-1" />
            ) : (
              <ShieldCheck className="h-3 w-3 mr-1" />
            )}
            {isEnforcingThis ? 'Disable' : 'Enforce'}
          </Button>
        </div>
      </div>
      {enforce?.enforced && (
        <div className="text-[10px] text-destructive">
          ⚠ LIVE: enforcing at threshold{' '}
          <span className="font-mono">{enforce.threshold}</span> — requests with score
          ≥ threshold are returning HTTP{' '}
          <span className="font-mono">{effectiveStatusCode}</span>.
        </div>
      )}
      <div className="flex items-center gap-2 flex-wrap text-[11px]">
        <label className="text-muted-foreground" htmlFor="enforce-status-code">
          Enforce response code:
        </label>
        <input
          id="enforce-status-code"
          type="number"
          min={statusCode?.min ?? 400}
          max={statusCode?.max ?? 599}
          step={1}
          value={codeDraft}
          onChange={(e) => onCodeDraftChange(e.target.value)}
          disabled={statusCodePending}
          className="h-6 w-16 rounded border bg-background px-1.5 text-[11px] font-mono"
          title="Any HTTP 4xx/5xx code (e.g. 403 Forbidden, 429 Too Many Requests, 451 Legal, 503 Service Unavailable). Reason phrase auto-mapped from the HTTP standard."
          aria-label="Enforce response code"
        />
        <Button
          size="sm"
          variant={codeDraftIsDirty ? 'default' : 'outline'}
          className="h-6 text-[11px]"
          disabled={!codeDraftIsDirty || statusCodePending}
          onClick={onApplyStatusCode}
          title={
            codeDraftIsDirty
              ? `Re-deploy the enforce snippet so flagged requests return HTTP ${codeDraftNum}`
              : 'No change to publish'
          }
        >
          Apply
        </Button>
        {statusCode && !statusCode.is_default && (
          <button
            type="button"
            disabled={statusCodePending}
            onClick={onResetStatusCode}
            className="text-[10px] text-muted-foreground underline hover:text-foreground"
            title={`Reset to default (${statusCode.default})`}
          >
            reset
          </button>
        )}
        {statusCodePending && (
          <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />
        )}
        {codeDraft !== '' && !codeDraftValid && (
          <span className="text-[10px] text-destructive">
            must be {statusCode?.min ?? 400}–{statusCode?.max ?? 599}
          </span>
        )}
      </div>
      <Slider
        value={[thresholdRaw]}
        onValueChange={(v) => onThresholdRawChange(v[0] ?? 75)}
        min={0}
        max={100}
        step={5}
        className="w-full"
        aria-label="Score threshold"
      />
      <div className="flex justify-between text-[10px] text-muted-foreground tabular-nums">
        <span>0 (flag everything)</span>
        <span>50</span>
        <span>100 (flag nothing)</span>
      </div>
    </div>
  )
}
