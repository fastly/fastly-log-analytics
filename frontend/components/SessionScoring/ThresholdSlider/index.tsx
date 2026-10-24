'use client'

import { useDebounce } from '@/hooks/useDebounce'
import { useDerivedState } from '@/hooks/useDerivedState'
import * as React from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Check, Loader2, ShieldCheck, ShieldOff, SlidersHorizontal } from 'lucide-react'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { ThresholdSliderHelp } from '@/components/SessionScoring/help-content'
import { useScoringQuery } from '../useScorerTimeseries'
import { Button } from '@/components/ui/button'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { Skeleton } from '@/components/ui/skeleton'
import { Slider } from '@/components/ui/slider'
import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import { formatCount, formatPercent } from '@/lib/format'
import { scoringConfigCompositeKey } from '@/lib/queries/scoring'
import type { Expect, Simplify, WireParity, WithRequired, WithRequiredNullable, components } from '@/types/api'

// ─── ThresholdSliderControls ─────────────────────────────────────────────────

type StatusCodeMeta = components['schemas']['ScoringEnforceStatusCodeState']
type CommittedThreshold = components['schemas']['ScoringThresholdState']
type EnforceThreshold = components['schemas']['ScoringEnforceThresholdResponse']

interface ThresholdSliderControlsProps {
  thresholdRaw: number
  onThresholdRawChange: (value: number) => void
  committed: CommittedThreshold | undefined
  enforce: EnforceThreshold | undefined
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

// ─── ThresholdSlider ─────────────────────────────────────────────────────────

export type BucketCounts = WithRequired<
  components['schemas']['ScoringThresholdBucket'],
  'total' | 'good' | 'bad' | 'unlabeled'
>

type _BucketCountsParity = Expect<WireParity<BucketCounts, components['schemas']['ScoringThresholdBucket']>>

interface ThresholdMatrixProps {
  flagged: BucketCounts
  passed: Omit<BucketCounts, 'total'>
}

export function ThresholdMatrix({ flagged, passed }: ThresholdMatrixProps) {
  return (
    <div className="grid grid-cols-2 gap-3">
      <Bucket
        title="Would FLAG"
        total={flagged.total}
        good={flagged.good}
        bad={flagged.bad}
        unlabeled={flagged.unlabeled}
        tone="warn"
      />
      <Bucket
        title="Would PASS"
        total={passed.good + passed.bad + passed.unlabeled}
        good={passed.good}
        bad={passed.bad}
        unlabeled={passed.unlabeled}
        tone="good"
      />
    </div>
  )
}

function Bucket({
  title,
  total,
  good,
  bad,
  unlabeled,
  tone,
}: {
  title: string
  total: number
  good: number
  bad: number
  unlabeled: number
  tone: 'warn' | 'good'
}) {
  const tint = tone === 'warn' ? 'border-amber-300 bg-amber-50/50' : 'border-emerald-300 bg-emerald-50/40'
  return (
    <div className={`p-3 border rounded-md ${tint}`}>
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground">{title}</div>
      <div className="text-xl font-mono font-semibold tabular-nums">{formatCount(total)}</div>
      <div className="mt-1 space-y-0.5 text-[11px]">
        <div className="flex justify-between">
          <span className="text-emerald-700">good</span>
          <span className="font-mono tabular-nums">{formatCount(good)}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-destructive">bad</span>
          <span className="font-mono tabular-nums">{formatCount(bad)}</span>
        </div>
        <div className="flex justify-between text-muted-foreground">
          <span>unlabeled</span>
          <span className="font-mono tabular-nums">{formatCount(unlabeled)}</span>
        </div>
      </div>
    </div>
  )
}

interface ThresholdPreviewStatsProps {
  precision: number | null
  recall: number | null
  totalScoredSessions: number
  sinceHours: number
}

export function ThresholdPreviewStats({
  precision,
  recall,
  totalScoredSessions,
  sinceHours,
}: ThresholdPreviewStatsProps) {
  return (
    <>
      <div className="grid grid-cols-2 gap-3 text-xs">
        <Stat
          label="Precision"
          value={precision != null ? formatPercent(precision, { decimals: 1 }) : '—'}
          hint="of labeled flagged sessions, how many are bad"
        />
        <Stat
          label="Recall"
          value={recall != null ? formatPercent(recall, { decimals: 1 }) : '—'}
          hint="of all labeled-bad sessions, how many got flagged"
        />
      </div>

      <p className="text-[11px] text-muted-foreground italic">
        {formatCount(totalScoredSessions)} distinct scored sessions in the last{' '}
        {sinceHours}h. Precision/recall only count sessions you&apos;ve labeled —
        the &quot;unlabeled&quot; tally is everything else.
      </p>
    </>
  )
}

function Stat({ label, value, hint }: { label: string; value: string; hint: string }) {
  return (
    <div className="p-3 border rounded-md">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</div>
      <div className="text-lg font-mono font-semibold tabular-nums">{value}</div>
      <div className="text-[10px] text-muted-foreground mt-0.5">{hint}</div>
    </div>
  )
}

type ThresholdPreviewResponse = Simplify<
  Omit<
    WithRequiredNullable<
      WithRequired<
        components['schemas']['ScoringThresholdPreviewResponse'],
        'threshold' | 'since_hours' | 'total_scored_sessions'
      >,
      'precision' | 'recall'
    >,
    'flagged' | 'passed'
  > & { flagged: BucketCounts; passed: Omit<BucketCounts, 'total'> }
>

type _ThresholdPreviewParity = Expect<WireParity<ThresholdPreviewResponse, components['schemas']['ScoringThresholdPreviewResponse']>>

interface ThresholdSliderProps {
  serviceId: string
  sinceHours?: number
}

export function ThresholdSlider({ serviceId, sinceHours = 24 }: ThresholdSliderProps) {
  const queryClient = useQueryClient()
  const [thresholdRaw, setThresholdRaw] = React.useState(75)
  const threshold = useDebounce(thresholdRaw, 150)

  const [pendingAction, setPendingAction] = React.useState<
    { action: 'commit' | 'enforce' | 'disable'; threshold: number }
    | { action: 'change-status-code'; statusCode: number }
    | null
  >(null)

  const { data: rawPreviewData, isLoading, error } = useScoringQuery(
    ['scoring-threshold-preview', serviceId, threshold, sinceHours],
    () =>
      client.GET('/api/services/{service_id}/scoring/threshold-preview', {
        params: {
          path: { service_id: serviceId },
          query: { threshold, since_hours: sinceHours },
        },
      }),
    { staleTime: 30_000 },
  )
  const data = rawPreviewData as ThresholdPreviewResponse | undefined

  const { data: committed } = useScoringQuery(
    ['scoring-threshold-committed', serviceId],
    () =>
      client.GET('/api/services/{service_id}/scoring/threshold', {
        params: { path: { service_id: serviceId } },
      }),
    { staleTime: 30_000 },
  )

  const [syncedServiceId, setSyncedServiceId] = React.useState<string | null>(null)
  if (syncedServiceId !== serviceId && committed?.threshold != null) {
    setSyncedServiceId(serviceId)
    setThresholdRaw(committed.threshold)
  }

  const commitMutation = useMutation({
    mutationFn: async (newThreshold: number | null) =>
      unwrapClient(
        client.PUT(
          '/api/services/{service_id}/scoring/threshold',
          {
            params: { path: { service_id: serviceId } },
            body: { threshold: newThreshold },
          },
        ),
      ),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['scoring-threshold-committed', serviceId] })
      queryClient.invalidateQueries({ queryKey: ['scoring-status', serviceId] })
      queryClient.invalidateQueries({ queryKey: scoringConfigCompositeKey(serviceId) })
      queryClient.invalidateQueries({ queryKey: ['scoring-audit', serviceId] })
    },
  })

  const isAlreadyCommitted = committed?.threshold === thresholdRaw

  const { data: enforce } = useScoringQuery(
    ['scoring-enforce-threshold', serviceId],
    () =>
      client.GET('/api/services/{service_id}/scoring/enforce-threshold', {
        params: { path: { service_id: serviceId } },
      }),
    { staleTime: 30_000 },
  )

  const enforceMutation = useMutation({
    mutationFn: async (newThreshold: number | null) =>
      unwrapClient(
        client.PUT(
          '/api/services/{service_id}/scoring/enforce-threshold',
          {
            params: {
              path: { service_id: serviceId },
              query: { confirm: true },
            },
            body: { threshold: newThreshold },
          },
        ),
      ),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['scoring-enforce-threshold', serviceId] })
      queryClient.invalidateQueries({ queryKey: ['scoring-status', serviceId] })
      queryClient.invalidateQueries({ queryKey: scoringConfigCompositeKey(serviceId) })
      queryClient.invalidateQueries({ queryKey: ['scoring-audit', serviceId] })
    },
  })

  const isEnforcingThis = enforce?.threshold === thresholdRaw

  const { data: statusCode } = useScoringQuery(
    ['scoring-enforce-status-code', serviceId],
    () =>
      client.GET('/api/services/{service_id}/scoring/enforce-status-code', {
        params: { path: { service_id: serviceId } },
      }),
    { staleTime: 30_000 },
  )

  const statusCodeMutation = useMutation({
    mutationFn: async (newCode: number | null) =>
      unwrapClient(
        client.PUT(
          '/api/services/{service_id}/scoring/enforce-status-code',
          {
            params: {
              path: { service_id: serviceId },
              query: { confirm: true },
            },
            body: { status_code: newCode },
          },
        ),
      ),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['scoring-enforce-status-code', serviceId] })
      queryClient.invalidateQueries({ queryKey: scoringConfigCompositeKey(serviceId) })
      queryClient.invalidateQueries({ queryKey: ['scoring-audit', serviceId] })
    },
  })

  const effectiveStatusCode = statusCode?.effective ?? 429

  const [codeDraft, setCodeDraft] = useDerivedState(String(effectiveStatusCode))

  const codeDraftNum = Number(codeDraft)
  const codeDraftValid =
    codeDraft !== '' &&
    !Number.isNaN(codeDraftNum) &&
    Number.isInteger(codeDraftNum) &&
    codeDraftNum >= (statusCode?.min ?? 400) &&
    codeDraftNum <= (statusCode?.max ?? 599)
  const codeDraftIsDirty = codeDraftValid && codeDraftNum !== effectiveStatusCode

  return (
    <AnalyticsCard
      title="Threshold preview"
      description={`Counterfactual: at threshold X, how many of the last ${sinceHours}h scored sessions would get flagged — and how well does that match your labels?`}
      icon={<SlidersHorizontal className="h-4 w-4" />}
      helpContent={<ThresholdSliderHelp />}
      helpTitle="About Threshold & Enforcement"
      error={error as (Error & { status?: number }) | null}
    >
      <div className="space-y-4">
        <ThresholdSliderControls
          thresholdRaw={thresholdRaw}
          onThresholdRawChange={setThresholdRaw}
          committed={committed}
          enforce={enforce}
          statusCode={statusCode}
          effectiveStatusCode={effectiveStatusCode}
          isAlreadyCommitted={isAlreadyCommitted}
          isEnforcingThis={isEnforcingThis}
          commitPending={commitMutation.isPending}
          enforcePending={enforceMutation.isPending}
          statusCodePending={statusCodeMutation.isPending}
          codeDraft={codeDraft}
          onCodeDraftChange={setCodeDraft}
          codeDraftValid={codeDraftValid}
          codeDraftIsDirty={codeDraftIsDirty}
          codeDraftNum={codeDraftNum}
          onCommitClick={() => setPendingAction({ action: 'commit', threshold: thresholdRaw })}
          onEnforceClick={() => {
            setPendingAction(
              isEnforcingThis
                ? { action: 'disable', threshold: enforce?.threshold ?? thresholdRaw }
                : { action: 'enforce', threshold: thresholdRaw },
            )
          }}
          onApplyStatusCode={() =>
            setPendingAction({ action: 'change-status-code', statusCode: codeDraftNum })
          }
          onResetStatusCode={() => {
            if (statusCode) {
              setPendingAction({ action: 'change-status-code', statusCode: statusCode.default })
            }
          }}
        />

        {isLoading || !data ? (
          <div className="grid grid-cols-2 gap-3">
            <Skeleton className="h-20 w-full" />
            <Skeleton className="h-20 w-full" />
          </div>
        ) : (
          <>
            <ThresholdMatrix flagged={data.flagged} passed={data.passed} />
            <ThresholdPreviewStats
              precision={data.precision}
              recall={data.recall}
              totalScoredSessions={data.total_scored_sessions}
              sinceHours={data.since_hours}
            />
          </>
        )}
      </div>

      <ConfirmDialog
        open={pendingAction !== null}
        onOpenChange={(open) => {
          if (
            !open &&
            !commitMutation.isPending &&
            !enforceMutation.isPending &&
            !statusCodeMutation.isPending
          ) {
            setPendingAction(null)
          }
        }}
        isPending={
          commitMutation.isPending || enforceMutation.isPending || statusCodeMutation.isPending
        }
        isDangerous={pendingAction?.action !== 'commit'}
        title={
          pendingAction?.action === 'commit'
            ? 'Commit threshold'
            : pendingAction?.action === 'disable'
              ? 'Disable enforcement'
              : pendingAction?.action === 'change-status-code'
                ? 'Change enforce response code'
                : 'Enforce threshold (LIVE)'
        }
        description={
          pendingAction?.action === 'commit' ? (
            <>
              Persist <span className="font-mono">{pendingAction.threshold}</span> as your committed
              threshold. This is a remembered preference only — it does NOT push to Compute or
              block any live requests.
            </>
          ) : pendingAction?.action === 'disable' ? (
            <>
              Currently enforcing at threshold{' '}
              <span className="font-mono">{pendingAction.threshold}</span>. Disable enforcement so
              the edge stops returning HTTP{' '}
              <span className="font-mono">{effectiveStatusCode}</span> for high-score sessions.
              Effective within seconds.
            </>
          ) : pendingAction?.action === 'change-status-code' ? (
            <>
              Re-deploy the enforce VCL snippet so flagged requests return HTTP{' '}
              <span className="font-mono">{pendingAction.statusCode}</span> instead of HTTP{' '}
              <span className="font-mono">{effectiveStatusCode}</span>. Takes ~5-10s (one Fastly
              version activation). Threshold + enforcement state are untouched.
            </>
          ) : pendingAction?.action === 'enforce' && enforce?.enforced && enforce.threshold != null && enforce.threshold !== pendingAction.threshold ? (
            <>
              Currently enforcing at threshold{' '}
              <span className="font-mono">{enforce.threshold}</span> (live blocking). Replace with
              threshold <span className="font-mono">{pendingAction.threshold}</span>? Sessions
              scoring between <span className="font-mono">{pendingAction.threshold}</span> and{' '}
              <span className="font-mono">{enforce.threshold}</span> will start getting HTTP{' '}
              <span className="font-mono">{effectiveStatusCode}</span> within seconds.
            </>
          ) : pendingAction?.action === 'enforce' ? (
            <>
              Enforce threshold <span className="font-mono">{pendingAction.threshold}</span>?
              Requests with score &ge; <span className="font-mono">{pendingAction.threshold}</span>{' '}
              will get HTTP <span className="font-mono">{effectiveStatusCode}</span> at the edge.
              Effective within seconds.
            </>
          ) : null
        }
        confirmLabel={
          pendingAction?.action === 'commit'
            ? 'Commit'
            : pendingAction?.action === 'disable'
              ? 'Disable'
              : pendingAction?.action === 'change-status-code'
                ? 'Re-deploy'
                : 'Enforce'
        }
        onConfirm={() => {
          if (!pendingAction) return
          if (pendingAction.action === 'commit') {
            commitMutation.mutate(pendingAction.threshold, {
              onSettled: () => setPendingAction(null),
            })
          } else if (pendingAction.action === 'disable') {
            enforceMutation.mutate(null, {
              onSettled: () => setPendingAction(null),
            })
          } else if (pendingAction.action === 'change-status-code') {
            const isDefault = statusCode != null && pendingAction.statusCode === statusCode.default
            statusCodeMutation.mutate(isDefault ? null : pendingAction.statusCode, {
              onSettled: () => setPendingAction(null),
            })
          } else {
            enforceMutation.mutate(pendingAction.threshold, {
              onSettled: () => setPendingAction(null),
            })
          }
        }}
      />
    </AnalyticsCard>
  )
}
