'use client'

import * as React from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { SlidersHorizontal } from 'lucide-react'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { ThresholdSliderHelp } from '@/components/SessionScoring/help-content'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { Skeleton } from '@/components/ui/skeleton'
import { client } from '@/lib/api'

import { ThresholdMatrix } from './Matrix'
import { ThresholdPreviewStats } from './Preview'
import { ThresholdSliderControls } from './Slider'

interface ThresholdPreviewResponse {
  threshold: number
  since_hours: number
  total_scored_sessions: number
  flagged: { total: number; good: number; bad: number; unlabeled: number }
  passed: { good: number; bad: number; unlabeled: number }
  precision: number | null
  recall: number | null
}

interface ThresholdSliderProps {
  serviceId: string
  sinceHours?: number
}

/**
 * Counterfactual preview of "at threshold X, what gets flagged?"
 *
 * Operator drags the slider; we show:
 *  - How many sessions land in each bucket (flagged vs passed)
 *  - 2x2 split: good / bad / unlabeled within each bucket
 *  - Precision (of flagged-labeled, how many are bad) + Recall
 *    (of all-bad-labeled, how many got flagged)
 *
 * Reporting-only — does not push the threshold to VCL/Compute. Acts as
 * a tuning surface so the operator picks an evidence-backed cutoff
 * before enabling enforcement.
 */
export function ThresholdSlider({ serviceId, sinceHours = 24 }: ThresholdSliderProps) {
  const queryClient = useQueryClient()
  // Local state for the slider — debounced into the React Query key so we
  // don't fire a new request on every drag pixel.
  const [thresholdRaw, setThresholdRaw] = React.useState(75)
  const [threshold, setThreshold] = React.useState(75)
  React.useEffect(() => {
    const t = setTimeout(() => setThreshold(thresholdRaw), 150)
    return () => clearTimeout(t)
  }, [thresholdRaw])

  // Controls the confirm dialog (commit / enforce / disable / change-status-code).
  // `value` is the threshold (0-100) for commit/enforce/disable, or the new
  // HTTP status code (400-599) for change-status-code. null = closed.
  const [pendingAction, setPendingAction] = React.useState<
    { action: 'commit' | 'enforce' | 'disable'; threshold: number }
    | { action: 'change-status-code'; statusCode: number }
    | null
  >(null)

  const { data, isLoading } = useQuery({
    queryKey: ['scoring-threshold-preview', serviceId, threshold, sinceHours],
    queryFn: async () => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/threshold-preview' as any,
        {
          params: {
            path: { service_id: serviceId },
            query: { threshold, since_hours: sinceHours },
          },
        } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as ThresholdPreviewResponse
    },
    staleTime: 30_000,
  })

  // The operator's previously-committed threshold (persisted in cfg —
  // NOT enforced by the live scorer; just a remembered preference).
  const { data: committed } = useQuery({
    queryKey: ['scoring-threshold-committed', serviceId],
    queryFn: async () => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/threshold' as any,
        { params: { path: { service_id: serviceId } } } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as { threshold: number | null; set_at: string | null; enforced: boolean }
    },
    staleTime: 30_000,
  })

  // One-shot: when the committed threshold loads, jump the slider to it so
  // the operator isn't staring at a misleading default of 75. Guarded by a
  // ref so subsequent edits don't get clobbered by the query refetching.
  const syncedFromCommittedRef = React.useRef(false)
  React.useEffect(() => {
    if (syncedFromCommittedRef.current) return
    if (committed?.threshold != null && thresholdRaw === 75) {
      syncedFromCommittedRef.current = true
      setThresholdRaw(committed.threshold)
      setThreshold(committed.threshold)
    }
    // intentionally not depending on thresholdRaw — one-shot sync
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [committed?.threshold])

  const commitMutation = useMutation({
    mutationFn: async (newThreshold: number | null) => {
      const { data, response } = await client.PUT(
        '/api/services/{service_id}/scoring/threshold' as any,
        {
          params: { path: { service_id: serviceId } },
          body: { threshold: newThreshold },
        } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['scoring-threshold-committed', serviceId] })
      queryClient.invalidateQueries({ queryKey: ['scoring-status', serviceId] })
    },
  })

  const isAlreadyCommitted = committed?.threshold === thresholdRaw

  // Live enforcement (writes to Compute ConfigStore via Fastly API)
  const { data: enforce } = useQuery({
    queryKey: ['scoring-enforce-threshold', serviceId],
    queryFn: async () => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/enforce-threshold' as any,
        { params: { path: { service_id: serviceId } } } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as { threshold: number | null; enforced: boolean }
    },
    staleTime: 30_000,
  })

  const enforceMutation = useMutation({
    mutationFn: async (newThreshold: number | null) => {
      const { data, response } = await client.PUT(
        '/api/services/{service_id}/scoring/enforce-threshold' as any,
        {
          params: {
            path: { service_id: serviceId },
            query: { confirm: true },
          },
          body: { threshold: newThreshold },
        } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['scoring-enforce-threshold', serviceId] })
      queryClient.invalidateQueries({ queryKey: ['scoring-status', serviceId] })
    },
  })

  const isEnforcingThis = enforce?.threshold === thresholdRaw

  // Operator-overridable HTTP status code returned by the enforce snippet.
  // Default 429. Reads back cfg.scoring.enforce_status_code via the backend.
  const { data: statusCode } = useQuery({
    queryKey: ['scoring-enforce-status-code', serviceId],
    queryFn: async () => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/enforce-status-code' as any,
        { params: { path: { service_id: serviceId } } } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as {
        current: number | null
        default: number
        effective: number
        min: number
        max: number
        is_default: boolean
      }
    },
    staleTime: 30_000,
  })

  const statusCodeMutation = useMutation({
    mutationFn: async (newCode: number | null) => {
      const { data, response } = await client.PUT(
        '/api/services/{service_id}/scoring/enforce-status-code' as any,
        {
          params: {
            path: { service_id: serviceId },
            query: { confirm: true },
          },
          body: { status_code: newCode },
        } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['scoring-enforce-status-code', serviceId] })
    },
  })

  // Effective code for display + dynamic copy (falls back to 429 before the
  // query resolves so dialogs read sensibly on first mount).
  const effectiveStatusCode = statusCode?.effective ?? 429

  // Local draft for the free-form status-code input — operator types any
  // 4xx/5xx code (backend validates 400-599 inclusive), clicks Apply, then
  // confirms. Local state isolates typing from the committed value so we
  // don't fire a re-publish on every keystroke. Sync once when the query
  // resolves, and again whenever the committed value changes from elsewhere
  // (e.g. after this user clicks Apply → confirm → mutation succeeds).
  const [codeDraft, setCodeDraft] = React.useState<string>(String(effectiveStatusCode))
  React.useEffect(() => {
    setCodeDraft(String(effectiveStatusCode))
  }, [effectiveStatusCode])

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
