'use client'

import React from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useForm } from 'react-hook-form'
import { z } from 'zod'
import {
  Activity,
  AlertTriangle,
  Check,
  CheckCircle2,
  Clock,
  Filter,
  Gauge,
  KeyRound,
  Loader2,
  Lock,
  RotateCcw,
  ServerCrash,
  Users,
  XCircle,
} from 'lucide-react'
import { AnalyticsCard, CardErrorState } from '@/components/AnalyticsCard'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { Label } from '@/components/ui/label'
import { Skeleton } from '@/components/ui/skeleton'
import { Textarea } from '@/components/ui/textarea'
import { client, extractApiError, unwrapClient } from '@/lib/api'
import { formatCount, keyByOccurrence } from '@/lib/utils'
import type { components } from '@/types/api'
import { ScorerFailOpenHelp, ScoringHealthHelp } from './help-content'
import { scoringConfigCompositeKey, useScoringQuery } from './ScoringAdminPanels'

type FailOpenBucket = NonNullable<components['schemas']['ScoringReasonCount']>

function LintWarningsList({ warnings }: { warnings: string[] }) {
  return (
    <ul className="list-disc ml-4 mt-1">
      {keyByOccurrence(warnings.slice(0, 5), String).map(({ key, item: w }) => (
        <li key={key} className="font-mono text-[10px]">
          {w}
        </li>
      ))}
    </ul>
  )
}

function HealthMetric({
  icon: Icon,
  label,
  value,
  sub,
  tone = 'default',
}: {
  icon: React.ComponentType<{ className?: string }>
  label: string
  value: string | number
  sub?: string
  tone?: 'default' | 'warn' | 'good'
}) {
  const valueClass =
    tone === 'warn' ? 'text-destructive' : tone === 'good' ? 'text-green-600' : 'text-foreground'
  return (
    <div className="flex flex-col gap-1 p-3 rounded-md border bg-card">
      <div className="flex items-center gap-1.5 text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
        <Icon className="h-3 w-3" />
        {label}
      </div>
      <div className={`text-lg font-mono tabular-nums ${valueClass}`}>{value}</div>
      {sub && <div className="text-[10px] text-muted-foreground whitespace-pre-line">{sub}</div>}
    </div>
  )
}

function useScoringHealth(serviceId: string, sinceHours: number) {
  return useScoringQuery(['scoring-health', serviceId, sinceHours], () =>
    client.GET('/api/services/{service_id}/scoring/health', {
      params: { path: { service_id: serviceId }, query: { since_hours: sinceHours } },
    }),
  )
}

export function ScoringHealthCard({
  serviceId,
  sinceHours = 24,
}: {
  serviceId: string
  sinceHours?: number
}) {
  const { data, isLoading, isError, error, refetch } = useScoringHealth(serviceId, sinceHours)

  if (isError) {
    const raw = String(error?.message || 'Unknown error')
    const friendly =
      /No files found that match the pattern/i.test(raw) || /IO Error/i.test(raw)
        ? 'Scoring data is still warming up — try again in a few minutes.'
        : /timed out|timeout/i.test(raw)
          ? 'The scoring service took too long to respond. Retry, or check the scorer Compute service is reachable.'
          : raw
    return (
      <AnalyticsCard title="Scoring Health" description={`Operational metrics for the last ${sinceHours}h.`}>
        <CardErrorState
          icon={<AlertTriangle className="h-4 w-4" />}
          title="Scoring health unavailable"
          message={friendly}
          onRetry={() => refetch()}
        />
      </AnalyticsCard>
    )
  }

  if (isLoading || !data) {
    return (
      <AnalyticsCard title="Scoring Health" description={`Operational metrics for the last ${sinceHours}h.`}>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
          {['m1', 'm2', 'm3', 'm4', 'm5', 'm6'].map((k) => (
            <Skeleton key={k} className="h-20 w-full" />
          ))}
        </div>
      </AnalyticsCard>
    )
  }

  const fireRateTone: 'default' | 'warn' = (data.fire_rate_pct ?? 0) < 20 ? 'warn' : 'default'
  const failRate = data.fail_open_rate_pct ?? null
  const scorerErrors = data.scorer_errors ?? 0
  const errorsTone: 'default' | 'warn' | 'good' =
    scorerErrors === 0
      ? 'good'
      : failRate != null
        ? failRate > 2.5
          ? 'warn'
          : 'default'
        : scorerErrors > 10
          ? 'warn'
          : 'default'
  const lat = data.latency
  const rttP95Us = lat?.rtt_p95_us ?? null
  const fmtMs = (us: number | null | undefined) =>
    us == null ? '—' : `${(us / 1000).toFixed(us < 10_000 ? 1 : 0)}ms`
  const fmtUs = (us: number | null | undefined) =>
    us == null ? '—' : us >= 1_000 ? `${(us / 1000).toFixed(1)}ms` : `${us}µs`
  const latencyTone: 'default' | 'warn' = rttP95Us != null && rttP95Us / 1000 > 80 ? 'warn' : 'default'
  const topReasons = data.top_reasons ?? []

  return (
    <AnalyticsCard
      title="Scoring Health"
      description={`Snapshot for the last ${sinceHours}h — fire rate, score distribution, and top reasons.`}
      helpContent={<ScoringHealthHelp />}
    >
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
        <HealthMetric
          icon={Activity}
          label="Fire Rate"
          value={`${(data.fire_rate_pct ?? 0).toFixed(1)}%`}
          sub={`${formatCount(data.scored_rows ?? 0)} of ${formatCount(data.total_edge_rows ?? 0)} edge rows`}
          tone={fireRateTone}
        />
        <HealthMetric
          icon={Gauge}
          label="Avg Score"
          value={(data.avg_score ?? 0).toFixed(1)}
          sub={`p50 ${(data.p50_score ?? 0).toFixed(0)} · p95 ${(data.p95_score ?? 0).toFixed(0)} · max ${data.max_score ?? 0}`}
        />
        <HealthMetric
          icon={Users}
          label="Sessions"
          value={formatCount(data.distinct_sids ?? 0)}
          sub="distinct sids seen"
        />
        <HealthMetric
          icon={XCircle}
          label="Scorer Errors"
          value={formatCount(scorerErrors)}
          sub={failRate != null ? `${failRate.toFixed(2)}% of edge rows` : 'fail-open + auth fail rows'}
          tone={errorsTone}
        />
        <HealthMetric
          icon={Clock}
          label="Scorer Latency (p95 rtt)"
          value={rttP95Us != null ? fmtMs(rttP95Us) : '—'}
          sub={
            rttP95Us != null
              ? `rtt: p50 ${fmtMs(lat?.rtt_p50_us)} · p99 ${fmtMs(lat?.rtt_p99_us)} · max ${fmtMs(lat?.rtt_max_us)}\nexec: p50 ${fmtUs(lat?.exec_p50_us)} · p95 ${fmtUs(lat?.exec_p95_us)}`
              : 'awaiting re-provision'
          }
          tone={latencyTone}
        />
        <HealthMetric
          icon={AlertTriangle}
          label="Top Reason"
          value={topReasons[0]?.reason ?? '—'}
          sub={topReasons[0] ? `${formatCount(topReasons[0].count ?? 0)} hits` : 'no scored rows yet'}
        />
      </div>
      {topReasons.length > 0 && (
        <div className="mt-4 space-y-1">
          <div className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
            Score Reason Breakdown
          </div>
          <div className="flex flex-wrap gap-2">
            {topReasons.map((r) => (
              <div
                key={r.reason ?? ''}
                className="flex items-center gap-1.5 px-2 py-1 rounded-md border bg-muted/40 text-xs"
              >
                <span className="font-mono">{r.reason}</span>
                <span className="text-muted-foreground tabular-nums">{formatCount(r.count ?? 0)}</span>
              </div>
            ))}
          </div>
        </div>
      )}
      {data.matrix_staleness?.is_stale && (
        <div className="mt-4 p-3 border border-amber-300 bg-amber-50/60 rounded-md text-xs flex items-start gap-2">
          <AlertTriangle className="h-4 w-4 text-amber-600 mt-0.5 flex-none" />
          <div className="space-y-0.5">
            <div className="font-semibold text-amber-900">Matrix may be stale</div>
            <div className="text-amber-800">
              {(data.matrix_staleness.l2_high_pct ?? 0).toFixed(1)}% of L2-evaluated sessions (
              {formatCount(data.matrix_staleness.l2_high_count ?? 0)} of{' '}
              {formatCount(data.matrix_staleness.l2_evaluated ?? 0)}) are scoring ≥50 — above the{' '}
              {data.matrix_staleness.threshold_pct ?? 0}% threshold. The matrix is treating too much
              real traffic as rare. Click
              <strong className="px-0.5">Retrain matrix</strong> in the header to refresh it.
            </div>
          </div>
        </div>
      )}
    </AnalyticsCard>
  )
}

export function ScorerFailOpenBreakdownCard({
  serviceId,
  sinceHours = 24,
}: {
  serviceId: string
  sinceHours?: number
}) {
  const { data, isLoading, isError, error, refetch } = useScoringHealth(serviceId, sinceHours)
  const buckets = data?.fail_open_breakdown ?? []
  const total = buckets.reduce((acc, b) => acc + (b.count ?? 0), 0)
  const max = buckets.reduce((acc, b) => Math.max(acc, b.count ?? 0), 0)

  return (
    <AnalyticsCard
      title={`Fail-open breakdown — last ${sinceHours}h`}
      description="Fail-opens grouped by reason/status — why the scorer let requests through unscored."
      helpContent={<ScorerFailOpenHelp />}
      helpTitle="About Fail-open Breakdown"
    >
      {isError ? (
        <CardErrorState
          icon={<AlertTriangle className="h-4 w-4" />}
          title="Fail-open breakdown unavailable"
          message={(error as Error)?.message || 'Unknown error'}
          onRetry={() => refetch()}
        />
      ) : isLoading || !data ? (
        <div className="space-y-2">
          {['f1', 'f2', 'f3'].map((k) => (
            <Skeleton key={k} className="h-10 w-full" />
          ))}
        </div>
      ) : buckets.length === 0 ? (
        <div className="flex items-center gap-2 rounded-md border border-emerald-200 bg-emerald-50/60 px-3 py-3 text-sm text-emerald-800">
          <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-600" />
          <span>No fail-opens in the last {sinceHours}h — every routed request was scored.</span>
        </div>
      ) : (
        <div className="space-y-3">
          <div className="text-xs text-muted-foreground">
            <span className="font-mono tabular-nums text-foreground">{formatCount(total)}</span>{' '}
            fail-open {total === 1 ? 'row' : 'rows'} across{' '}
            <span className="font-mono tabular-nums text-foreground">{buckets.length}</span>{' '}
            {buckets.length === 1 ? 'class' : 'classes'}.
          </div>
          <div className="space-y-1.5">
            {buckets.map((b) => (
              <FailOpenRow key={b.reason ?? ''} bucket={b} max={max} />
            ))}
          </div>
        </div>
      )}
    </AnalyticsCard>
  )
}

function FailOpenRow({ bucket, max }: { bucket: FailOpenBucket; max: number }) {
  const { label, status, Icon } = describeFailOpen(bucket.reason ?? '')
  const widthPct = max > 0 ? Math.max(4, Math.round(((bucket.count ?? 0) / max) * 100)) : 0
  return (
    <div className="flex items-center gap-3 rounded-md border bg-card px-3 py-2 text-xs">
      <Icon className="h-4 w-4 shrink-0 text-destructive/80" />
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <span className="font-medium text-foreground">{label}</span>
          {status != null && (
            <span className="rounded bg-destructive/10 px-1.5 py-0.5 font-mono text-[10px] font-semibold text-destructive">
              {status}
            </span>
          )}
        </div>
        <div className="mt-1 h-1.5 w-full overflow-hidden rounded-full bg-border/50">
          <div className="h-full rounded-full bg-rose-500/50" style={{ width: `${widthPct}%` }} />
        </div>
        <code className="mt-1 block truncate text-[10px] text-muted-foreground">{bucket.reason}</code>
      </div>
      <span className="shrink-0 font-mono tabular-nums text-foreground">
        {formatCount(bucket.count ?? 0)}
      </span>
    </div>
  )
}

function describeFailOpen(reason: string): {
  label: string
  status: string | null
  Icon: React.ComponentType<{ className?: string }>
} {
  const m = /^compute-unavailable-(\d+|.+)$/.exec(reason)
  if (m) {
    const status = m[1]
    if (status === '503') return { label: 'Scorer timeout / unavailable', status, Icon: Clock }
    if (status === '500') return { label: 'Scorer error', status, Icon: ServerCrash }
    if (status === '401') return { label: 'Auth rejected', status, Icon: Lock }
    return { label: 'Scorer unavailable', status, Icon: ServerCrash }
  }
  if (reason === 'internal-error-keys') {
    return { label: 'Key / KV store load failure', status: null, Icon: KeyRound }
  }
  if (reason.startsWith('internal-error')) {
    return { label: 'Internal scorer error', status: null, Icon: ServerCrash }
  }
  if (reason === 'unauthorized') return { label: 'Unauthorized', status: null, Icon: Lock }
  return { label: reason, status: null, Icon: XCircle }
}

const EXCLUDE_REGEX_DESCRIPTION =
  'Requests whose URL matches this regex are NOT sent to the scoring Compute service. The default skips common static-asset extensions; override to scope scoring to specific traffic patterns.'

const excludeRegexFormSchema = z.object({ regex: z.string().max(2048) })
type ExcludeRegexFormValues = z.infer<typeof excludeRegexFormSchema>

type LintResult =
  | { regex: string; ok: true; lint_warnings: string[] }
  | { regex: string; ok: false; error: string; reason: string }

export function ExcludeRegexCard({ serviceId }: { serviceId: string }) {
  const queryClient = useQueryClient()
  const form = useForm<ExcludeRegexFormValues>({ defaultValues: { regex: '' } })
  const draft = form.watch('regex')
  const [confirmOpen, setConfirmOpen] = React.useState(false)
  const [lintResult, setLintResult] = React.useState<LintResult | null>(null)
  const [lintPending, setLintPending] = React.useState(false)

  const { data, isLoading, error } = useScoringQuery(
    ['scoring-exclude-regex', serviceId],
    () =>
      client.GET('/api/services/{service_id}/scoring/exclude-regex', {
        params: { path: { service_id: serviceId } },
      }),
    { staleTime: 60_000 },
  )

  const initialisedServiceRef = React.useRef<string | null>(null)
  React.useEffect(() => {
    if (data && initialisedServiceRef.current !== serviceId) {
      form.reset({ regex: data.current || data.effective || '' })
      initialisedServiceRef.current = serviceId
    }
  }, [data, serviceId, form])

  const isDirty = data ? draft !== (data.current || data.effective) : false
  const isResetToDefault = data ? draft === data.default && !data.is_default : false

  const saveMut = useMutation({
    mutationFn: async (regex: string) => {
      const parsed = excludeRegexFormSchema.safeParse({ regex })
      const validRegex = parsed.success ? parsed.data.regex : regex
      return unwrapClient(
        client.PUT('/api/services/{service_id}/scoring/exclude-regex', {
          params: { path: { service_id: serviceId }, query: { confirm: true } },
          body: { regex: validRegex },
        }),
      )
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['scoring-exclude-regex', serviceId] })
      queryClient.invalidateQueries({ queryKey: scoringConfigCompositeKey(serviceId) })
      queryClient.invalidateQueries({ queryKey: ['scoring-audit', serviceId] })
      setConfirmOpen(false)
    },
  })

  const runLintCheck = React.useCallback(
    async (regex: string) => {
      if (regex.trim() === '') {
        setLintResult(null)
        return
      }
      if (lintResult && lintResult.regex === regex) return
      setLintPending(true)
      try {
        const {
          data: r,
          error: errBody,
          response,
        } = await client.POST('/api/services/{service_id}/scoring/exclude-regex/validate', {
          params: { path: { service_id: serviceId } },
          body: { regex },
        })
        if (!response.ok || !r) {
          setLintResult({
            regex,
            ok: false,
            error: errBody ? extractApiError(errBody) : `HTTP ${response.status}`,
            reason: 'http_error',
          })
          return
        }
        setLintResult(
          r.ok
            ? { regex, ok: true, lint_warnings: r.lint_warnings || [] }
            : { regex, ok: false, error: r.error ?? '', reason: r.reason ?? '' },
        )
      } catch (e) {
        setLintResult({
          regex,
          ok: false,
          error: e instanceof Error ? e.message : String(e),
          reason: 'network_error',
        })
      } finally {
        setLintPending(false)
      }
    },
    [serviceId, lintResult],
  )

  const handleDraftChange = (v: string) => {
    form.setValue('regex', v, { shouldDirty: true })
    if (lintResult && lintResult.regex !== v) setLintResult(null)
  }

  if (isLoading || !data) {
    return (
      <AnalyticsCard
        title="URL exclusion regex"
        icon={<Filter className="h-4 w-4" />}
        description={EXCLUDE_REGEX_DESCRIPTION}
        error={!isLoading ? (error as (Error & { status?: number }) | null) : undefined}
      >
        {isLoading ? (
          <Skeleton className="h-32 w-full" />
        ) : (
          <Alert variant="destructive">
            <AlertTriangle className="h-4 w-4" />
            <AlertDescription>Failed to load current exclusion regex.</AlertDescription>
          </Alert>
        )}
      </AnalyticsCard>
    )
  }

  const activeLint = !lintPending && lintResult && lintResult.regex === draft ? lintResult : null

  return (
    <AnalyticsCard
      title="URL exclusion regex"
      icon={<Filter className="h-4 w-4" />}
      description={EXCLUDE_REGEX_DESCRIPTION}
      error={error as (Error & { status?: number }) | null}
    >
      <div className="space-y-4">
        <div className="space-y-1.5">
          <Label htmlFor="exclude-regex-textarea" className="text-xs font-semibold">
            Exclusion regex{' '}
            <span className="text-muted-foreground">
              ({data.is_default ? 'currently default' : 'currently custom override'})
            </span>
          </Label>
          <Textarea
            id="exclude-regex-textarea"
            value={draft}
            onChange={(e) => handleDraftChange(e.target.value)}
            onBlur={(e) => runLintCheck(e.target.value)}
            className="font-mono text-xs min-h-24"
            spellCheck={false}
            autoComplete="off"
          />
          <p className="text-[10px] text-muted-foreground">
            VCL pattern compiled by Fastly&apos;s regex engine (RE2). No quotes / control chars; max
            2 KB. Hit <span className="font-semibold">Reset to default</span> to drop any custom
            override. Lint runs on blur and on publish.
          </p>
        </div>
        {lintPending && (
          <div className="flex items-center gap-2 text-[11px] text-muted-foreground">
            <Loader2 className="h-3 w-3 animate-spin" /> Linting regex…
          </div>
        )}
        {activeLint && !activeLint.ok && (
          <Alert variant="destructive">
            <AlertTriangle className="h-4 w-4" />
            <AlertDescription className="text-xs">
              <strong>Lint error ({activeLint.reason}):</strong>
              <div className="font-mono whitespace-pre-wrap mt-1">{activeLint.error}</div>
            </AlertDescription>
          </Alert>
        )}
        {activeLint && activeLint.ok && activeLint.lint_warnings.length > 0 && (
          <Alert>
            <AlertTriangle className="h-4 w-4" />
            <AlertDescription className="text-xs">
              <strong>Lint warnings (non-blocking):</strong>
              <LintWarningsList warnings={activeLint.lint_warnings} />
            </AlertDescription>
          </Alert>
        )}
        {activeLint && activeLint.ok && activeLint.lint_warnings.length === 0 && isDirty && (
          <div className="flex items-center gap-2 text-[11px] text-emerald-700">
            <Check className="h-3 w-3" /> Lint clean — ready to publish.
          </div>
        )}
        {saveMut.isError && (
          <Alert variant="destructive">
            <AlertTriangle className="h-4 w-4" />
            <AlertDescription className="text-xs whitespace-pre-wrap font-mono">
              {(saveMut.error as Error).message}
            </AlertDescription>
          </Alert>
        )}
        {saveMut.isSuccess && (
          <Alert>
            <Check className="h-4 w-4" />
            <AlertDescription className="text-xs">
              {saveMut.data.message}
              {saveMut.data.lint_warnings && saveMut.data.lint_warnings.length > 0 && (
                <div className="mt-2">
                  <strong>Lint warnings (non-blocking):</strong>
                  <LintWarningsList warnings={saveMut.data.lint_warnings} />
                </div>
              )}
            </AlertDescription>
          </Alert>
        )}
        <div className="flex items-center justify-end gap-2 pt-2 border-t">
          <Button
            variant="outline"
            size="sm"
            onClick={() => form.setValue('regex', data.default ?? '', { shouldDirty: true })}
            disabled={(data.is_default ?? false) && draft === ''}
            className="text-xs"
          >
            <RotateCcw className="h-3 w-3 mr-1" /> Reset to default
          </Button>
          <Button
            size="sm"
            onClick={() => setConfirmOpen(true)}
            disabled={!isDirty || saveMut.isPending}
            className="text-xs"
          >
            {saveMut.isPending ? (
              <>
                <Loader2 className="h-3 w-3 mr-1 animate-spin" />
                Publishing…
              </>
            ) : isResetToDefault ? (
              'Reset & publish'
            ) : (
              'Save & publish'
            )}
          </Button>
        </div>
      </div>
      <ConfirmDialog
        open={confirmOpen}
        onOpenChange={setConfirmOpen}
        title="Publish updated scoring exclusion regex?"
        description={
          isResetToDefault
            ? 'This will revert this service to the built-in default static-asset regex and activate a new VCL version on the Fastly service. Effective at the edge after activation propagates (~5-15s).'
            : "This will clone the active Fastly version, swap the scoring recv snippet to use your custom regex, validate via falco + Fastly's compiler, and activate. Effective at the edge after activation propagates (~5-15s). A bad regex could disable scoring entirely or DoS Compute — double-check the value above."
        }
        confirmLabel="Publish"
        isDangerous
        isPending={saveMut.isPending}
        onConfirm={() => saveMut.mutate(draft)}
      />
    </AnalyticsCard>
  )
}
