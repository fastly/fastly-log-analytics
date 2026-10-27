'use client'

import React, { useState, type ReactNode } from 'react'
import { queryOptions, useMutation, useQuery, useQueryClient, type QueryClient, type QueryKey, type UseQueryOptions } from '@tanstack/react-query'
import { useForm } from 'react-hook-form'
import { z } from 'zod'
import { Activity, AlertTriangle, Bookmark, Brain, Check, CheckCircle, CheckCircle2, Clock, Filter, Gauge, History, Info, KeyRound, LineChart, ListChecks, Loader2, Lock, Pencil, RotateCcw, ScrollText, ServerCrash, Shield, ShieldAlert, ShieldCheck, ShieldOff, SlidersHorizontal, Trash2, Users, XCircle, ChevronRight, Flag, X, Archive, Database, ExternalLink, FileCode, Layers, Power, PowerOff, RefreshCw, TrendingUp } from 'lucide-react'
import { ColumnDef, flexRender, getCoreRowModel, useReactTable } from '@tanstack/react-table'
import { AnalyticsCard, CardErrorState, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { DataTableReadonly, DateTimeCell, DataTable, ColumnVisibilityDropdown, columnIdOf } from '@/components/DataTable'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { ConfirmDialog, CopyButton, LabelWithInfo } from '@/components/ui/confirm-dialog'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Skeleton, SkeletonGrid } from '@/components/ui/skeleton'
import { Slider } from '@/components/ui/slider'
import { useDebounce } from '@/hooks/useDashboardCards'
import { Textarea } from '@/components/ui/textarea'
import { useAutoFlash, useDeferredDeleteWithUndo, useDerivedState, useOpenGeneration, useDateFormat } from '@/hooks/useSSE'
import { client, extractApiError, unwrapClient, queryKeys } from '@/lib/api'
import { formatBytes, formatCount, formatDeterministicUTC, formatPercent, formatTimeAgo, keyByOccurrence, formatLocalDateTime, CHART_COLORS, denseBarTrace, denseTimeGrid, lineTrace, withSecondaryAxis, formatBytesTiered, formatCountMaybe, formatPctValue, formatRelative, cn } from '@/lib/utils'
import type { Expect, Simplify, WireParity, WithRequired, WithRequiredNullable, components, PlotlyLayout, PlotlyTrace } from '@/types/api'
import { Table, TableBody, TableCell, TableRow, TableHead, TableHeader } from '@/components/ui/table'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Switch } from '@/components/ui/switch'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { PlotlyChart } from '@/components/PlotlyChart'
import { SSEModal } from '@/components/SSEModal'
import { useMounted } from '@/hooks/useBootstrap'
import { useServiceStore } from '@/stores/filterStore'

export const scoringAnalyticsCompositeKey = (serviceId: string | null | undefined) => ['scoring-analytics-composite', serviceId] as const
export const scoringConfigCompositeKey = (serviceId: string | null | undefined) => ['scoring-config-composite', serviceId] as const
export const scoringAnalyticsCompositeOptions = (serviceId: string | null | undefined, sinceHours: number) => queryOptions({
    queryKey: [...scoringAnalyticsCompositeKey(serviceId), sinceHours] as const,
    queryFn: async ({ signal }) => unwrapClient(client.GET('/api/services/{service_id}/scoring/analytics', { params: { path: { service_id: serviceId ?? '' }, query: { since_hours: sinceHours } }, signal })),
  })
export const scoringConfigCompositeOptions = (serviceId: string | null | undefined) => queryOptions({ queryKey: scoringConfigCompositeKey(serviceId),
    queryFn: async ({ signal }) => unwrapClient(client.GET('/api/services/{service_id}/scoring/config', { params: { path: { service_id: serviceId ?? '' } }, signal })),
  })
type LabelRowWire = components['schemas']['ScoringLabelRow']
export type LabelValue = LabelRowWire['label']
export type LabelRow = LabelRowWire
interface ScoringLabelsResult { labels: LabelRow[]; counts: Record<LabelValue, number>; labelBySid: Map<string, LabelValue>
  idBySid: Map<string, string>; isLoading: boolean; isFetching: boolean; error: (Error & { status?: number }) | null
}
export function useScoringLabels(serviceId: string, opts: { enabled?: boolean } = {}): ScoringLabelsResult { const { enabled = true } = opts
  const q = useQuery({ queryKey: queryKeys.scoringLabels(serviceId), enabled: enabled && !!serviceId,
    queryFn: async () => unwrapClient(client.GET('/api/services/{service_id}/scoring/labels', { params: { path: { service_id: serviceId } } })),
    staleTime: 5 * 60_000, })
  const { labelBySid, idBySid } = React.useMemo(() => { const lab = new Map<string, LabelValue>()
    const id = new Map<string, string>()
    for (const l of q.data?.labels ?? []) { lab.set(l.sid, l.label); id.set(l.sid, l.id) }
    return { labelBySid: lab, idBySid: id }
  }, [q.data])
  return { labels: q.data?.labels ?? [], counts: q.data?.counts ?? { good: 0, bad: 0, neutral: 0 },
    labelBySid, idBySid, isLoading: q.isLoading, isFetching: q.isFetching, error: (q.error as (Error & { status?: number }) | null) ?? null, }
}
export const SCORING_KEY_PREFIXES = [ 'scoring-status', 'scoring-evaluation', 'scoring-score-dist', 'scoring-latency-timeseries',
  'scoring-fail-open-breakdown', 'scoring-compliance', 'scoring-top-flagged', 'scoring-per-reason-auc',
  'scoring-threshold-preview', 'scoring-threshold-committed', 'scoring-enforce-threshold',
  'scoring-enforce-status-code', 'scoring-l2-enforce', 'scoring-exclude-regex',
  'scoring-matrix-versions', 'scoring-audit', 'scoring-session-events', 'scoring-labels', ] as const
export function invalidateServiceScoringQueries(queryClient: QueryClient, serviceId: string): void { if (!serviceId) return
  queryClient.invalidateQueries({ queryKey: scoringAnalyticsCompositeKey(serviceId) })
  queryClient.invalidateQueries({ queryKey: scoringConfigCompositeKey(serviceId) })
  for (const prefix of SCORING_KEY_PREFIXES) queryClient.invalidateQueries({ queryKey: [prefix, serviceId] })
}
export function useScoringQuery<TData>( queryKey: QueryKey, fn: () => Promise<{ data?: TData; error?: unknown; response: Response }>,
  options?: Omit<UseQueryOptions<TData, Error>, 'queryKey' | 'queryFn'>, ) {
  return useQuery<TData, Error>({ queryKey, queryFn: () => unwrapClient(fn()), staleTime: 60_000, ...options })
}
export type LatencyRow = components['schemas']['ScoringLatencyRow']
export type LatencyResponse = components['schemas']['ScoringLatencyTimeseriesResponse']
export const usToMs = (us: number | null | undefined) => (us == null ? null : us / 1000)
export function deriveScorerSeries(data: LatencyResponse | undefined) { const rows = data?.rows ?? []
  return { rows, hours: rows.map((r) => r.hour), perMinute: data?.granularity === 'minute' }
}
export function scorerHourlyLayout(perMinute: boolean, yTitle: string, showlegend: boolean) { return { showlegend,
    margin: { l: 50, r: 20, t: showlegend ? 35 : 10, b: 40 }, xaxis: { title: '', type: 'date', ...(perMinute ? { tickformat: '%H:%M' } : {}) },
    yaxis: { title: yTitle, rangemode: 'tozero' },
    legend: { orientation: 'h' as const, y: 1.05, x: 0.5, xanchor: 'center' as const, yanchor: 'bottom' as const, font: { size: 9.5 }, bgcolor: 'transparent' }, }
}
export function useScorerTimeseries(serviceId: string, sinceHours: number) { return useScoringQuery( ['scoring-latency-timeseries', serviceId, sinceHours],
    () => client.GET('/api/services/{service_id}/scoring/latency-timeseries', { params: { path: { service_id: serviceId }, query: { since_hours: sinceHours } } }), )
}
function HelpList({ intro, items }: { intro: React.ReactNode; items: [React.ComponentType<{ className?: string }>, string, React.ReactNode][] }) { return (
    <div className="space-y-4 text-sm text-muted-foreground leading-relaxed">
      <p>{intro}</p>
      <ul className="space-y-3 list-none pl-0">
        {items.map(([Icon, cls, body], idx) => (
          <li key={`${cls}-${idx.toString()}`} className="flex gap-3">
            <Icon className={`h-5 w-5 shrink-0 ${cls} mt-0.5`} />
            <span>{body}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}
export const StatusPanelHelp = () => ( <HelpList
    intro={<>The Session Scorer uses a highly-optimized, multi-layer classification engine running at the <strong>Fastly Compute</strong> edge. Every client request is evaluated in real-time to assess risk.</>}
    items={[
      [Shield, 'text-primary', <><strong>Layer 1 (L1 - Deterministic Checks):</strong> Fast, rules-based verification checking session duration limits, query structures, and cryptographic signature validity of the session cookie.</>],
      [Activity, 'text-emerald-500', <><strong>Layer 2 (L2 - Transition Analysis):</strong> Evaluates navigational behavior. It scores the unlikeliness of specific route-to-route transitions using a site-specific PageRank-trained matrix.</>],
      [Info, 'text-blue-500', <><strong>Infrastructure:</strong> Toggling the scorer dynamically deploys Wasm binaries, registers 6 custom logging custom_fields, and updates Edge VCL snippets without downtime.</>],
    ]} />
)
export const ScoringHealthHelp = () => ( <HelpList
    intro="Operational metrics monitoring the throughput, effectiveness, and reliability of the edge scoring system over the selected time window."
    items={[
      [Activity, 'text-primary', <><strong>Fire Rate:</strong> The percentage of total CDN request volume routed through and successfully classified by the scorer.</>],
      [Gauge, 'text-blue-500', <><strong>Average & Percentiles:</strong> Average, median (p50), tail latency (p95), and maximum scores. Higher values denote greater anomalous/threat activity.</>],
      [XCircle, 'text-destructive', <><strong>Scorer Errors:</strong> Failures like token authentication issues or timeouts. High numbers indicate configuration mismatch or network degradation (the system safely fails-open).</>],
      [AlertTriangle, 'text-amber-500', <><strong>Matrix Staleness:</strong> Triggered when a high percentage of live traffic scores ≥ 50, indicating the matrix is treating legitimate, updated routes as anomalous. Click <strong>Retrain matrix</strong> to resolve.</>],
    ]} />
)
export const ThresholdSliderHelp = () => ( <HelpList
    intro="Simulate and enforce the sensitivity threshold for edge blocking."
    items={[
      [CheckCircle, 'text-emerald-500', <><strong>Commit Threshold:</strong> Saves a local reference cutoff for your team to visualize. This does not block live traffic, but serves as a tuning baseline.</>],
      [ShieldAlert, 'text-destructive', <><strong>Enforce (LIVE BLOCKING):</strong> Deploys the threshold directly to Fastly ConfigStore. Real-time requests scoring ≥ the threshold will be immediately blocked at the edge with the configured HTTP status code (default 429; override via the &quot;Enforce response code&quot; selector — 403 / 451 / 503 also supported).</>],
      [SlidersHorizontal, 'text-primary', <><strong>Precision & Recall:</strong> Statistical performance measures. Precision shows what portion of flagged sessions are actually labeled bad. Recall shows what portion of all bad sessions were successfully caught.</>],
    ]} />
)
export const RocPrCurvesHelp = () => ( <HelpList
    intro="Sanity checks displaying the classification power of the current scoring model against your analyst-assigned labels."
    items={[
      [LineChart, 'text-primary', <><strong>ROC Curve (AUC):</strong> Measures how well the matrix separates good and bad sessions. Curves hugging the top-left indicate great separation; the diagonal represents random guessing.</>],
      [LineChart, 'text-emerald-500', <><strong>Precision-Recall Curve (AP):</strong> Evaluates quality when dealing with highly imbalanced classes (typical of bot traffic). High AP (area under PR curve) means the model maintains high accuracy even when catching most threats.</>],
    ]} />
)
export const PerReasonAucHelp = () => ( <HelpList
    intro="Predictive performance of individual L1/L2 rule atoms (e.g., cookie compliance issues, impossible speed, anomalous navigation paths)."
    items={[[ListChecks, 'text-primary', <><strong>Relevance:</strong> Helps operators see which rules are highly predictive (AUC &gt; 0.8) and which are mostly noisy (AUC ~ 0.5) for the current site&apos;s legitimate traffic patterns.</>]]}
  />
)
export const TopFlaggedHelp = () => ( <HelpList
    intro="Real-time feed of the highest-scoring client sessions currently visiting your service."
    items={[
      [Users, 'text-primary', <><strong>Session ID (SID):</strong> Click any SID to open the deep request timeline and observe absolute timing offsets, sequence headers, and referrers.</>],
      [Bookmark, 'text-emerald-500', <><strong>Labeling:</strong> Use the Flag column to label a session as Good, Bad, or Neutral. This updates the dataset used to compute AUC metrics and retrain the behavioral model.</>],
    ]} />
)
export const ScoreDistHelp = () => ( <HelpList
    intro="Hourly aggregation of requests categorized into four distinct risk buckets."
    items={[ [Activity, 'text-primary', <><strong>Risk Buckets:</strong> 0-25 (Safe), 25-50 (Low Risk), 50-75 (Suspicious), and 75-100 (Anomalous).</>],
      [AlertTriangle, 'text-amber-500', <><strong>Analysis:</strong> Sudden spikes in orange (50-75) or red (75-100) buckets represent volumetric scanning, scraper surges, or credential-stuffing campaigns.</>],
    ]} />
)
export const ScorerLatencyHelp = () => ( <HelpList
    intro={<>Hourly latency of the Compute scorer sub-fetch. Read it alongside the Scorer&nbsp;errors chart — fail-opens are a latency symptom.</>}
    items={[
      [LineChart, 'text-primary', <><strong>Round-trip (solid):</strong> edge-observed p50/p95/p99 latency (<code>edge_score_rtt_us</code>) — network + Compute cold-start + Wasm exec. This is the number compared against the backend timeout (~100&nbsp;ms).</>],
      [Activity, 'text-emerald-500', <><strong>Exec (dashed):</strong> the scorer&apos;s own Wasm time (<code>edge_score_exec_us</code>) is ~µs; if the round-trip lines sit far above the exec lines, the cost is network/cold-start, not compute.</>],
    ]} />
)
export const ScorerErrorsHelp = () => ( <HelpList
    intro={<>Hourly count of scorer fail-opens. Read it alongside the Scorer&nbsp;latency chart — these rise as latency does.</>}
    items={[
      [AlertTriangle, 'text-destructive', <><strong>Fail-opens:</strong> rows that timed out or failed auth and were let through unscored (the system fails open rather than blocking on a scorer error).</>],
      [LineChart, 'text-primary', <><strong>Correlate with latency:</strong> when scorer p95 climbs toward the ~100&nbsp;ms timeout budget, these bars rise with it — that&apos;s the signal to raise the timeout or investigate cold starts.</>],
    ]} />
)
export const ScorerFailOpenHelp = () => ( <HelpList
    intro={<>When the scorer can&apos;t produce a verdict it fails <strong>open</strong> — the request flows through unscored rather than being blocked. This card groups those fail-opens by the exact reason so you can tell the failure modes apart (the Scorer&nbsp;errors chart shows <em>when</em>; this shows <em>what kind</em>).</>}
    items={[
      [AlertTriangle, 'text-destructive', <><strong>compute-unavailable-503:</strong> the scorer sub-fetch timed out or the service was unreachable — usually cold-start latency past the timeout budget. Raise the timeout or investigate cold starts.</>],
      [XCircle, 'text-destructive', <><strong>compute-unavailable-500 / -401:</strong> a 500 is a Wasm trap inside the scorer; a 401 means the VCL-stamped <code>X-Edge-Scorer-Auth</code> secret drifted from the scoring_keys store — redeploy to re-sync it.</>],
      [ShieldAlert, 'text-amber-500', <><strong>internal-error-keys:</strong> the scorer started but couldn&apos;t load the AES keys / KV store — a provisioning issue on the scoring service, not a transient timeout.</>],
    ]} />
)
export const ComplianceHelp = () => ( <HelpList
    intro="Cookie state integrity evaluated on the initial Edge VCL pass before routing requests."
    items={[
      [AlertTriangle, 'text-amber-500', <><strong>missing:</strong> The request requested a dynamic path but did not include the session cookie. This is a primary indicator of automated web scrapers.</>],
      [ShieldAlert, 'text-destructive', <><strong>tampered:</strong> The session cookie signature failed cryptographic validation, indicating active replay or session hijacking attempts.</>],
      [Info, 'text-blue-500', <><strong>expired:</strong> The session exceeded its hard-cap lifetime and is slated to receive a clean renewal cookie.</>], ]} />
)
export const LabelsHelp = () => ( <HelpList
    intro="Management interface for analyst-defined ground-truth session ratings."
    items={[
      [Bookmark, 'text-emerald-500', <><strong>Purpose:</strong> Labels are compiled into evaluation datasets for calculating ROC curves. They are not active blocking rules — edge blocking is driven exclusively by the score threshold.</>],
      [Info, 'text-blue-500', <><strong>SID Lifespans:</strong> SIDs are encrypted state tokens that naturally rotate to secure client-side transactions. If a visitor clears cookies or is idle too long, they will get a fresh SID.</>],
    ]} />
)
export const MatrixHistoryHelp = () => ( <HelpList
    intro="Deploy and manage transition matrices for behavioral modeling."
    items={[
      [History, 'text-primary', <><strong>History:</strong> Lists all transition matrices generated by retraining. You can roll back to any prior matrix version with one click.</>],
      [Shield, 'text-blue-500', <><strong>Deployment:</strong> Restoring a matrix immediately switches L2 edge evaluation to that behavioral dataset.</>], ]} />
)
export const AuditLogHelp = () => ( <HelpList
    intro="Immutable ledger tracking administrative session scoring operations on this service."
    items={[[ScrollText, 'text-primary', <><strong>Tracking:</strong> Records operator-attributed events such as threshold overrides, matrix deployments, key rotations, and toggles.</>]]}
  />
)
type RetrainResult = components['schemas']['ScoringRetrainResponse']
type RotateKeyResponse = components['schemas']['ScoringRotateKeyResponse']
type FailOpenBucket = NonNullable<components['schemas']['ScoringReasonCount']>
type RestoreResponse = components['schemas']['ScoringMatrixRestoreResponse']
type MatrixVersion = components['schemas']['ScoringMatrixVersion']
type AuditRow = components['schemas']['ScoringAuditRow']
function DeployHintBox({ title, hint, ariaLabel, copiedAriaLabel, buttonTitle, timeoutMs }: {
  title: string; hint: string; ariaLabel: string; copiedAriaLabel: string; buttonTitle?: string; timeoutMs?: number
}) { return ( <div className="rounded-md border bg-muted overflow-hidden">
      <div className="px-3 py-1.5 border-b bg-muted/50 flex items-center justify-between">
        <span className="text-[10px] font-mono text-muted-foreground">{title}</span>
        <CopyButton text={hint} ariaLabel={ariaLabel} copiedAriaLabel={copiedAriaLabel} title={buttonTitle} className="h-6 w-6 hover:bg-muted-foreground/10" timeoutMs={timeoutMs} />
      </div>
      <pre className="p-3 text-[11px] font-mono text-muted-foreground leading-relaxed whitespace-pre-wrap break-all">{hint}</pre>
    </div>
  )
}
function LintWarningsList({ warnings }: { warnings: string[] }) { return ( <ul className="list-disc ml-4 mt-1">
      {keyByOccurrence(warnings.slice(0, 5), String).map(({ key, item: w }) => ( <li key={key} className="font-mono text-[10px]">{w}</li>
      ))}
    </ul>
  )
}
function HealthMetric({ icon: Icon, label, value, sub, tone = 'default' }: {
  icon: React.ComponentType<{ className?: string }>; label: string; value: string | number; sub?: string; tone?: 'default' | 'warn' | 'good'
}) { const valueClass = tone === 'warn' ? 'text-destructive' : tone === 'good' ? 'text-green-600' : 'text-foreground'
  return ( <div className="flex flex-col gap-1 p-3 rounded-md border bg-card">
      <div className="flex items-center gap-1.5 text-[10px] font-bold uppercase tracking-wider text-muted-foreground"><Icon className="h-3 w-3" />{label}</div>
      <div className={`text-lg font-mono tabular-nums ${valueClass}`}>{value}</div>
      {sub && <div className="text-[10px] text-muted-foreground whitespace-pre-line">{sub}</div>}
    </div>
  )
}
function useScoringHealth(serviceId: string, sinceHours: number) { return useScoringQuery(['scoring-health', serviceId, sinceHours], () =>
    client.GET('/api/services/{service_id}/scoring/health', { params: { path: { service_id: serviceId }, query: { since_hours: sinceHours } } }), )
}
// ─── 1. LabelsTab ─────────────────────────────────────────────────────────────
function labelBadge(label: LabelValue) { if (label === 'bad') return <Badge className="bg-rose-600 hover:bg-rose-600">bad</Badge>
  if (label === 'good') return <Badge className="bg-emerald-600 hover:bg-emerald-600">good</Badge>
  return <Badge variant="secondary">neutral</Badge>
}
// Edit state lives here (not in LabelsTab) so typing in the popover does not
// invalidate the table's column defs, which would remount the cell mid-edit.
function LabelActions({ row: r, onSave, onDelete }: {
  row: LabelRow; onSave: (row: LabelRow, label: LabelValue, notes: string) => Promise<boolean>; onDelete: (row: LabelRow) => void
}) { const [open, setOpen] = React.useState(false)
  const [busy, setBusy] = React.useState(false)
  const [editLabel, setEditLabel] = React.useState<LabelValue>(r.label)
  const [editNotes, setEditNotes] = React.useState(r.notes ?? '')
  const save = async () => { setBusy(true)
    const ok = await onSave(r, editLabel, editNotes)
    setBusy(false)
    if (ok) setOpen(false)
  }
  return ( <div className="flex items-center justify-end gap-1">
      <Popover open={open} onOpenChange={(o) => { if (o) { setEditLabel(r.label); setEditNotes(r.notes ?? '') } setOpen(o) }}>
        <PopoverTrigger render={(props: React.ComponentPropsWithRef<'button'>) => (
          <Button {...props} variant="ghost" size="icon" aria-label="Edit label" className="h-7 w-7" title="Edit label"><Pencil className="h-3.5 w-3.5" /></Button>
        )} />
        <PopoverContent className="w-80" align="end">
          <div className="space-y-3">
            <p className="text-sm font-medium">Edit label</p>
            <Select value={editLabel} onValueChange={(v) => setEditLabel(v as LabelValue)}>
              <SelectTrigger className="text-xs"><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="good">good</SelectItem>
                <SelectItem value="neutral">neutral</SelectItem>
                <SelectItem value="bad">bad</SelectItem>
              </SelectContent>
            </Select>
            <Textarea value={editNotes} onChange={(e) => setEditNotes(e.target.value)} placeholder="Notes (optional)" className="text-xs min-h-[60px]" />
            <div className="flex justify-end gap-2">
              <Button variant="ghost" size="sm" onClick={() => setOpen(false)}>Cancel</Button>
              <Button size="sm" disabled={busy} onClick={save}>{busy ? <Loader2 className="h-3 w-3 animate-spin" /> : 'Save'}</Button>
            </div>
          </div>
        </PopoverContent>
      </Popover>
      <Button variant="ghost" size="icon" aria-label="Delete label" className="h-7 w-7 text-rose-600 hover:text-rose-700" title="Delete label" onClick={() => onDelete(r)} disabled={busy}>
        <Trash2 className="h-3.5 w-3.5" />
      </Button>
    </div>
  )
}
export function LabelsTab({ serviceId }: { serviceId: string }) { const qc = useQueryClient()
  const [error, setError] = React.useState('')
  const [pendingDelete, setPendingDelete] = React.useState<LabelRow | null>(null)
  const { pendingDeleteIds, scheduleDelete } = useDeferredDeleteWithUndo()
  const labels = useScoringLabels(serviceId)
  const saveEdit = React.useCallback(async (row: LabelRow, label: LabelValue, notes: string): Promise<boolean> => { setError('')
    try { await unwrapClient(client.PATCH('/api/services/{service_id}/scoring/labels/{label_id}', {
        params: { path: { service_id: serviceId, label_id: row.id } }, body: { label, notes }, }))
      qc.invalidateQueries({ queryKey: queryKeys.scoringLabels(serviceId) })
      return true
    } catch (e) { setError(extractApiError(e) || 'Failed to update label')
      return false
    }
  }, [qc, serviceId])
  const requestDelete = React.useCallback((row: LabelRow) => { setError(''); setPendingDelete(row) }, [])
  const remove = (row: LabelRow) => { setError('')
    setPendingDelete(null)
    const id = row.id
    scheduleDelete({ id,
      toastMessage: `Deleted label for ${row.sid}`,
      performDelete: async () => { await unwrapClient(client.DELETE('/api/services/{service_id}/scoring/labels/{label_id}', {
          params: { path: { service_id: serviceId, label_id: id } }, }))
        qc.invalidateQueries({ queryKey: queryKeys.scoringLabels(serviceId) }) }, errorMessage: (e) => extractApiError(e) || 'Failed to delete label', })
  }
  const rows = React.useMemo(() => labels.labels.filter((r) => !pendingDeleteIds.has(r.id)), [labels.labels, pendingDeleteIds])
  const { counts } = labels
  const columns = React.useMemo<ColumnDef<LabelRow>[]>(() => [ { id: 'sid', header: 'SID', accessorFn: (r) => r.sid, size: 160, enableSorting: false,
      cell: ({ row }) => ( <SessionEventsDialog
          serviceId={serviceId} sid={row.original.sid} label={row.original.label}
          trigger={<button type="button" className="text-xs font-mono hover:underline text-primary" title="View session events">{row.original.sid}</button>} />
      ), }, { id: 'label', header: 'Label', accessorFn: (r) => r.label, size: 90, enableSorting: false, cell: ({ row }) => labelBadge(row.original.label) }, ...([
      { id: 'notes' as const, header: 'Notes', size: 260, mono: false, title: true },
      { id: 'sample_url' as const, header: 'Sample URL', size: 260, mono: false, title: true },
      { id: 'sample_ip' as const, header: 'IP', size: 140, mono: true, title: false }, ].map(({ id, header, size, mono, title }): ColumnDef<LabelRow> => ({
      id, header, accessorFn: (r) => r[id] ?? '', size, enableSorting: false,
      cell: ({ row }) => <span className={mono ? 'text-xs font-mono' : 'text-xs'} title={title ? (row.original[id] ?? undefined) : undefined}>{row.original[id] || '—'}</span>,
    }))),
    { id: 'updated_at', header: 'Updated', accessorFn: (r) => r.updated_at ?? '', size: 170, enableSorting: false, cell: ({ row }) => <span className="text-xs whitespace-nowrap"><DateTimeCell iso={row.original.updated_at} /></span> },
    { id: 'actions', header: 'Actions', size: 100, enableSorting: false, cell: ({ row }) => <LabelActions row={row.original} onSave={saveEdit} onDelete={requestDelete} /> },
  ], [serviceId, saveEdit, requestDelete])
  return ( <AnalyticsCard
      title="Session labels"
      description="Human-applied labels feeding the matrix-quality evaluator (ROC-AUC). Labels are NOT used for active blocking — that's VCL's job — they only tell us whether the trained matrix can separate good from bad."
      helpContent={<LabelsHelp />} helpTitle="About Session Labels"
      isLoading={labels.isLoading} isFetching={labels.isFetching} error={labels.error} contentClassName="p-0"
      headerAction={ <div className="flex items-center gap-2 text-xs">
          <span className="text-emerald-700">good: {counts.good}</span>
          <span className="text-slate-500">·</span>
          <span className="text-slate-700">neutral: {counts.neutral}</span>
          <span className="text-slate-500">·</span>
          <span className="text-rose-700">bad: {counts.bad}</span>
        </div>
      } >
      <p className="text-[11px] text-muted-foreground italic px-4 py-2 border-b">
        Labels are keyed on <span className="font-mono">sid</span>, which rotates when
        the encrypted cookie hits its idle-expire or hard-cap (security feature). If a
        previously-labeled session disappears from view, the underlying visitor likely
        got a fresh sid — label the new one, and treat each labeled row as a snapshot
        of one session, not an identity across all of a visitor&apos;s activity.
      </p>
      {error && <p className="text-xs text-rose-600 px-4 py-2">{error}</p>}
      <DataTableReadonly columns={columns} data={rows} hideToolbar showPagination={rows.length > 50} tableCaption="Session labels" isLoading={labels.isLoading}
        emptyMessage="No labels yet. Flag a session from the dashboard or the Top Flagged table to add one." />
      <ConfirmDialog open={pendingDelete !== null} onOpenChange={(open) => { if (!open) setPendingDelete(null) }} isDangerous title="Delete label"
        description={pendingDelete ? <>Delete label for sid <span className="font-mono">{pendingDelete.sid}</span>?</> : null}
        confirmLabel="Delete" onConfirm={() => { if (pendingDelete) remove(pendingDelete) }} />
    </AnalyticsCard>
  )
}
// ─── 2. RetrainButton & RotateKeyButton ───────────────────────────────────────
export function RetrainButton({ serviceId }: { serviceId: string }) { const [open, setOpen] = React.useState(false)
  const generation = useOpenGeneration(open)
  return ( <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger render={<Button variant="outline" size="sm"><Brain className="h-4 w-4 mr-1" />Retrain matrix</Button>} />
      <RetrainDialogBody key={generation} serviceId={serviceId} onClose={() => setOpen(false)} />
    </Dialog>
  )
}
function RetrainDialogBody({ serviceId, onClose }: { serviceId: string; onClose: () => void }) { const queryClient = useQueryClient()
  const [sinceDays, setSinceDays] = React.useState(7)
  const [result, setResult] = React.useState<RetrainResult | null>(null)
  const mutation = useMutation({ mutationFn: (): Promise<RetrainResult> => unwrapClient(client.POST('/api/services/{service_id}/scoring/retrain', {
        params: { path: { service_id: serviceId }, query: { since_days: sinceDays } }, })),
    onSuccess: (data) => { setResult(data); invalidateServiceScoringQueries(queryClient, serviceId) }, })
  return ( <DialogContent className="max-w-2xl">
      <DialogHeader>
        <DialogTitle className="flex items-center gap-2"><Brain className="h-4 w-4" />Retrain scoring matrix</DialogTitle>
        <DialogDescription>
          Builds a fresh transition matrix from the last N days of DuckDB traffic and
          evaluates AUC against your labels. Saves matrix.json + publishes to FOS so
          every backend host sees the new matrix. The Wasm rebuild + Compute deploy
          is a separate step (requires Fastly CLI on your local box).
        </DialogDescription>
      </DialogHeader>
      {!result && ( <div className="space-y-4 py-2">
          <div className="space-y-1.5">
            <Label htmlFor="since-days" className="text-xs">Training window (days)</Label>
            <Input id="since-days" type="number" min={1} max={90} value={sinceDays}
              onChange={(e) => setSinceDays(Number(e.target.value) || 7)} className="text-sm" disabled={mutation.isPending} />
            <p className="text-[11px] text-muted-foreground">
              7 days is the default; bump to 30 once you have steady traffic. Larger
              windows take longer but build a more stable matrix.
            </p>
          </div>
          {mutation.isError && <p className="text-sm text-destructive">{(mutation.error as Error).message}</p>}
        </div>
      )}
      {result && ( <div className="space-y-3 py-2 text-sm">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
            {[ { label: 'Matrix version', value: result.matrix_version, mono: true },
              { label: 'Sessions trained', value: formatCount(result.sessions_trained_on ?? 0) },
              { label: 'Transitions', value: formatCount(result.transitions ?? 0) }, { label: 'Vocab size', value: formatCount(result.vocab_size ?? 0) },
            ].map((m) => ( <div key={m.label} className="p-2 border rounded-md">
                <div className="text-[10px] uppercase tracking-wider text-muted-foreground">{m.label}</div>
                <div className={`text-sm ${m.mono ? 'font-mono' : ''} font-semibold`}>{m.value}</div>
              </div>
            ))}
          </div>
          {result.rejected && ( <p className="text-xs text-muted-foreground">
              Kept {formatCount(result.rejected.kept ?? 0)} sessions · dropped {formatCount(result.rejected.too_few_events ?? 0)} too-short, {formatCount(result.rejected.too_fast ?? 0)} too-fast · {formatCount(result.rejected.routes_seen ?? 0)} routes seen
            </p>
          )}
          {result.auc_against_labels ? ( <div className="p-3 border rounded-md bg-muted/40">
              <div className="text-xs uppercase text-muted-foreground mb-1">AUC against your labels</div>
              <div className="flex items-baseline gap-2">
                <span className={`text-2xl font-mono font-semibold tabular-nums ${result.auc_against_labels.passed ? 'text-emerald-600' : 'text-amber-600'}`}>
                  {(result.auc_against_labels.auc ?? 0).toFixed(3)}
                </span>
                <span className="text-sm text-muted-foreground">
                  {result.auc_against_labels.passed ? 'PASS' : 'BELOW THRESHOLD'} (threshold {(result.auc_against_labels.threshold ?? 0).toFixed(2)})
                </span>
              </div>
              <p className="text-[11px] text-muted-foreground mt-1">
                n={result.auc_against_labels.n_good} good / {result.auc_against_labels.n_bad} bad — minimum {result.default_min_auc} to pass.
              </p>
            </div>
          ) : ( <p className="text-xs text-muted-foreground italic">AUC not computed — need at least 3 good + 3 bad labels to evaluate.</p>
          )}
          <div className="space-y-1 text-[11px] text-muted-foreground">
            <p>{result.local_matrix_saved ? '✓' : '✗'} matrix.json saved locally</p>
            <p>{result.fos_matrix_published ? '✓' : '✗'} matrix published to FOS</p>
          </div>
          {result.deploy_hint && ( <details className="text-xs">
              <summary className="cursor-pointer text-muted-foreground hover:text-foreground mb-2">Deploy the new matrix to Fastly Compute</summary>
              <DeployHintBox title="deploy command" hint={result.deploy_hint} ariaLabel="Copy deploy hint" copiedAriaLabel="Copied deploy hint" />
            </details>
          )}
        </div>
      )}
      <DialogFooter>
        {!result ? ( <>
            <Button variant="outline" onClick={onClose} disabled={mutation.isPending}>Cancel</Button>
            <Button onClick={() => mutation.mutate()} disabled={mutation.isPending}>
              {mutation.isPending ? <><Loader2 className="h-4 w-4 mr-1 animate-spin" />Training…</> : <><Brain className="h-4 w-4 mr-1" />Start retrain</>}
            </Button>
          </>
        ) : ( <Button onClick={onClose}>Close</Button>
        )}
      </DialogFooter>
    </DialogContent>
  )
}
export function RotateKeyButton({ serviceId }: { serviceId: string }) { const qc = useQueryClient()
  const [open, setOpen] = React.useState(false)
  const { value: status, flash: flashStatus, set: setStatus, clear: clearStatus } = useAutoFlash<{ kind: 'success' | 'error'; message: string }>(4000)
  const mutation = useMutation({ mutationFn: (): Promise<RotateKeyResponse> =>
      unwrapClient(client.POST('/api/services/{service_id}/scoring/rotate-key', { params: { path: { service_id: serviceId } } })), onSuccess: () => {
      flashStatus({ kind: 'success', message: 'Key rotated; previous slot updated' })
      setOpen(false)
      qc.invalidateQueries({ queryKey: ['scoring-status', serviceId] })
      qc.invalidateQueries({ queryKey: scoringConfigCompositeKey(serviceId) })
      qc.invalidateQueries({ queryKey: ['scoring-audit', serviceId] }) }, onError: (err: unknown) => {
      setStatus({ kind: 'error', message: err instanceof Error ? err.message : 'Failed to rotate key' }) }, })
  return ( <div className="inline-flex flex-col items-end gap-1">
      <Button variant="destructive" size="sm" onClick={() => { clearStatus(); setOpen(true) }}
        disabled={mutation.isPending} title="Rotate the AES key used to encrypt scoring cookies" >
        {mutation.isPending ? <Loader2 className="h-4 w-4 mr-1 animate-spin" /> : <KeyRound className="h-4 w-4 mr-1" />}
        Rotate AES key
      </Button>
      {status && (
        <p className={`text-[11px] ${status.kind === 'success' ? 'text-emerald-600' : 'text-destructive'}`} role="status">
          {status.message}
        </p>
      )}
      <ConfirmDialog open={open} onOpenChange={(o) => { if (!mutation.isPending) setOpen(o) }}
        isDangerous isPending={mutation.isPending} title="Rotate AES cookie key?"
        description={ <div className="space-y-2 text-sm">
            <p>
              Generates a new AES key and promotes it to the <strong>current</strong>{' '}
              slot. The old current key moves to <strong>previous</strong> and is still
              accepted for one grace cycle.
            </p>
            <ul className="list-disc list-inside space-y-1 text-xs">
              <li>In-flight sessions keep working — their cookies decode under the previous-slot key.</li>
              <li>New sessions get cookies encrypted with the new current key.</li>
              <li>
                <strong>Warning:</strong> rotating twice in quick succession discards the original key entirely. Cookies issued under it will be treated as{' '}
                <span className="font-mono">tampered</span> at the edge.
              </li>
            </ul>
          </div>
        }
        confirmLabel="Rotate key" cancelLabel="Cancel" onConfirm={() => mutation.mutate()} />
    </div>
  )
}
// ─── 3. ScoringHealthCard & ScorerFailOpenBreakdownCard ───────────────────────
export function ScoringHealthCard({ serviceId, sinceHours = 24 }: { serviceId: string; sinceHours?: number }) {
  const { data, isLoading, isError, error, refetch } = useScoringHealth(serviceId, sinceHours)
  if (isError) { const raw = String(error?.message || 'Unknown error')
    const friendly = /No files found that match the pattern/i.test(raw) || /IO Error/i.test(raw)
        ? 'Scoring data is still warming up — try again in a few minutes.'
        : /timed out|timeout/i.test(raw)
          ? 'The scoring service took too long to respond. Retry, or check the scorer Compute service is reachable.'
          : raw
    return (
      <AnalyticsCard title="Scoring Health" description={`Operational metrics for the last ${sinceHours}h.`}>
        <CardErrorState icon={<AlertTriangle className="h-4 w-4" />} title="Scoring health unavailable" message={friendly} onRetry={() => refetch()} />
      </AnalyticsCard>
    )
  }
  if (isLoading || !data) { return (
      <AnalyticsCard title="Scoring Health" description={`Operational metrics for the last ${sinceHours}h.`}>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
          {['m1', 'm2', 'm3', 'm4', 'm5', 'm6'].map((k) => <Skeleton key={k} className="h-20 w-full" />)}
        </div>
      </AnalyticsCard>
    )
  }
  const fireRateTone: 'default' | 'warn' = (data.fire_rate_pct ?? 0) < 20 ? 'warn' : 'default'
  const failRate = data.fail_open_rate_pct ?? null
  const scorerErrors = data.scorer_errors ?? 0
  const errorsTone: 'default' | 'warn' | 'good' =
    scorerErrors === 0 ? 'good' : failRate != null ? (failRate > 2.5 ? 'warn' : 'default') : scorerErrors > 10 ? 'warn' : 'default'
  const lat = data.latency
  const rttP95Us = lat?.rtt_p95_us ?? null
  const fmtMs = (us: number | null | undefined) => us == null ? '—' : `${(us / 1000).toFixed(us < 10_000 ? 1 : 0)}ms`
  const fmtUs = (us: number | null | undefined) => us == null ? '—' : us >= 1_000 ? `${(us / 1000).toFixed(1)}ms` : `${us}µs`
  const latencyTone: 'default' | 'warn' = rttP95Us != null && rttP95Us / 1000 > 80 ? 'warn' : 'default'
  const topReasons = data.top_reasons ?? []
  return ( <AnalyticsCard
      title="Scoring Health"
      description={`Snapshot for the last ${sinceHours}h — fire rate, score distribution, and top reasons.`}
      helpContent={<ScoringHealthHelp />} >
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
        <HealthMetric icon={Activity} label="Fire Rate" value={`${(data.fire_rate_pct ?? 0).toFixed(1)}%`} sub={`${formatCount(data.scored_rows ?? 0)} of ${formatCount(data.total_edge_rows ?? 0)} edge rows`} tone={fireRateTone} />
        <HealthMetric icon={Gauge} label="Avg Score" value={(data.avg_score ?? 0).toFixed(1)} sub={`p50 ${(data.p50_score ?? 0).toFixed(0)} · p95 ${(data.p95_score ?? 0).toFixed(0)} · max ${data.max_score ?? 0}`} />
        <HealthMetric icon={Users} label="Sessions" value={formatCount(data.distinct_sids ?? 0)} sub="distinct sids seen" />
        <HealthMetric icon={XCircle} label="Scorer Errors" value={formatCount(scorerErrors)} sub={failRate != null ? `${failRate.toFixed(2)}% of edge rows` : 'fail-open + auth fail rows'} tone={errorsTone} />
        <HealthMetric icon={Clock} label="Scorer Latency (p95 rtt)" value={rttP95Us != null ? fmtMs(rttP95Us) : '—'}
          sub={rttP95Us != null ? `rtt: p50 ${fmtMs(lat?.rtt_p50_us)} · p99 ${fmtMs(lat?.rtt_p99_us)} · max ${fmtMs(lat?.rtt_max_us)}\nexec: p50 ${fmtUs(lat?.exec_p50_us)} · p95 ${fmtUs(lat?.exec_p95_us)}` : 'awaiting re-provision'}
          tone={latencyTone} />
        <HealthMetric icon={AlertTriangle} label="Top Reason" value={topReasons[0]?.reason ?? '—'} sub={topReasons[0] ? `${formatCount(topReasons[0].count ?? 0)} hits` : 'no scored rows yet'} />
      </div>
      {topReasons.length > 0 && ( <div className="mt-4 space-y-1">
          <div className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">Score Reason Breakdown</div>
          <div className="flex flex-wrap gap-2">
            {topReasons.map((r) => ( <div key={r.reason ?? ''} className="flex items-center gap-1.5 px-2 py-1 rounded-md border bg-muted/40 text-xs">
                <span className="font-mono">{r.reason}</span>
                <span className="text-muted-foreground tabular-nums">{formatCount(r.count ?? 0)}</span>
              </div>
            ))}
          </div>
        </div>
      )}
      {data.matrix_staleness?.is_stale && ( <div className="mt-4 p-3 border border-amber-300 bg-amber-50/60 rounded-md text-xs flex items-start gap-2">
          <AlertTriangle className="h-4 w-4 text-amber-600 mt-0.5 flex-none" />
          <div className="space-y-0.5">
            <div className="font-semibold text-amber-900">Matrix may be stale</div>
            <div className="text-amber-800">
              {(data.matrix_staleness.l2_high_pct ?? 0).toFixed(1)}% of L2-evaluated sessions
              {' '}({formatCount(data.matrix_staleness.l2_high_count ?? 0)} of{' '}
              {formatCount(data.matrix_staleness.l2_evaluated ?? 0)}) are scoring
              ≥50 — above the {data.matrix_staleness.threshold_pct ?? 0}% threshold.
              The matrix is treating too much real traffic as rare. Click
              <strong className="px-0.5">Retrain matrix</strong> in the header to refresh it.
            </div>
          </div>
        </div>
      )}
    </AnalyticsCard>
  )
}
export function ScorerFailOpenBreakdownCard({ serviceId, sinceHours = 24 }: { serviceId: string; sinceHours?: number }) {
  const { data, isLoading, isError, error, refetch } = useScoringHealth(serviceId, sinceHours)
  const buckets = data?.fail_open_breakdown ?? []
  const total = buckets.reduce((acc, b) => acc + (b.count ?? 0), 0)
  const max = buckets.reduce((acc, b) => Math.max(acc, b.count ?? 0), 0)
  return ( <AnalyticsCard
      title={`Fail-open breakdown — last ${sinceHours}h`}
      description="Fail-opens grouped by reason/status — why the scorer let requests through unscored."
      helpContent={<ScorerFailOpenHelp />} helpTitle="About Fail-open Breakdown" >
      {isError ? (
        <CardErrorState icon={<AlertTriangle className="h-4 w-4" />} title="Fail-open breakdown unavailable" message={(error as Error)?.message || 'Unknown error'} onRetry={() => refetch()} />
      ) : isLoading || !data ? ( <div className="space-y-2">{['f1', 'f2', 'f3'].map((k) => <Skeleton key={k} className="h-10 w-full" />)}</div>
      ) : buckets.length === 0 ? ( <div className="flex items-center gap-2 rounded-md border border-emerald-200 bg-emerald-50/60 px-3 py-3 text-sm text-emerald-800">
          <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-600" />
          <span>No fail-opens in the last {sinceHours}h — every routed request was scored.</span>
        </div>
      ) : ( <div className="space-y-3">
          <div className="text-xs text-muted-foreground">
            <span className="font-mono tabular-nums text-foreground">{formatCount(total)}</span>{' '} fail-open {total === 1 ? 'row' : 'rows'} across{' '}
            <span className="font-mono tabular-nums text-foreground">{buckets.length}</span>{' '} {buckets.length === 1 ? 'class' : 'classes'}.
          </div>
          <div className="space-y-1.5">{buckets.map((b) => <FailOpenRow key={b.reason ?? ''} bucket={b} max={max} />)}</div>
        </div>
      )}
    </AnalyticsCard>
  )
}
function FailOpenRow({ bucket, max }: { bucket: FailOpenBucket; max: number }) { const { label, status, Icon } = describeFailOpen(bucket.reason ?? '')
  const widthPct = max > 0 ? Math.max(4, Math.round(((bucket.count ?? 0) / max) * 100)) : 0
  return ( <div className="flex items-center gap-3 rounded-md border bg-card px-3 py-2 text-xs">
      <Icon className="h-4 w-4 shrink-0 text-destructive/80" />
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <span className="font-medium text-foreground">{label}</span>
          {status != null && <span className="rounded bg-destructive/10 px-1.5 py-0.5 font-mono text-[10px] font-semibold text-destructive">{status}</span>}
        </div>
        <div className="mt-1 h-1.5 w-full overflow-hidden rounded-full bg-border/50">
          <div className="h-full rounded-full bg-rose-500/50" style={{ width: `${widthPct}%` }} />
        </div>
        <code className="mt-1 block truncate text-[10px] text-muted-foreground">{bucket.reason}</code>
      </div>
      <span className="shrink-0 font-mono tabular-nums text-foreground">{formatCount(bucket.count ?? 0)}</span>
    </div>
  )
}
function describeFailOpen(reason: string): { label: string; status: string | null; Icon: React.ComponentType<{ className?: string }> } {
  const m = /^compute-unavailable-(\d+|.+)$/.exec(reason)
  if (m) { const status = m[1]
    if (status === '503') return { label: 'Scorer timeout / unavailable', status, Icon: Clock }
    if (status === '500') return { label: 'Scorer error', status, Icon: ServerCrash }
    if (status === '401') return { label: 'Auth rejected', status, Icon: Lock }
    return { label: 'Scorer unavailable', status, Icon: ServerCrash }
  }
  if (reason === 'internal-error-keys') return { label: 'Key / KV store load failure', status: null, Icon: KeyRound }
  if (reason.startsWith('internal-error')) return { label: 'Internal scorer error', status: null, Icon: ServerCrash }
  if (reason === 'unauthorized') return { label: 'Unauthorized', status: null, Icon: Lock }
  return { label: reason, status: null, Icon: XCircle }
}
// ─── 4. ExcludeRegexCard (zod + react-hook-form) ──────────────────────────────
const EXCLUDE_REGEX_DESCRIPTION =
  'Requests whose URL matches this regex are NOT sent to the scoring Compute service. The default skips common static-asset extensions; override to scope scoring to specific traffic patterns.'
const excludeRegexFormSchema = z.object({ regex: z.string().max(2048) })
type ExcludeRegexFormValues = z.infer<typeof excludeRegexFormSchema>
type LintResult = | { regex: string; ok: true; lint_warnings: string[] }
  | { regex: string; ok: false; error: string; reason: string }
export function ExcludeRegexCard({ serviceId }: { serviceId: string }) { const queryClient = useQueryClient()
  const form = useForm<ExcludeRegexFormValues>({ defaultValues: { regex: '' } })
  const draft = form.watch('regex')
  const [confirmOpen, setConfirmOpen] = React.useState(false)
  const [lintResult, setLintResult] = React.useState<LintResult | null>(null)
  const [lintPending, setLintPending] = React.useState(false)
  const { data, isLoading, error } = useScoringQuery( ['scoring-exclude-regex', serviceId],
    () => client.GET('/api/services/{service_id}/scoring/exclude-regex', { params: { path: { service_id: serviceId } } }), { staleTime: 60_000 }, )
  const initialisedServiceRef = React.useRef<string | null>(null)
  React.useEffect(() => { if (data && initialisedServiceRef.current !== serviceId) { form.reset({ regex: data.current || data.effective || '' })
      initialisedServiceRef.current = serviceId
    }
  }, [data, serviceId, form])
  const isDirty = data ? draft !== (data.current || data.effective) : false
  const isResetToDefault = data ? draft === data.default && !data.is_default : false
  const saveMut = useMutation({ mutationFn: async (regex: string) => { const parsed = excludeRegexFormSchema.safeParse({ regex })
      const validRegex = parsed.success ? parsed.data.regex : regex
      return unwrapClient(client.PUT('/api/services/{service_id}/scoring/exclude-regex', { params: { path: { service_id: serviceId }, query: { confirm: true } },
        body: { regex: validRegex }, })) }, onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['scoring-exclude-regex', serviceId] })
      queryClient.invalidateQueries({ queryKey: scoringConfigCompositeKey(serviceId) })
      queryClient.invalidateQueries({ queryKey: ['scoring-audit', serviceId] })
      setConfirmOpen(false) }, })
  const runLintCheck = React.useCallback(async (regex: string) => { if (regex.trim() === '') { setLintResult(null); return }
    if (lintResult && lintResult.regex === regex) return
    setLintPending(true)
    try { const { data: r, error: errBody, response } = await client.POST('/api/services/{service_id}/scoring/exclude-regex/validate', {
        params: { path: { service_id: serviceId } }, body: { regex }, })
      if (!response.ok || !r) {
        setLintResult({ regex, ok: false, error: errBody ? extractApiError(errBody) : `HTTP ${response.status}`, reason: 'http_error' })
        return
      }
      setLintResult(r.ok ? { regex, ok: true, lint_warnings: r.lint_warnings || [] } : { regex, ok: false, error: r.error ?? '', reason: r.reason ?? '' })
    } catch (e) { setLintResult({ regex, ok: false, error: e instanceof Error ? e.message : String(e), reason: 'network_error' })
    } finally { setLintPending(false)
    }
  }, [serviceId, lintResult])
  const handleDraftChange = (v: string) => { form.setValue('regex', v, { shouldDirty: true })
    if (lintResult && lintResult.regex !== v) setLintResult(null)
  }
  if (isLoading || !data) { return ( <AnalyticsCard
        title="URL exclusion regex" icon={<Filter className="h-4 w-4" />}
        description={EXCLUDE_REGEX_DESCRIPTION}
        error={!isLoading ? (error as (Error & { status?: number }) | null) : undefined} >
        {isLoading ? <Skeleton className="h-32 w-full" /> : ( <Alert variant="destructive">
            <AlertTriangle className="h-4 w-4" />
            <AlertDescription>Failed to load current exclusion regex.</AlertDescription>
          </Alert>
        )}
      </AnalyticsCard>
    )
  }
  const activeLint = !lintPending && lintResult && lintResult.regex === draft ? lintResult : null
  return ( <AnalyticsCard
      title="URL exclusion regex" icon={<Filter className="h-4 w-4" />}
      description={EXCLUDE_REGEX_DESCRIPTION} error={error as (Error & { status?: number }) | null} >
      <div className="space-y-4">
        <div className="space-y-1.5">
          <Label htmlFor="exclude-regex-textarea" className="text-xs font-semibold">
            Exclusion regex{' '}
            <span className="text-muted-foreground">({data.is_default ? 'currently default' : 'currently custom override'})</span>
          </Label>
          <Textarea id="exclude-regex-textarea" value={draft} onChange={(e) => handleDraftChange(e.target.value)} onBlur={(e) => runLintCheck(e.target.value)}
            className="font-mono text-xs min-h-24" spellCheck={false} autoComplete="off" />
          <p className="text-[10px] text-muted-foreground">
            VCL pattern compiled by Fastly&apos;s regex engine (RE2). No quotes / control chars; max 2 KB.
            Hit <span className="font-semibold">Reset to default</span> to drop any custom override.
            Lint runs on blur and on publish.
          </p>
        </div>
        {lintPending && ( <div className="flex items-center gap-2 text-[11px] text-muted-foreground">
            <Loader2 className="h-3 w-3 animate-spin" /> Linting regex…
          </div>
        )}
        {activeLint && !activeLint.ok && ( <Alert variant="destructive">
            <AlertTriangle className="h-4 w-4" />
            <AlertDescription className="text-xs">
              <strong>Lint error ({activeLint.reason}):</strong>
              <div className="font-mono whitespace-pre-wrap mt-1">{activeLint.error}</div>
            </AlertDescription>
          </Alert>
        )}
        {activeLint && activeLint.ok && activeLint.lint_warnings.length > 0 && ( <Alert>
            <AlertTriangle className="h-4 w-4" />
            <AlertDescription className="text-xs">
              <strong>Lint warnings (non-blocking):</strong>
              <LintWarningsList warnings={activeLint.lint_warnings} />
            </AlertDescription>
          </Alert>
        )}
        {activeLint && activeLint.ok && activeLint.lint_warnings.length === 0 && isDirty && ( <div className="flex items-center gap-2 text-[11px] text-emerald-700">
            <Check className="h-3 w-3" /> Lint clean — ready to publish.
          </div>
        )}
        {saveMut.isError && ( <Alert variant="destructive">
            <AlertTriangle className="h-4 w-4" />
            <AlertDescription className="text-xs whitespace-pre-wrap font-mono">{(saveMut.error as Error).message}</AlertDescription>
          </Alert>
        )}
        {saveMut.isSuccess && ( <Alert>
            <Check className="h-4 w-4" />
            <AlertDescription className="text-xs">
              {saveMut.data.message}
              {saveMut.data.lint_warnings && saveMut.data.lint_warnings.length > 0 && ( <div className="mt-2">
                  <strong>Lint warnings (non-blocking):</strong>
                  <LintWarningsList warnings={saveMut.data.lint_warnings} />
                </div>
              )}
            </AlertDescription>
          </Alert>
        )}
        <div className="flex items-center justify-end gap-2 pt-2 border-t">
          <Button variant="outline" size="sm" onClick={() => form.setValue('regex', data.default ?? '', { shouldDirty: true })}
            disabled={(data.is_default ?? false) && draft === ''} className="text-xs" >
            <RotateCcw className="h-3 w-3 mr-1" /> Reset to default
          </Button>
          <Button size="sm" onClick={() => setConfirmOpen(true)} disabled={!isDirty || saveMut.isPending} className="text-xs">
            {saveMut.isPending ? <><Loader2 className="h-3 w-3 mr-1 animate-spin" />Publishing…</> : isResetToDefault ? 'Reset & publish' : 'Save & publish'}
          </Button>
        </div>
      </div>
      <ConfirmDialog open={confirmOpen} onOpenChange={setConfirmOpen} title="Publish updated scoring exclusion regex?" description={ isResetToDefault
            ? 'This will revert this service to the built-in default static-asset regex and activate a new VCL version on the Fastly service. Effective at the edge after activation propagates (~5-15s).'
            : "This will clone the active Fastly version, swap the scoring recv snippet to use your custom regex, validate via falco + Fastly's compiler, and activate. Effective at the edge after activation propagates (~5-15s). A bad regex could disable scoring entirely or DoS Compute — double-check the value above."
        }
        confirmLabel="Publish" isDangerous isPending={saveMut.isPending}
        onConfirm={() => saveMut.mutate(draft)} />
    </AnalyticsCard>
  )
}
// ─── 5. MatrixVersionsCard & AuditLogTab ──────────────────────────────────────
export function MatrixVersionsCard({ serviceId }: { serviceId: string }) { const qc = useQueryClient()
  const [pendingVersion, setPendingVersion] = React.useState<string | null>(null)
  const [restoreResult, setRestoreResult] = React.useState<RestoreResponse | null>(null)
  const { data, isLoading, isFetching, isError, error } = useScoringQuery( ['scoring-matrix-versions', serviceId],
    () => client.GET('/api/services/{service_id}/scoring/matrix-versions', { params: { path: { service_id: serviceId } } }), )
  const restore = useMutation({ mutationFn: (version: string): Promise<RestoreResponse> =>
      unwrapClient(client.POST('/api/services/{service_id}/scoring/matrix-versions/{version}/restore', {
        params: { path: { service_id: serviceId, version }, query: { confirm: true } }, })),
    onSuccess: (res) => { setRestoreResult(res); setPendingVersion(null); invalidateServiceScoringQueries(qc, serviceId) }, })
  const currentVersion = data?.current_version ?? null
  const restorePending = restore.isPending
  const columns = React.useMemo<ColumnDef<MatrixVersion>[]>(() => [ {
      id: 'version', header: 'Version', accessorFn: (v) => v.version ?? '', size: 260, enableSorting: false, cell: ({ row }) => (
        <div className="flex items-center gap-2 font-mono text-xs">
          <span>{row.original.version}</span>
          {currentVersion === row.original.version && <Badge variant="default" className="text-[10px]">current</Badge>}
        </div>
      ), }, { id: 'last_modified', header: 'Last modified', accessorFn: (v) => v.last_modified ?? '', size: 200, enableSorting: false,
      cell: ({ row }) => <span className="text-xs text-muted-foreground">{formatDeterministicUTC(row.original.last_modified)}</span>, }, {
      id: 'size_bytes', header: 'Size', accessorFn: (v) => v.size_bytes ?? 0, size: 110, enableSorting: false,
      cell: ({ row }) => <span className="text-xs font-mono tabular-nums">{formatBytes(row.original.size_bytes ?? 0)}</span>, }, {
      id: 'actions', header: 'Actions', size: 130, enableSorting: false, cell: ({ row }) => ( <div className="flex justify-end">
          <Button variant="outline" size="sm" className="h-7 text-xs text-destructive hover:bg-destructive/10 hover:text-destructive border-destructive/30"
            disabled={currentVersion === row.original.version || restorePending}
            onClick={() => setPendingVersion(row.original.version ?? null)} >
            <RotateCcw className="h-3 w-3 mr-1" /> Restore
          </Button>
        </div>
      ), }, ], [currentVersion, restorePending])
  if (isError) {
    return <CardErrorState icon={<Info className="h-4 w-4" />} title="Failed to load matrix versions" message={(error as Error)?.message || 'Unknown error'} />
  }
  const versions = data?.versions ?? []
  return ( <AnalyticsCard
      title="Matrix history" icon={<History className="h-4 w-4" />}
      description="Restore a prior trained matrix. Edge Wasm keeps the embedded matrix until you re-run deploy_wasm.sh."
      helpContent={<MatrixHistoryHelp />} helpTitle="About Matrix History"
      isLoading={isLoading} isFetching={isFetching} contentClassName="p-0" >
      {isLoading ? ( <div className="p-4 space-y-2"><SkeletonGrid count={3} height="2.5rem" className="rounded-md" /></div>
      ) : versions.length === 0 ? (
        <div className="p-6 text-center text-sm text-muted-foreground">No prior matrix versions saved yet. Versions accumulate as you retrain.</div>
      ) : ( <DataTableReadonly columns={columns} data={versions} hideToolbar showPagination={versions.length > 50} tableCaption="Matrix versions" />
      )}
      {restoreResult && ( <div className="border-t p-4 space-y-2">
          <div className="text-xs font-semibold">Restored matrix version <span className="font-mono">{restoreResult.restored_version}</span></div>
          <DeployHintBox title="deploy hint" hint={restoreResult.deploy_hint ?? ''} ariaLabel="Copy deploy command" copiedAriaLabel="Copied deploy command" buttonTitle="Copy to clipboard" timeoutMs={1500} />
        </div>
      )}
      <ConfirmDialog open={pendingVersion !== null} onOpenChange={(open) => { if (!open) setPendingVersion(null) }}
        title={`Restore matrix version ${pendingVersion ?? ''}?`}
        description={ <div className="space-y-2 text-sm">
            <p>This will:</p>
            <ul className="list-disc list-inside space-y-1 text-xs">
              <li>Snapshot the current matrix.json to <span className="font-mono">pre-restore-{`{epoch_ms}`}</span> (reversible).</li>
              <li>Make the chosen version live for the admin AUC immediately.</li>
              <li><strong>EDGE behavior:</strong> the Wasm at the edge keeps the embedded matrix until <span className="font-mono">deploy_wasm.sh</span> re-runs.</li>
            </ul>
            {restore.isError && <p className="text-destructive text-xs">{(restore.error as Error).message}</p>}
          </div>
        }
        confirmLabel="Restore version" cancelLabel="Cancel"
        isDangerous isPending={restore.isPending}
        onConfirm={() => { if (pendingVersion) restore.mutate(pendingVersion) }} />
    </AnalyticsCard>
  )
}
const AUDIT_LIMIT = 200
function detailsToString(details: unknown): string { if (details == null) return ''
  if (typeof details === 'string') return details
  try { return JSON.stringify(details) } catch { return String(details) }
}
const AUDIT_COLUMNS: ColumnDef<AuditRow>[] = [ { id: 'timestamp', header: 'Timestamp', accessorFn: (r) => r.timestamp ?? '', size: 140, enableSorting: false,
    cell: ({ row }) => <span className="text-xs whitespace-nowrap text-muted-foreground" title={row.original.timestamp ?? undefined}>{formatTimeAgo(row.original.timestamp ?? '') || row.original.timestamp}</span>,
  }, { id: 'action', header: 'Action', accessorFn: (r) => r.action, size: 200, enableSorting: false,
    cell: ({ row }) => <Badge variant="secondary" className="font-mono text-[10px]">{row.original.action}</Badge>, }, {
    id: 'actor', header: 'Actor', accessorFn: (r) => r.actor ?? '', size: 160, enableSorting: false,
    cell: ({ row }) => <span className="text-xs font-mono">{row.original.actor || '—'}</span>, }, {
    id: 'details', header: 'Details', accessorFn: (r) => detailsToString(r.details), size: 480, enableSorting: false, cell: ({ row }) => {
      const details = detailsToString(row.original.details)
      return <span className="text-xs font-mono" title={details || undefined}>{details ? (details.length <= 60 ? details : `${details.slice(0, 60)}…`) : '—'}</span>
    }, }, ]
export function AuditLogTab({ serviceId }: { serviceId: string }) { const query = useScoringQuery( ['scoring-audit', serviceId, AUDIT_LIMIT],
    () => client.GET('/api/services/{service_id}/scoring/audit', { params: { path: { service_id: serviceId }, query: { limit: AUDIT_LIMIT } } }),
    { staleTime: 30_000 }, )
  if (query.isError) { return (
      <AnalyticsCard title="Audit log" icon={<ScrollText className="h-4 w-4" />} helpContent={<AuditLogHelp />} helpTitle="About Scoring Audit Log">
        <CardErrorState icon={<Info className="h-4 w-4" />} title="Failed to load audit log" message={query.error?.message || 'Unknown error'} onRetry={() => query.refetch()} variant="stacked" />
      </AnalyticsCard>
    )
  }
  const rows = query.data?.audit ?? []
  return ( <AnalyticsCard
      title="Audit log" icon={<ScrollText className="h-4 w-4" />}
      description="Recent operator actions on this service's scoring config (most-recent first)."
      helpContent={<AuditLogHelp />} helpTitle="About Scoring Audit Log"
      isLoading={query.isLoading} isFetching={query.isFetching} contentClassName="p-0" >
      {query.isLoading ? ( <div className="p-4 space-y-2"><SkeletonGrid count={3} height="2.5rem" className="rounded-md" /></div>
      ) : ( <DataTableReadonly
          columns={AUDIT_COLUMNS} data={rows} hideToolbar showPagination={rows.length > 50}
          tableCaption="Scoring audit log"
          emptyMessage="No audit events yet. Mutations to scoring (enable, retrain, rotate key, etc.) will appear here." />
      )}
    </AnalyticsCard>
  )
}
// ─── ThresholdSliderControls ─────────────────────────────────────────────────
type StatusCodeMeta = components['schemas']['ScoringEnforceStatusCodeState']
type CommittedThreshold = components['schemas']['ScoringThresholdState']
type EnforceThreshold = components['schemas']['ScoringEnforceThresholdResponse']
interface ThresholdSliderControlsProps {
  thresholdRaw: number; onThresholdRawChange: (value: number) => void; committed: CommittedThreshold | undefined
  enforce: EnforceThreshold | undefined; statusCode: StatusCodeMeta | undefined; effectiveStatusCode: number
  isAlreadyCommitted: boolean; isEnforcingThis: boolean; commitPending: boolean; enforcePending: boolean; statusCodePending: boolean
  codeDraft: string; onCodeDraftChange: (value: string) => void; codeDraftValid: boolean; codeDraftIsDirty: boolean; codeDraftNum: number
  onCommitClick: () => void; onEnforceClick: () => void; onApplyStatusCode: () => void; onResetStatusCode: () => void
}
export function ThresholdSliderControls({
  thresholdRaw, onThresholdRawChange, committed, enforce, statusCode, effectiveStatusCode, isAlreadyCommitted,
  isEnforcingThis, commitPending, enforcePending, statusCodePending, codeDraft, onCodeDraftChange, codeDraftValid,
  codeDraftIsDirty, codeDraftNum, onCommitClick, onEnforceClick, onApplyStatusCode, onResetStatusCode,
}: ThresholdSliderControlsProps) {
  return (
    <div className="space-y-2">
      <div className="flex items-baseline justify-between">
        <label className="text-xs font-medium text-muted-foreground">
          Score threshold
          {committed?.threshold != null && <span className="ml-2 text-[10px] text-muted-foreground">· committed: <span className="font-mono">{committed.threshold}</span></span>}
        </label>
        <div className="flex items-center gap-2">
          <span className="font-mono text-lg font-semibold tabular-nums">{thresholdRaw}</span>
          <Button variant={isAlreadyCommitted ? 'outline' : 'default'} size="sm" disabled={commitPending || isAlreadyCommitted} onClick={onCommitClick} className="h-7 text-xs" title="Persist this as your committed threshold (preview only — does NOT push to Compute)">
            {commitPending ? <Loader2 className="h-3 w-3 animate-spin mr-1" /> : isAlreadyCommitted ? <Check className="h-3 w-3 mr-1" /> : null}
            {isAlreadyCommitted ? 'Committed' : 'Commit'}
          </Button>
          <Button variant={isEnforcingThis ? 'outline' : 'destructive'} size="sm" disabled={enforcePending} onClick={onEnforceClick} className="h-7 text-xs" title={isEnforcingThis ? 'Currently ENFORCING this threshold. Click to disable enforcement.' : `Push this threshold to Compute. Live requests with score >= threshold will be blocked (HTTP ${effectiveStatusCode}).`}>
            {enforcePending ? <Loader2 className="h-3 w-3 animate-spin mr-1" /> : isEnforcingThis ? <ShieldOff className="h-3 w-3 mr-1" /> : <ShieldCheck className="h-3 w-3 mr-1" />}
            {isEnforcingThis ? 'Disable' : 'Enforce'}
          </Button>
        </div>
      </div>
      {enforce?.enforced && (
        <div className="text-[10px] text-destructive">
          ⚠ LIVE: enforcing at threshold <span className="font-mono">{enforce.threshold}</span> — requests with score ≥ threshold are returning HTTP <span className="font-mono">{effectiveStatusCode}</span>.
        </div>
      )}
      <div className="flex items-center gap-2 flex-wrap text-[11px]">
        <label className="text-muted-foreground" htmlFor="enforce-status-code">Enforce response code:</label>
        <input id="enforce-status-code" type="number" min={statusCode?.min ?? 400} max={statusCode?.max ?? 599} step={1} value={codeDraft} onChange={(e) => onCodeDraftChange(e.target.value)} disabled={statusCodePending} className="h-6 w-16 rounded border bg-background px-1.5 text-[11px] font-mono" title="Any HTTP 4xx/5xx code (e.g. 403 Forbidden, 429 Too Many Requests, 451 Legal, 503 Service Unavailable). Reason phrase auto-mapped from the HTTP standard." aria-label="Enforce response code" />
        <Button size="sm" variant={codeDraftIsDirty ? 'default' : 'outline'} className="h-6 text-[11px]" disabled={!codeDraftIsDirty || statusCodePending} onClick={onApplyStatusCode} title={codeDraftIsDirty ? `Re-deploy the enforce snippet so flagged requests return HTTP ${codeDraftNum}` : 'No change to publish'}>Apply</Button>
        {statusCode && !statusCode.is_default && <button type="button" disabled={statusCodePending} onClick={onResetStatusCode} className="text-[10px] text-muted-foreground underline hover:text-foreground" title={`Reset to default (${statusCode.default})`}>reset</button>}
        {statusCodePending && <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />}
        {codeDraft !== '' && !codeDraftValid && <span className="text-[10px] text-destructive">must be {statusCode?.min ?? 400}–{statusCode?.max ?? 599}</span>}
      </div>
      <Slider value={[thresholdRaw]} onValueChange={(v) => onThresholdRawChange(v[0] ?? 75)} min={0} max={100} step={5} className="w-full" aria-label="Score threshold" />
      <div className="flex justify-between text-[10px] text-muted-foreground tabular-nums"><span>0 (flag everything)</span><span>50</span><span>100 (flag nothing)</span></div>
    </div>
  )
}
// ─── ThresholdSlider ─────────────────────────────────────────────────────────
export type BucketCounts = WithRequired<components['schemas']['ScoringThresholdBucket'], 'total' | 'good' | 'bad' | 'unlabeled'>
type _BucketCountsParity = Expect<WireParity<BucketCounts, components['schemas']['ScoringThresholdBucket']>>
interface ThresholdMatrixProps { flagged: BucketCounts; passed: Omit<BucketCounts, 'total'> }
export function ThresholdMatrix({ flagged, passed }: ThresholdMatrixProps) {
  return (
    <div className="grid grid-cols-2 gap-3">
      <Bucket title="Would FLAG" total={flagged.total} good={flagged.good} bad={flagged.bad} unlabeled={flagged.unlabeled} tone="warn" />
      <Bucket title="Would PASS" total={passed.good + passed.bad + passed.unlabeled} good={passed.good} bad={passed.bad} unlabeled={passed.unlabeled} tone="good" />
    </div>
  )
}
function Bucket({ title, total, good, bad, unlabeled, tone }: { title: string; total: number; good: number; bad: number; unlabeled: number; tone: 'warn' | 'good' }) {
  const tint = tone === 'warn' ? 'border-amber-300 bg-amber-50/50' : 'border-emerald-300 bg-emerald-50/40'
  return (
    <div className={`p-3 border rounded-md ${tint}`}>
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground">{title}</div>
      <div className="text-xl font-mono font-semibold tabular-nums">{formatCount(total)}</div>
      <div className="mt-1 space-y-0.5 text-[11px]">
        <div className="flex justify-between"><span className="text-emerald-700">good</span><span className="font-mono tabular-nums">{formatCount(good)}</span></div>
        <div className="flex justify-between"><span className="text-destructive">bad</span><span className="font-mono tabular-nums">{formatCount(bad)}</span></div>
        <div className="flex justify-between text-muted-foreground"><span>unlabeled</span><span className="font-mono tabular-nums">{formatCount(unlabeled)}</span></div>
      </div>
    </div>
  )
}
interface ThresholdPreviewStatsProps { precision: number | null; recall: number | null; totalScoredSessions: number; sinceHours: number }
export function ThresholdPreviewStats({ precision, recall, totalScoredSessions, sinceHours }: ThresholdPreviewStatsProps) {
  return (
    <>
      <div className="grid grid-cols-2 gap-3 text-xs">
        <Stat label="Precision" value={precision != null ? formatPercent(precision, { decimals: 1 }) : '—'} hint="of labeled flagged sessions, how many are bad" />
        <Stat label="Recall" value={recall != null ? formatPercent(recall, { decimals: 1 }) : '—'} hint="of all labeled-bad sessions, how many got flagged" />
      </div>
      <p className="text-[11px] text-muted-foreground italic">
        {formatCount(totalScoredSessions)} distinct scored sessions in the last {sinceHours}h. Precision/recall only count sessions you&apos;ve labeled — the &quot;unlabeled&quot; tally is everything else.
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
  Omit<WithRequiredNullable<WithRequired<components['schemas']['ScoringThresholdPreviewResponse'], 'threshold' | 'since_hours' | 'total_scored_sessions'>, 'precision' | 'recall'>, 'flagged' | 'passed'> & {
    flagged: BucketCounts; passed: Omit<BucketCounts, 'total'>
  }
>
type _ThresholdPreviewParity = Expect<WireParity<ThresholdPreviewResponse, components['schemas']['ScoringThresholdPreviewResponse']>>
interface ThresholdSliderProps { serviceId: string; sinceHours?: number }
export function ThresholdSlider({ serviceId, sinceHours = 24 }: ThresholdSliderProps) {
  const queryClient = useQueryClient()
  const [thresholdRaw, setThresholdRaw] = React.useState(75)
  const threshold = useDebounce(thresholdRaw, 150)
  const [pendingAction, setPendingAction] = React.useState<{ action: 'commit' | 'enforce' | 'disable'; threshold: number } | { action: 'change-status-code'; statusCode: number } | null>(null)
  const { data: rawPreviewData, isLoading, error } = useScoringQuery(
    ['scoring-threshold-preview', serviceId, threshold, sinceHours],
    () => client.GET('/api/services/{service_id}/scoring/threshold-preview', { params: { path: { service_id: serviceId }, query: { threshold, since_hours: sinceHours } } }),
    { staleTime: 30_000 },
  )
  const data = rawPreviewData as ThresholdPreviewResponse | undefined
  const { data: committed } = useScoringQuery(
    ['scoring-threshold-committed', serviceId],
    () => client.GET('/api/services/{service_id}/scoring/threshold', { params: { path: { service_id: serviceId } } }),
    { staleTime: 30_000 },
  )
  const [syncedServiceId, setSyncedServiceId] = React.useState<string | null>(null)
  if (syncedServiceId !== serviceId && committed?.threshold != null) {
    setSyncedServiceId(serviceId)
    setThresholdRaw(committed.threshold)
  }
  const invalidateThresholdKeys = (firstKey: string) => {
    for (const key of [[firstKey, serviceId], ['scoring-status', serviceId], scoringConfigCompositeKey(serviceId), ['scoring-audit', serviceId]]) {
      queryClient.invalidateQueries({ queryKey: key })
    }
  }
  const commitMutation = useMutation({
    mutationFn: async (newThreshold: number | null) =>
      unwrapClient(client.PUT('/api/services/{service_id}/scoring/threshold', { params: { path: { service_id: serviceId } }, body: { threshold: newThreshold } })),
    onSuccess: () => invalidateThresholdKeys('scoring-threshold-committed'),
  })
  const isAlreadyCommitted = committed?.threshold === thresholdRaw
  const { data: enforce } = useScoringQuery(
    ['scoring-enforce-threshold', serviceId],
    () => client.GET('/api/services/{service_id}/scoring/enforce-threshold', { params: { path: { service_id: serviceId } } }),
    { staleTime: 30_000 },
  )
  const enforceMutation = useMutation({
    mutationFn: async (newThreshold: number | null) =>
      unwrapClient(client.PUT('/api/services/{service_id}/scoring/enforce-threshold', { params: { path: { service_id: serviceId }, query: { confirm: true } }, body: { threshold: newThreshold } })),
    onSuccess: () => invalidateThresholdKeys('scoring-enforce-threshold'),
  })
  const isEnforcingThis = enforce?.threshold === thresholdRaw
  const { data: statusCode } = useScoringQuery(
    ['scoring-enforce-status-code', serviceId],
    () => client.GET('/api/services/{service_id}/scoring/enforce-status-code', { params: { path: { service_id: serviceId } } }),
    { staleTime: 30_000 },
  )
  const statusCodeMutation = useMutation({
    mutationFn: async (newCode: number | null) =>
      unwrapClient(client.PUT('/api/services/{service_id}/scoring/enforce-status-code', { params: { path: { service_id: serviceId }, query: { confirm: true } }, body: { status_code: newCode } })),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['scoring-enforce-status-code', serviceId] })
      queryClient.invalidateQueries({ queryKey: scoringConfigCompositeKey(serviceId) })
      queryClient.invalidateQueries({ queryKey: ['scoring-audit', serviceId] })
    },
  })
  const effectiveStatusCode = statusCode?.effective ?? 429
  const [codeDraft, setCodeDraft] = useDerivedState(String(effectiveStatusCode))
  const codeDraftNum = Number(codeDraft)
  const codeDraftValid = codeDraft !== '' && !Number.isNaN(codeDraftNum) && Number.isInteger(codeDraftNum) && codeDraftNum >= (statusCode?.min ?? 400) && codeDraftNum <= (statusCode?.max ?? 599)
  const codeDraftIsDirty = codeDraftValid && codeDraftNum !== effectiveStatusCode
  const anyMutationPending = commitMutation.isPending || enforceMutation.isPending || statusCodeMutation.isPending
  return (
    <AnalyticsCard
      title="Threshold preview"
      description={`Counterfactual: at threshold X, how many of the last ${sinceHours}h scored sessions would get flagged — and how well does that match your labels?`}
      icon={<SlidersHorizontal className="h-4 w-4" />} helpContent={<ThresholdSliderHelp />} helpTitle="About Threshold & Enforcement"
      error={error as (Error & { status?: number }) | null}
    >
      <div className="space-y-4">
        <ThresholdSliderControls
          thresholdRaw={thresholdRaw} onThresholdRawChange={setThresholdRaw} committed={committed} enforce={enforce}
          statusCode={statusCode} effectiveStatusCode={effectiveStatusCode} isAlreadyCommitted={isAlreadyCommitted}
          isEnforcingThis={isEnforcingThis} commitPending={commitMutation.isPending} enforcePending={enforceMutation.isPending}
          statusCodePending={statusCodeMutation.isPending} codeDraft={codeDraft} onCodeDraftChange={setCodeDraft}
          codeDraftValid={codeDraftValid} codeDraftIsDirty={codeDraftIsDirty} codeDraftNum={codeDraftNum}
          onCommitClick={() => setPendingAction({ action: 'commit', threshold: thresholdRaw })}
          onEnforceClick={() => setPendingAction(isEnforcingThis ? { action: 'disable', threshold: enforce?.threshold ?? thresholdRaw } : { action: 'enforce', threshold: thresholdRaw })}
          onApplyStatusCode={() => setPendingAction({ action: 'change-status-code', statusCode: codeDraftNum })}
          onResetStatusCode={() => { if (statusCode) setPendingAction({ action: 'change-status-code', statusCode: statusCode.default }) }}
        />
        {isLoading || !data ? (
          <div className="grid grid-cols-2 gap-3"><Skeleton className="h-20 w-full" /><Skeleton className="h-20 w-full" /></div>
        ) : (
          <>
            <ThresholdMatrix flagged={data.flagged} passed={data.passed} />
            <ThresholdPreviewStats precision={data.precision} recall={data.recall} totalScoredSessions={data.total_scored_sessions} sinceHours={data.since_hours} />
          </>
        )}
      </div>
      <ConfirmDialog
        open={pendingAction !== null}
        onOpenChange={(open) => { if (!open && !anyMutationPending) setPendingAction(null) }}
        isPending={anyMutationPending} isDangerous={pendingAction?.action !== 'commit'}
        title={pendingAction?.action === 'commit' ? 'Commit threshold' : pendingAction?.action === 'disable' ? 'Disable enforcement' : pendingAction?.action === 'change-status-code' ? 'Change enforce response code' : 'Enforce threshold (LIVE)'}
        description={
          pendingAction?.action === 'commit' ? (
            <>Persist <span className="font-mono">{pendingAction.threshold}</span> as your committed threshold. This is a remembered preference only — it does NOT push to Compute or block any live requests.</>
          ) : pendingAction?.action === 'disable' ? (
            <>Currently enforcing at threshold <span className="font-mono">{pendingAction.threshold}</span>. Disable enforcement so the edge stops returning HTTP <span className="font-mono">{effectiveStatusCode}</span> for high-score sessions. Effective within seconds.</>
          ) : pendingAction?.action === 'change-status-code' ? (
            <>Re-deploy the enforce VCL snippet so flagged requests return HTTP <span className="font-mono">{pendingAction.statusCode}</span> instead of HTTP <span className="font-mono">{effectiveStatusCode}</span>. Takes ~5-10s (one Fastly version activation). Threshold + enforcement state are untouched.</>
          ) : pendingAction?.action === 'enforce' && enforce?.enforced && enforce.threshold != null && enforce.threshold !== pendingAction.threshold ? (
            <>Currently enforcing at threshold <span className="font-mono">{enforce.threshold}</span> (live blocking). Replace with threshold <span className="font-mono">{pendingAction.threshold}</span>? Sessions scoring between <span className="font-mono">{pendingAction.threshold}</span> and <span className="font-mono">{enforce.threshold}</span> will start getting HTTP <span className="font-mono">{effectiveStatusCode}</span> within seconds.</>
          ) : pendingAction?.action === 'enforce' ? (
            <>Enforce threshold <span className="font-mono">{pendingAction.threshold}</span>? Requests with score &ge; <span className="font-mono">{pendingAction.threshold}</span> will get HTTP <span className="font-mono">{effectiveStatusCode}</span> at the edge. Effective within seconds.</>
          ) : null
        }
        confirmLabel={pendingAction?.action === 'commit' ? 'Commit' : pendingAction?.action === 'disable' ? 'Disable' : pendingAction?.action === 'change-status-code' ? 'Re-deploy' : 'Enforce'}
        onConfirm={() => {
          if (!pendingAction) return
          const clear = { onSettled: () => setPendingAction(null) }
          if (pendingAction.action === 'commit') commitMutation.mutate(pendingAction.threshold, clear)
          else if (pendingAction.action === 'disable') enforceMutation.mutate(null, clear)
          else if (pendingAction.action === 'change-status-code') statusCodeMutation.mutate(statusCode != null && pendingAction.statusCode === statusCode.default ? null : pendingAction.statusCode, clear)
          else enforceMutation.mutate(pendingAction.threshold, clear)
        }}
      />
    </AnalyticsCard>
  )
}

interface FlagSessionPopoverProps {
  serviceId: string
  // ``null`` for a cookieless request — the popover renders disabled.
  sid: string | null
  // Optional metadata captured at flag time — denormalized into the label
  // row so the Labels tab can show "what this session looked like" without
  // re-querying the logs.
  sampleIp?: string | null; sampleUa?: string | null; sampleUrl?: string | null
  // Currently-applied label (from the labels API). Undefined means we
  // don't know / haven't checked.
  currentLabel?: LabelValue | null
  // Id of the currently-applied label row, required to issue the DELETE
  // that un-flags the session. Optional — when absent the "Clear Label"
  // affordance is hidden.
  currentLabelId?: string | null
  trigger?: React.ReactNode; onFlagged?: () => void
}

const LABEL_STYLES: Record<LabelValue, string> = {
  good: 'border-emerald-500 text-emerald-700 hover:bg-emerald-50',
  bad: 'border-rose-500 text-rose-700 hover:bg-rose-50',
  neutral: 'border-slate-400 text-slate-700 hover:bg-slate-50',
}

/**
 * Reusable popover for labeling a session as good / bad / neutral. Used
 * from both the SessionScoring admin page's TopFlaggedTable and from the
 * dashboard raw-logs table's Flag column.
 *
 * Always posts to /api/services/{serviceId}/scoring/labels, which upserts
 * on (service_id, sid), so re-flagging the same session overwrites
 * cleanly. The `currentLabel` prop is purely cosmetic (visual hint of
 * existing state) — the upsert semantics on the backend mean we never
 * need to choose between POST and PATCH from the client.
 */
export function FlagSessionPopover({
  serviceId, sid, sampleIp = '', sampleUa = '', sampleUrl = '', currentLabel, currentLabelId, trigger, onFlagged,
}: FlagSessionPopoverProps) {
  const [open, setOpen] = React.useState(false)
  const [notes, setNotes] = React.useState('')
  const [error, setError] = React.useState('')
  const disabled = !sid

  const submitMutation = useMutation({
    mutationFn: async (label: LabelValue) => {
      await unwrapClient(
        client.POST('/api/services/{service_id}/scoring/labels', {
          params: { path: { service_id: serviceId } },
          body: { sid: sid!, label, notes, flagged_by: 'admin', sample_ip: sampleIp ?? '', sample_ua: sampleUa ?? '', sample_url: sampleUrl ?? '' },
        }),
      )
    },
    onMutate: () => setError(''),
    onSuccess: () => { setOpen(false); setNotes(''); onFlagged?.() },
    onError: (e) => setError(extractApiError(e) || 'Failed to save label'),
  })
  const busy = submitMutation.isPending ? (submitMutation.variables ?? null) : null
  const submit = (label: LabelValue) => { if (sid) submitMutation.mutate(label) }

  const clearMutation = useMutation({
    mutationFn: async () => {
      await unwrapClient(
        client.DELETE('/api/services/{service_id}/scoring/labels/{label_id}', {
          params: { path: { service_id: serviceId, label_id: currentLabelId! } },
        }),
      )
    },
    onMutate: () => setError(''),
    onSuccess: () => { setOpen(false); onFlagged?.() },
    onError: (e) => setError(extractApiError(e) || 'Failed to clear label'),
  })
  const clearing = clearMutation.isPending
  const handleClearLabel = () => { if (currentLabelId) clearMutation.mutate() }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger
        render={(props: React.ComponentPropsWithRef<'button'>) =>
          trigger ? (
            <button {...props}>{trigger}</button>
          ) : (
            <Button
              {...props} variant="ghost" size="icon" className="h-7 w-7" disabled={disabled}
              aria-label={disabled ? 'No session id (cookieless request)' : 'Flag this session'}
              title={disabled ? 'No session id (cookieless request)' : 'Flag this session'}
            >
              <Flag className={currentLabel === 'bad' ? 'h-3.5 w-3.5 text-rose-600 fill-rose-600' : currentLabel === 'good' ? 'h-3.5 w-3.5 text-emerald-600 fill-emerald-600' : currentLabel === 'neutral' ? 'h-3.5 w-3.5 text-slate-500 fill-slate-500' : 'h-3.5 w-3.5 text-muted-foreground'} />
            </Button>
          )
        }
      />
      <PopoverContent className="w-80" align="end">
        <div className="space-y-3">
          <div>
            <p className="text-sm font-medium">Label session</p>
            <p className="text-xs text-muted-foreground font-mono break-all mt-0.5">sid: {sid || '(none)'}</p>
            {currentLabel && <p className="text-xs text-muted-foreground mt-1">Currently: <span className="font-medium">{currentLabel}</span> · re-flagging overwrites</p>}
          </div>
          <Textarea value={notes} onChange={(e) => setNotes(e.target.value)} placeholder="Notes (optional) — why this session?" className="text-xs min-h-[60px]" disabled={!!busy} />
          {error && <p className="text-xs text-rose-600">{error}</p>}
          <div className="grid grid-cols-3 gap-2">
            {(['good', 'neutral', 'bad'] as const).map((lbl) => (
              <Button key={lbl} variant="outline" size="sm" className={`text-xs ${LABEL_STYLES[lbl]}`} disabled={disabled || !!busy || clearing} onClick={() => submit(lbl)}>
                {busy === lbl ? <Loader2 className="h-3 w-3 animate-spin" /> : lbl}
              </Button>
            ))}
          </div>
          {currentLabel && currentLabelId && (
            <div className="pt-1 border-t">
              <Button variant="ghost" size="sm" className="w-full text-xs text-rose-600 hover:text-rose-700 hover:bg-rose-50" disabled={!!busy || clearing} onClick={handleClearLabel}>
                {clearing ? <Loader2 className="h-3 w-3 animate-spin" /> : 'Clear Label (Un-flag)'}
              </Button>
            </div>
          )}
        </div>
      </PopoverContent>
    </Popover>
  )
}

type SessionEvent = components['schemas']['ScoringSessionEvent']
type SessionEventRow = { key: string; event: SessionEvent; index: number; prev: SessionEvent | null }

const EVENT_COLUMNS: ColumnDef<SessionEventRow>[] = [
  { id: 'index', cell: ({ row }) => <span className="text-muted-foreground font-mono">{row.original.index + 1}.</span> },
  {
    id: 'details',
    cell: ({ row }) => {
      const { event, prev } = row.original
      const ts = event.ts ? formatLocalDateTime(event.ts) : '—'
      return (
        <div className="space-y-1">
          <div className="flex items-center gap-2">
            {prev && <span className="text-muted-foreground inline-flex items-center gap-0.5"><code className="text-[10px] truncate max-w-[200px]">{prev.url}</code><ChevronRight className="h-3 w-3" /></span>}
            <code className="font-mono text-xs truncate">{event.url}</code>
          </div>
          <div className="flex items-center gap-3 text-[10px] text-muted-foreground">
            <span>{ts}</span>
            {event.status != null && <span>HTTP {event.status}</span>}
            {event.edge_cookie_compliance && <span>cookie:{event.edge_cookie_compliance}</span>}
            {event.edge_score_reason && <span className="text-amber-600">{event.edge_score_reason}</span>}
          </div>
        </div>
      )
    },
  },
  {
    id: 'score',
    cell: ({ row }) => {
      const score = row.original.event.edge_score
      if (score == null) return null
      const tone = score >= 75 ? 'text-destructive' : score >= 50 ? 'text-amber-600' : 'text-muted-foreground'
      return <div className={`text-right font-mono font-semibold ${tone}`}>{score}</div>
    },
  },
]

interface SessionEventsDialogProps { serviceId: string; sid: string; label?: string; trigger?: React.ReactElement }

export function SessionEventsDialog({ serviceId, sid, label, trigger }: SessionEventsDialogProps) {
  const [open, setOpen] = React.useState(false)
  const { data, isLoading, isError, error } = useScoringQuery(
    ['scoring-session-events', serviceId, sid],
    () => client.GET('/api/services/{service_id}/scoring/sessions/{sid}/events', { params: { path: { service_id: serviceId, sid }, query: { since_days: 30 } } }),
    { enabled: open, staleTime: 30_000 },
  )
  const eventRows = React.useMemo<SessionEventRow[]>(
    () => keyByOccurrence(data?.events ?? [], (ev) => `${ev.ts ?? ''}|${ev.edge_score ?? ''}`).map(({ key, item: event }, index, arr) => ({ key, event, index, prev: index > 0 ? arr[index - 1].item : null })),
    [data?.events],
  )
  const eventsTable = useReactTable({ data: eventRows, columns: EVENT_COLUMNS, getCoreRowModel: getCoreRowModel() })

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger render={trigger ?? <button type="button" className="text-xs text-primary hover:underline inline-flex items-center gap-0.5" title="View session events"><Activity className="h-3 w-3" />events</button>} />
      <DialogContent className="max-w-3xl max-h-[80vh] overflow-hidden flex flex-col">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Activity className="h-4 w-4" /><span>Session</span><code className="font-mono text-sm">{sid}</code>
            {label && <Badge variant={label === 'good' ? 'success' : label === 'bad' ? 'destructive' : 'secondary'} className="uppercase text-[10px]">{label}</Badge>}
          </DialogTitle>
          <DialogDescription>
            URL sequence the session walked, oldest first. The L2 scorer evaluates each
            transition (prev URL → current URL) against the trained matrix — high-score
            transitions are the ones the matrix flagged as unusual.
          </DialogDescription>
        </DialogHeader>
        <div className="flex-1 overflow-y-auto -mx-6 px-6">
          {isLoading && <div className="space-y-2" role="status" aria-busy="true"><span className="sr-only">Loading session events</span><SkeletonGrid count={6} height="3rem" className="rounded-md" /></div>}
          {isError && <div className="text-sm text-destructive p-3 border rounded-md">Failed to load events: {(error as Error)?.message ?? 'unknown'}</div>}
          {data && data.event_count === 0 && (
            <div className="text-sm text-muted-foreground p-6 text-center border rounded-md">
              No events found for this sid in the last {data.since_days} days. The
              session may have rotated its cookie, or the rows haven&apos;t been
              ingested yet (cron ticks every ~30s; FOS-to-parquet lag is ~1–2 min).
            </div>
          )}
          {data && (data.event_count ?? 0) > 0 && (
            <div className="space-y-1">
              <div className="text-xs text-muted-foreground mb-2">{data.event_count} event{data.event_count === 1 ? '' : 's'} in last {data.since_days}d</div>
              <Table className="text-xs">
                <TableBody>
                  {eventsTable.getRowModel().rows.map((row) => (
                    <TableRow key={row.original.key} className="align-top">
                      {row.getVisibleCells().map((cell) => <TableCell key={cell.id} className="py-2">{flexRender(cell.column.columnDef.cell, cell.getContext())}</TableCell>)}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}

interface TopFlaggedTableProps { serviceId: string; sinceHours?: number }
type FlaggedRow = components['schemas']['ScoringTopFlaggedRow']

function scoreBadge(score: number) {
  if (score >= 75) return <Badge className="bg-rose-600 hover:bg-rose-600">{score}</Badge>
  if (score >= 50) return <Badge className="bg-amber-500 hover:bg-amber-500">{score}</Badge>
  if (score >= 25) return <Badge className="bg-yellow-400 text-black hover:bg-yellow-400">{score}</Badge>
  return <Badge variant="secondary">{score}</Badge>
}

function complianceBadge(c?: string) {
  if (!c) return null
  if (c === 'ok') return <Badge variant="secondary" className="text-emerald-700">ok</Badge>
  return <Badge variant="outline" className="text-rose-700 border-rose-300">{c}</Badge>
}

const COLUMN_LABELS: Record<string, string> = {
  timestamp: 'Time', edge_score: 'Score', edge_score_l1: 'L1', edge_score_l2: 'L2',
  edge_cookie_compliance: 'Cookie', url: 'URL', ip: 'IP', ua: 'UA', edge_sid: 'SID', __flag: 'Flag',
}

// Hidden by default — UA strings are long + L1/L2 are detail rarely needed.
// Matches the dashboard's "compact by default, opt-in detail" convention.
const DEFAULT_HIDDEN: Record<string, boolean> = { edge_score_l1: false, edge_score_l2: false, ua: false }

export function TopFlaggedTable({ serviceId, sinceHours = 24 }: TopFlaggedTableProps) {
  const qc = useQueryClient()
  const [complianceFilter, setComplianceFilter] = React.useState<string>('all')
  const [minScore, setMinScore] = React.useState<string>('0')
  const [visibility, setVisibility] = React.useState<Record<string, boolean>>(DEFAULT_HIDDEN)

  const flagged = useScoringQuery(['scoring-top-flagged', serviceId, sinceHours], () =>
    client.GET('/api/services/{service_id}/scoring/top-flagged', { params: { path: { service_id: serviceId }, query: { since_hours: sinceHours, limit: 200 } } }),
  )
  const { labelBySid } = useScoringLabels(serviceId)
  const onFlagged = () => {
    qc.invalidateQueries({ queryKey: queryKeys.scoringLabels(serviceId) })
    qc.invalidateQueries({ queryKey: ['scoring-top-flagged', serviceId] })
  }

  // Distinct compliance values present in the result — populates the
  // filter dropdown without hardcoding a list that might drift from
  // what the scorer actually emits.
  const complianceValues = React.useMemo(() => {
    const s = new Set<string>()
    for (const r of flagged.data?.rows ?? []) if (r.edge_cookie_compliance) s.add(r.edge_cookie_compliance)
    return Array.from(s).sort()
  }, [flagged.data])

  const filteredData = React.useMemo(() => {
    const min = parseInt(minScore, 10) || 0
    return (flagged.data?.rows ?? []).filter((r) => r.edge_score >= min && (complianceFilter === 'all' || r.edge_cookie_compliance === complianceFilter))
  }, [flagged.data, complianceFilter, minScore])

  const columns = React.useMemo<ColumnDef<FlaggedRow>[]>(() => [
    { accessorKey: 'timestamp', header: COLUMN_LABELS.timestamp, enableSorting: true, cell: ({ row }) => <DateTimeCell iso={row.original.timestamp} className="text-xs font-mono whitespace-nowrap" /> },
    { accessorKey: 'edge_score', header: COLUMN_LABELS.edge_score, enableSorting: true, cell: ({ row }) => scoreBadge(row.getValue<number>('edge_score')) },
    { accessorKey: 'edge_score_l1', header: COLUMN_LABELS.edge_score_l1, enableSorting: true, cell: ({ row }) => <span className="text-xs">{row.getValue<number>('edge_score_l1') ?? '—'}</span> },
    { accessorKey: 'edge_score_l2', header: COLUMN_LABELS.edge_score_l2, enableSorting: true, cell: ({ row }) => <span className="text-xs">{row.getValue<number>('edge_score_l2') ?? '—'}</span> },
    { accessorKey: 'edge_cookie_compliance', header: COLUMN_LABELS.edge_cookie_compliance, enableSorting: true, cell: ({ row }) => complianceBadge(row.getValue<string>('edge_cookie_compliance')) },
    { accessorKey: 'url', header: COLUMN_LABELS.url, enableSorting: true, cell: ({ row }) => { const url = row.getValue<string>('url') ?? ''; return <span className="text-xs max-w-[320px] truncate inline-block" title={url}>{url}</span> } },
    { accessorKey: 'ip', header: COLUMN_LABELS.ip, enableSorting: true, cell: ({ row }) => <span className="text-xs font-mono">{row.getValue<string>('ip') ?? '—'}</span> },
    { accessorKey: 'ua', header: COLUMN_LABELS.ua, enableSorting: false, cell: ({ row }) => { const ua = row.getValue<string>('ua') ?? ''; return <span className="text-xs max-w-[260px] truncate inline-block" title={ua}>{ua}</span> } },
    {
      accessorKey: 'edge_sid', header: COLUMN_LABELS.edge_sid, enableSorting: true,
      cell: ({ row }) => {
        const sid = row.getValue<string>('edge_sid') ?? ''
        if (!sid) return <span className="text-xs font-mono">—</span>
        return (
          <SessionEventsDialog
            serviceId={serviceId} sid={sid} label={labelBySid.get(sid) ?? undefined}
            trigger={<button type="button" className="text-xs font-mono hover:underline text-primary" title="View session events">{sid}</button>}
          />
        )
      },
    },
    {
      id: '__flag', header: () => <span className="text-right block">{COLUMN_LABELS.__flag}</span>, enableSorting: false,
      cell: ({ row }) => {
        const r = row.original
        return (
          <div className="text-right">
            <FlagSessionPopover serviceId={serviceId} sid={r.edge_sid} sampleIp={r.ip} sampleUa={r.ua} sampleUrl={r.url} currentLabel={r.edge_sid ? (labelBySid.get(r.edge_sid) ?? null) : null} onFlagged={onFlagged} />
          </div>
        )
      },
    },
  ], [serviceId, labelBySid])

  const columnIds = React.useMemo(() => columns.map(columnIdOf), [columns])
  const since = sinceHours >= 24 ? `${sinceHours / 24}d` : `${sinceHours}h`

  return (
    <AnalyticsCard
      title={`Top flagged sessions — last ${since}`}
      description="Sortable + filterable. Use the Flag column to label sessions for matrix evaluation."
      isLoading={flagged.isLoading} isFetching={flagged.isFetching} error={flagged.error as (Error & { status?: number }) | null}
      contentClassName="p-0" helpContent={<TopFlaggedHelp />} helpTitle="About Top Flagged Sessions"
      headerAction={
        <div className="flex items-center gap-2">
          <Select value={complianceFilter} onValueChange={(v) => v && setComplianceFilter(v)}>
            <SelectTrigger className="h-7 text-xs w-32"><SelectValue placeholder="Cookie: any" /></SelectTrigger>
            <SelectContent>
              <SelectItem value="all" className="text-xs">Cookie: any</SelectItem>
              {complianceValues.map((v) => <SelectItem key={v} value={v} className="text-xs">Cookie: {v}</SelectItem>)}
            </SelectContent>
          </Select>
          <Select value={minScore} onValueChange={(v) => v && setMinScore(v)}>
            <SelectTrigger className="h-7 text-xs w-28"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="0" className="text-xs">Score: any</SelectItem>
              <SelectItem value="25" className="text-xs">Score ≥ 25</SelectItem>
              <SelectItem value="50" className="text-xs">Score ≥ 50</SelectItem>
              <SelectItem value="75" className="text-xs">Score ≥ 75</SelectItem>
            </SelectContent>
          </Select>
          <ColumnVisibilityDropdown
            columns={columnIds.filter((id) => id !== '__flag').map((id) => ({ id, label: COLUMN_LABELS[id] ?? id }))}
            visibility={visibility} onChange={(id, visible) => setVisibility((v) => ({ ...v, [id]: visible }))}
          />
        </div>
      }
    >
      <DataTable
        columns={columns} data={filteredData} searchKey="url" initialSorting={[{ id: 'timestamp', desc: true }]}
        hideToolbar columnVisibility={visibility} onColumnVisibilityChange={setVisibility}
        emptyMessage={flagged.isLoading ? '' : `No scored requests match the current filters in the last ${since}.`} showPagination
      />
    </AnalyticsCard>
  )
}

export interface StatusPanelProps { serviceId: string }
type ScoringEvaluation = components['schemas']['ScoringEvaluationResponse']
type CurvePoint = NonNullable<components['schemas']['ScoringCurvePoint']>

const formatActivated = (raw: string): string => {
  const d = new Date(raw)
  return isNaN(d.getTime()) ? raw : formatLocalDateTime(d)
}

function Field({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-center gap-2">
      <span className="text-muted-foreground text-xs w-32 shrink-0">{label}:</span>
      <span className="text-xs">{value}</span>
    </div>
  )
}

function AucField({ evalData }: { evalData: ScoringEvaluation }) {
  if (!evalData.has_min_samples) {
    return (
      <Field
        label="Matrix Quality"
        value={<span className="text-xs text-muted-foreground">Need {evalData.min_per_class}+ good / {evalData.min_per_class}+ bad labels<span className="ml-1 text-foreground tabular-nums">(have {evalData.n_good}/{evalData.n_bad})</span></span>}
      />
    )
  }
  if (evalData.error) return <Field label="Matrix Quality" value={<span className="text-xs text-amber-600">{evalData.error}</span>} />
  const auc = evalData.auc ?? 0
  const passed = !!evalData.passed
  const colorClass = passed ? 'text-emerald-600' : 'text-amber-600'
  return (
    <Field
      label="Matrix Quality (AUC)"
      value={
        <span className="text-xs">
          <span className={`font-mono font-semibold tabular-nums ${colorClass}`}>{auc.toFixed(3)}</span>
          <span className={`ml-1 font-semibold ${colorClass}`}>— {passed ? 'PASS' : 'BELOW THRESHOLD'}</span>
          <span className="ml-1 text-muted-foreground">(n={evalData.n_good} good / {evalData.n_bad} bad{evalData.threshold ? `, threshold ${evalData.threshold.toFixed(2)}` : ''})</span>
        </span>
      }
    />
  )
}

const REDEPLOY_STEPS = [
  'Checks the live edge first: re-uploads the Wasm only if the build changed',
  'Re-activates the logging VCL only if the scoring snippets/backend changed',
  'Skips no-op version bumps when nothing changed — no scoring gap',
]

const DISABLE_STEPS = [
  'Removes the 6 VCL snippets (recv / pass / fetch / deliver / miss / enforce) that route requests through the scorer',
  'Removes the 6 scoring custom_fields from the log format',
  'Clones + activates a new VCL version on the customer service',
  'Deletes the scorer Compute service + its ConfigStores and matrix KV Store (a future Enable re-provisions them from scratch)',
]

function StepListDescription({ intro, serviceId, steps, header, footer }: { intro: string; serviceId: string; steps: React.ReactNode[]; header?: React.ReactNode; footer?: React.ReactNode }) {
  return (
    <div className="space-y-2">
      {header}
      <p>{intro} <code className="text-xs bg-muted px-1 rounded">{serviceId}</code>:</p>
      <ul className="text-sm text-muted-foreground list-disc pl-5 space-y-1">{steps.map((s) => <li key={typeof s === 'string' ? s : String(steps.indexOf(s))}>{s}</li>)}</ul>
      {footer}
      <p className="text-xs text-muted-foreground italic">Click <strong>Start</strong> to proceed.</p>
    </div>
  )
}

function CalloutBanner({ tone = 'amber', children, testId }: { tone?: 'amber' | 'sky' | 'muted'; children: React.ReactNode; testId?: string }) {
  const Icon = tone === 'amber' ? AlertTriangle : Info
  const cls = tone === 'amber' ? 'border-amber-300 bg-amber-50 text-amber-900' : tone === 'sky' ? 'border-sky-300 bg-sky-50 text-sky-900' : 'bg-muted/40 text-muted-foreground'
  return (
    <div className={`flex items-start gap-2 rounded border px-2.5 py-1.5 ${cls}`} data-testid={testId}>
      <Icon className="h-3.5 w-3.5 mt-0.5 shrink-0" />
      <div className="text-xs">{children}</div>
    </div>
  )
}

export function StatusPanel({ serviceId }: StatusPanelProps) {
  const queryClient = useQueryClient()
  const { data, isLoading, isFetching, error, refetch } = useScoringQuery(
    ['scoring-status', serviceId],
    () => client.GET('/api/services/{service_id}/scoring/status', { params: { path: { service_id: serviceId } } }),
  )

  const enabled = !!data?.enabled
  const pragmasMissing = !enabled && data?.preflight_pragmas_ready === false
  const [pragmaModalOpen, setPragmaModalOpen] = React.useState(false)
  if (data?.preflight_pragmas_ready === true && pragmaModalOpen) setPragmaModalOpen(false)

  const accountTeamMessage = `Hi Fastly team — please enable the preflight request body preservation service pragmas on Fastly service ${serviceId} (preserving unsent request bodies across VCL restarts and omitting request bodies when backend requests are rewritten to GET or HEAD) so we can enable our Session Scoring Compute preflight pass.`

  const refreshScoring = React.useCallback(() => {
    refetch()
    invalidateServiceScoringQueries(queryClient, serviceId)
  }, [queryClient, refetch, serviceId])

  const { data: evalData } = useScoringQuery(
    ['scoring-evaluation', serviceId],
    () => client.GET('/api/services/{service_id}/scoring/evaluation', { params: { path: { service_id: serviceId } } }),
    { enabled, staleTime: 30_000 },
  )

  const extLinkCls = 'text-muted-foreground hover:text-foreground opacity-50 hover:opacity-100 transition-opacity'

  return (
    <div data-testid="admin-session-scoring-status-card">
      <AnalyticsCard
        title="Session Scoring" icon={enabled ? <ShieldCheck className="h-4 w-4" /> : <ShieldOff className="h-4 w-4" />}
        description="Edge scorer that classifies sessions in real-time using the cookie-bound state machine and matrix-based L2 evaluation."
        isLoading={isLoading} isFetching={isFetching} loadingMessage="Loading session scoring status..."
        error={error as (Error & { status?: number }) | null} helpContent={<StatusPanelHelp />} helpTitle="About Session Scoring" className="min-h-[220px]"
        headerAction={
          enabled ? (
            <div className="flex items-center gap-2">
              <SSEModal
                title="Redeploy Scorer" endpoint={`/api/services/${serviceId}/scoring/enable`} body={{}} onClose={refreshScoring}
                description={<StepListDescription intro="Push the latest scorer build to the edge for" serviceId={serviceId} steps={REDEPLOY_STEPS} />}
                trigger={<Button variant={data?.scorer_drift ? 'default' : 'outline'} size="sm" data-testid="admin-session-scoring-redeploy-btn"><RefreshCw className="h-3.5 w-3.5 mr-1" /> Redeploy</Button>}
              />
              <SSEModal
                title="Disable Session Scoring" endpoint={`/api/services/${serviceId}/scoring/disable`} body={{}} onClose={refreshScoring}
                description={<StepListDescription intro="This will disable session scoring on" serviceId={serviceId} steps={DISABLE_STEPS} />}
                trigger={<Button variant="outline" size="sm" className="text-rose-700 border-rose-300 hover:bg-rose-50" data-testid="admin-session-scoring-disable-btn"><PowerOff className="h-3.5 w-3.5 mr-1" /> Disable</Button>}
              />
            </div>
          ) : pragmasMissing ? (
            <>
              <Button variant="default" size="sm" data-testid="admin-session-scoring-enable-btn" onClick={() => setPragmaModalOpen(true)}>
                <Power className="h-3.5 w-3.5 mr-1" /> Enable
              </Button>
              <Dialog open={pragmaModalOpen} onOpenChange={setPragmaModalOpen}>
                <DialogContent className="sm:max-w-lg" data-testid="admin-session-scoring-pragma-modal">
                  <DialogHeader>
                    <DialogTitle>Preflight Request Body Preservation Required</DialogTitle>
                    <DialogDescription>
                      Before enabling the Session Scoring Compute scorer on <code className="text-xs bg-muted px-1 rounded">{serviceId}</code>, your Fastly account team must enable preflight request body preservation on this service.
                    </DialogDescription>
                  </DialogHeader>
                  <div className="space-y-3 py-2 text-sm">
                    <div className="flex items-start gap-2 rounded border border-amber-300 bg-amber-50 px-3 py-2 text-amber-900">
                      <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
                      <div className="text-xs space-y-1">
                        <div className="font-semibold">Why is this required?</div>
                        <div>Session Scoring routes each request through a lightweight Compute preflight sub-request (rewritten to <code>GET</code>) and then restarts VCL to forward the original request to your origin. Without service-level preflight body preservation enabled, <code>POST</code>/<code>PUT</code>/<code>PATCH</code> request bodies can be drained or stall when forwarding to origin after the restart.</div>
                      </div>
                    </div>
                    <div className="space-y-1.5">
                      <div className="text-xs font-medium text-muted-foreground">Send this request to your Fastly account team:</div>
                      <pre className="rounded border bg-muted/60 p-3 text-xs whitespace-pre-wrap break-words font-mono" data-testid="admin-session-scoring-pragma-message">{accountTeamMessage}</pre>
                    </div>
                  </div>
                  <DialogFooter className="gap-2 sm:justify-between">
                    <CopyButton text={accountTeamMessage} label="Copy Message" copiedLabel="Copied" variant="outline" size="sm" className="" iconClassName="h-3.5 w-3.5" data-testid="admin-session-scoring-pragma-copy-btn" />
                    <div className="flex items-center gap-2">
                      <Button type="button" variant="outline" size="sm" onClick={() => refetch()} disabled={isFetching} data-testid="admin-session-scoring-pragma-recheck-btn">
                        <RefreshCw className={`h-3.5 w-3.5 mr-1 ${isFetching ? 'animate-spin' : ''}`} /> Check Again
                      </Button>
                      <Button type="button" variant="default" size="sm" onClick={() => setPragmaModalOpen(false)}>Close</Button>
                    </div>
                  </DialogFooter>
                </DialogContent>
              </Dialog>
            </>
          ) : (
            <SSEModal
              title="Enable Session Scoring" endpoint={`/api/services/${serviceId}/scoring/enable`} body={{}} onClose={refreshScoring}
              description={
                <StepListDescription
                  intro="This will enable session scoring on" serviceId={serviceId}
                  header={
                    <div className="flex items-start gap-2 rounded border border-amber-300 bg-amber-50 px-3 py-2 text-amber-900">
                      <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
                      <div className="text-xs space-y-1">
                        <div className="font-semibold">Experimental feature</div>
                        <div>Session scoring has not been validated against production traffic at scale. Run it on a non-critical service first, watch the scorer Compute service&apos;s error rate, and keep the disable path handy.</div>
                      </div>
                    </div>
                  }
                  steps={[
                    "Provisions the scorer Compute service if it doesn't exist, deploys the trained Wasm scorer with the latest matrix",
                    <>Adds the <code>session_scorer</code> backend, 6 VCL snippets, and 9 score fields (<code>edge_score</code>, session id, reason, L1/L2, latency, …) to the customer service</>,
                    'Clones + activates a new VCL version',
                    'Every request will be routed through the scorer for an L1+L2 score on the response cookie',
                  ]}
                  footer={
                    <div className="flex items-start gap-2 rounded border border-sky-300 bg-sky-50 px-3 py-2 text-sky-900">
                      <Info className="h-4 w-4 mt-0.5 shrink-0" />
                      <div className="text-xs space-y-1">
                        <div className="font-semibold">Log fields</div>
                        <div>Scoring also needs the standard request fields for context — <strong>URL, method, user agent, and geo</strong>. Enabling scoring ensures those stay logged (added automatically if this service was set up with a minimal log-field set), so scored sessions show <em>which</em> requests were flagged, not just the score.</div>
                      </div>
                    </div>
                  }
                />
              }
              trigger={<Button variant="default" size="sm" data-testid="admin-session-scoring-enable-btn"><Power className="h-3.5 w-3.5 mr-1" /> Enable</Button>}
            />
          )
        }
      >
        <div className="space-y-3 text-sm">
          <div className="flex items-center gap-2" data-testid="admin-session-scoring-status-line">
            <span className="text-muted-foreground">Status:</span>
            {enabled
              ? <Badge variant="default" className="bg-emerald-600" data-testid="admin-session-scoring-status-badge">Enabled</Badge>
              : <Badge variant="secondary" data-testid="admin-session-scoring-status-badge">Disabled</Badge>}
          </div>

          {enabled && (
            <>
              {data?.scorer_drift && (
                <CalloutBanner><span className="font-semibold">Redeploy needed.</span> The edge is running an older scorer build{data.drift_detail ? ` (${data.drift_detail})` : ''} than the one shipped to the backend. Click <strong>Redeploy</strong> above to push the latest.</CalloutBanner>
              )}
              {!data?.scorer_drift && data?.drift_known === false && (
                <CalloutBanner tone="muted"><span className="font-semibold text-foreground">Drift detection not baselined.</span> This service was enabled before edge-drift tracking shipped, so we can&apos;t tell whether the edge is up to date. Click <strong>Redeploy</strong> once to baseline it — after that a banner appears here whenever the edge falls behind.</CalloutBanner>
              )}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-x-6 gap-y-2">
                {data?.scoring_service_id && (
                  <Field label="Scoring Service ID" value={<div className="flex items-center gap-1.5"><code className="text-xs">{data.scoring_service_id}</code><a href={`https://manage.fastly.com/configure/services/${data.scoring_service_id}`} target="_blank" rel="noreferrer" className={extLinkCls} title="View Service in Fastly"><ExternalLink className="h-3 w-3" /></a></div>} />
                )}
                {typeof data?.scoring_active_version === 'number' && (
                  <Field label="Active Version" value={<div className="flex items-center gap-1.5"><code className="text-xs">v{data.scoring_active_version}</code>{data?.scoring_service_id && <a href={`https://manage.fastly.com/configure/services/${data.scoring_service_id}/versions`} target="_blank" rel="noreferrer" className={extLinkCls} title="View versions in Fastly"><ExternalLink className="h-3 w-3" /></a>}</div>} />
                )}
                {data?.scoring_activated_at && <Field label="Activated" value={<span className="text-xs text-muted-foreground" title={data.scoring_activated_at}>{formatActivated(data.scoring_activated_at)}</span>} />}
                {data?.scoring_service_name && <Field label="Service Name" value={data.scoring_service_name} />}
                {data?.scoring_domain && <Field label="Domain" value={<a href={`https://${data.scoring_domain}/_status`} target="_blank" rel="noreferrer" className="text-primary hover:underline inline-flex items-center gap-1">{data.scoring_domain} <ExternalLink className="h-3 w-3" /></a>} />}
                {data?.matrix_version && <Field label="Matrix Version" value={<code className="text-xs">{data.matrix_version}</code>} />}
                {data?.enabled_at && <Field label="Enabled At" value={<span className="text-xs text-muted-foreground">{data.enabled_at}</span>} />}
                {evalData && <AucField evalData={evalData} />}
              </div>
            </>
          )}

          {!enabled && !isLoading && (
            <>
              {pragmasMissing && (
                <CalloutBanner testId="admin-session-scoring-pragma-warning"><span className="font-semibold">Prerequisite required.</span> Preflight request body preservation is not yet enabled on this service. Click <strong>Enable</strong> for instructions to share with your Fastly account team.</CalloutBanner>
              )}
              <CalloutBanner><span className="font-semibold">Experimental.</span> Not yet validated against production traffic at scale — run on a non-critical service first.</CalloutBanner>
              <p className="text-xs text-muted-foreground">Session scoring is off. Click <strong>Enable</strong> to provision the scorer Compute service and start scoring every request at the edge.</p>
            </>
          )}
        </div>
      </AnalyticsCard>
    </div>
  )
}

export interface L2EnforcementCardProps { serviceId: string; sinceHours?: number }
const L2_DESCRIPTION = 'Decide when edge Layer-2 (route-transition anomaly) joins the enforced score. L2 is always logged; this only gates blocking.'
const fmtDaysFixed = (n: number | null | undefined) => (n == null ? '—' : n.toFixed(1))

export function L2EnforcementCard({ serviceId, sinceHours = 24 }: L2EnforcementCardProps) {
  const queryClient = useQueryClient()
  const [pendingEnable, setPendingEnable] = React.useState<boolean | null>(null)

  const { data, isLoading, isError, error, refetch } = useScoringQuery(
    ['scoring-l2-enforce', serviceId],
    () => client.GET('/api/services/{service_id}/scoring/l2-enforce', { params: { path: { service_id: serviceId } } }),
    { staleTime: 30_000 },
  )

  const { data: health } = useScoringQuery(
    ['scoring-health', serviceId, sinceHours],
    () => client.GET('/api/services/{service_id}/scoring/health', { params: { path: { service_id: serviceId }, query: { since_hours: sinceHours } } }),
    { staleTime: 30_000 },
  )

  const mutation = useMutation({
    mutationFn: async (enabled: boolean) =>
      unwrapClient(client.PUT('/api/services/{service_id}/scoring/l2-enforce', { params: { path: { service_id: serviceId }, query: { confirm: true } }, body: { enabled } })),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['scoring-l2-enforce', serviceId] })
      queryClient.invalidateQueries({ queryKey: scoringAnalyticsCompositeKey(serviceId) })
      queryClient.invalidateQueries({ queryKey: scoringConfigCompositeKey(serviceId) })
    },
  })

  if (isError) {
    const raw = String(error?.message || 'Unknown error')
    const friendly = /status 400|http 400|config store missing|scoring not enabled/i.test(raw) || (error as { status?: number } | null)?.status === 400
      ? 'Enable session scoring for this service to manage L2 enforcement.'
      : raw
    return (
      <AnalyticsCard title="L2 enforcement" description={L2_DESCRIPTION}>
        <CardErrorState icon={<ShieldAlert className="h-4 w-4" />} title="L2 enforcement unavailable" message={friendly} onRetry={() => refetch()} />
      </AnalyticsCard>
    )
  }

  if (isLoading || !data) {
    return (
      <AnalyticsCard title="L2 enforcement" description={L2_DESCRIPTION}>
        <div className="space-y-3" aria-busy="true"><Skeleton className="h-16 w-full" /><Skeleton className="h-12 w-full" /></div>
      </AnalyticsCard>
    )
  }

  const { enabled, ready } = data
  const ageDays = data.deployment_age_days
  const readinessDays = data.readiness_days || 7
  const rampDays = data.ramp_days || 3
  const l2HighPct = health?.matrix_staleness?.l2_high_pct ?? null
  const rampPct = Math.round((data.ramp_progress ?? 0) * 100)

  const isGreenAlert = enabled || ready
  const AlertIcon = enabled ? TrendingUp : ready ? ShieldCheck : ShieldAlert
  const alertTitle = enabled ? 'L2 contributes to enforcement' : ready ? 'Ready to enable' : 'Not yet recommended'
  const alertDesc = enabled ? (
    data.fully_ramped ? <>L2 is fully active in the enforced score.</> : (
      <>Fading in — {rampPct}% of full weight{data.warmup_days_remaining != null && <> (~{fmtDaysFixed(data.warmup_days_remaining)} days to full)</>}. The ramp opens at 0% at opt-in, so there&apos;s no instant block.</>
    )
  ) : ready ? (
    <>{fmtDaysFixed(ageDays)} days of observed L2 data (≥{readinessDays} recommended).{l2HighPct != null && <> About {formatPctValue(l2HighPct, 1)} of recent sessions would be flagged by L2.</>}</>
  ) : (
    <>Only {fmtDaysFixed(ageDays)} days of observed L2 data so far (≥{readinessDays} recommended). Enabling blocking now is risky — you haven&apos;t seen enough of how L2 scores real traffic.{l2HighPct != null && <> It would currently flag ~{formatPctValue(l2HighPct, 1)} of sessions.</>}</>
  )

  return (
    <AnalyticsCard title="L2 enforcement" description={L2_DESCRIPTION} icon={<ShieldAlert className="h-4 w-4" />}>
      <div className="space-y-4">
        <Alert className={isGreenAlert ? 'border-green-300 bg-green-50/60 dark:bg-green-950/20' : 'border-amber-300 bg-amber-50/60 dark:bg-amber-950/20'}>
          <AlertIcon className={cn('h-4 w-4', isGreenAlert ? 'text-green-600' : 'text-amber-600')} />
          <AlertTitle className={isGreenAlert ? 'text-green-900 dark:text-green-300' : 'text-amber-900 dark:text-amber-300'}>{alertTitle}</AlertTitle>
          <AlertDescription className={isGreenAlert ? 'text-green-800 dark:text-green-400' : 'text-amber-800 dark:text-amber-400'}>{alertDesc}</AlertDescription>
        </Alert>

        <div className="flex items-center justify-between gap-4 rounded-md border bg-card p-3">
          <div className="space-y-0.5">
            <div id="l2-enforce-label" className="text-sm font-medium">Enforce L2 (live edge blocking)</div>
            <div className="text-xs text-muted-foreground">{enabled ? 'On — L2 contributes to the enforced score.' : 'Off — L2 is observe-only (computed and logged, never blocks).'}</div>
          </div>
          <Switch checked={enabled} aria-labelledby="l2-enforce-label" disabled={mutation.isPending} onCheckedChange={(next) => setPendingEnable(next)} />
        </div>
      </div>

      <ConfirmDialog
        open={pendingEnable !== null}
        onOpenChange={(open) => { if (!open && !mutation.isPending) setPendingEnable(null) }}
        isPending={mutation.isPending}
        isDangerous={pendingEnable === true}
        title={pendingEnable ? 'Enable L2 enforcement (LIVE)' : 'Disable L2 enforcement'}
        description={
          pendingEnable ? (
            !ready ? (
              <><strong>Heads up:</strong> only {fmtDaysFixed(ageDays)} days of observed L2 data so far (≥{readinessDays} recommended). Enabling now means L2 starts contributing to live edge blocking before you&apos;ve seen how it scores real traffic. It fades in over {rampDays} days from now (starting at 0, so no instant block), but consider waiting until the readiness gauge is green. Enable anyway?</>
            ) : (
              <>L2 will join the enforced score, fading in over {rampDays} days from now (starting at 0% weight, so no instant block).{l2HighPct != null && <> About {formatPctValue(l2HighPct, 1)} of recent sessions would be flagged by L2.</>} Effective at the edge within seconds.</>
            )
          ) : (
            <>Disable L2 enforcement. L2 returns to observe-only — still computed and logged, but it stops contributing to the enforced score (no edge blocking from L2). Effective within seconds. Re-enabling later restarts the {rampDays}-day fade-in.</>
          )
        }
        confirmLabel={pendingEnable ? 'Enable' : 'Disable'}
        onConfirm={() => { if (pendingEnable !== null) mutation.mutate(pendingEnable, { onSettled: () => setPendingEnable(null) }) }}
      />
    </AnalyticsCard>
  )
}

export interface RocPrCurvesProps { serviceId: string; sinceHours?: number }

const makeCurveTrace = (pts: CurvePoint[] | null | undefined, xK: keyof CurvePoint, yK: keyof CurvePoint, name: string, color: string, xLbl: string, yLbl: string) => ({
  x: (pts ?? []).map((p) => (p[xK] as number | undefined) ?? 0),
  y: (pts ?? []).map((p) => (p[yK] as number | undefined) ?? 0),
  type: 'scatter', mode: 'lines+markers', name, line: { color, width: 2 }, marker: { size: 4 },
  hovertemplate: `threshold=%{customdata}<br>${xLbl}=%{x:.3f}<br>${yLbl}=%{y:.3f}<extra></extra>`,
  customdata: (pts ?? []).map((p) => p.threshold),
})

export function RocPrCurves({ serviceId }: RocPrCurvesProps) {
  const { data, isLoading, isError, error, refetch } = useScoringQuery(
    ['scoring-curves', serviceId],
    () => client.GET('/api/services/{service_id}/scoring/curves', { params: { path: { service_id: serviceId } } }),
    { staleTime: 30_000 },
  )

  return (
    <AnalyticsCard
      title="ROC + Precision-Recall curves" icon={<LineChart className="h-4 w-4" />} helpContent={<RocPrCurvesHelp />} helpTitle="About ROC & PR Curves"
      description="Two views of the trade-off between catching bad sessions (true positives) and over-flagging good ones (false positives). Computed against ALL of your accumulated labels."
    >
      {isError ? (
        <CardErrorState icon={<Info className="h-4 w-4" />} title="Failed to load ROC + PR curves" message={error?.message || 'Unknown error'} onRetry={() => refetch()} />
      ) : isLoading || !data ? (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-3"><Skeleton className="h-64 w-full" /><Skeleton className="h-64 w-full" /></div>
      ) : !data.has_min_samples ? (
        <div className="p-6 border border-dashed rounded-md text-center text-sm text-muted-foreground">
          Need {data.min_per_class}+ good and {data.min_per_class}+ bad labels to plot meaningful curves
          <span className="ml-1 text-foreground font-mono tabular-nums">(have {data.n_good}/{data.n_bad})</span>
          {data.note && <div className="mt-2 text-xs"><span className="text-amber-600">{data.note}</span></div>}
        </div>
      ) : (
        <div className="space-y-3">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
            <CurveChart
              header={`ROC — AUC ${(data.auc ?? 0).toFixed(3)}`} xTitle="False positive rate" yTitle="True positive rate"
              traces={[
                makeCurveTrace(data.roc, 'fpr', 'tpr', 'ROC', '#0ea5e9', 'FPR', 'TPR'),
                { x: [0, 1], y: [0, 1], type: 'scatter', mode: 'lines', name: 'random', line: { color: CHART_COLORS.neutral, width: 1, dash: 'dash' }, hoverinfo: 'skip' },
              ]}
            />
            <CurveChart
              header={`Precision-Recall — AP ${(data.average_precision ?? 0).toFixed(3)}`} xTitle="Recall" yTitle="Precision"
              traces={[makeCurveTrace(data.pr, 'recall', 'precision', 'PR', CHART_COLORS.success, 'recall', 'precision')]}
            />
          </div>
          <p className="text-[11px] text-muted-foreground italic text-center">
            n={data.n_good} good / {data.n_bad} bad · AUC <span className="font-mono tabular-nums font-semibold">{(data.auc ?? 0).toFixed(3)}</span> · Average precision <span className="font-mono tabular-nums font-semibold">{(data.average_precision ?? 0).toFixed(3)}</span>
          </p>
        </div>
      )}
    </AnalyticsCard>
  )
}

function CurveChart({ header, xTitle, yTitle, traces }: { header: string; xTitle: string; yTitle: string; traces: Record<string, unknown>[] }) {
  return (
    <div className="border rounded-md p-2">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground px-2 pb-1">{header}</div>
      <PlotlyChart data={traces} height={240} layout={{ xaxis: { title: xTitle, range: [0, 1] }, yaxis: { title: yTitle, range: [0, 1] }, showlegend: false, margin: { l: 50, r: 10, t: 10, b: 40 } }} />
    </div>
  )
}

export function PerReasonAucCard({ serviceId }: { serviceId: string }) {
  const { data, isLoading, isError, error, refetch } = useScoringQuery(
    ['scoring-evaluation-per-reason', serviceId],
    () => client.GET('/api/services/{service_id}/scoring/evaluation/per-reason', { params: { path: { service_id: serviceId } } }),
    { staleTime: 30_000 },
  )

  return (
    <AnalyticsCard
      title="AUC by rule" icon={<ListChecks className="h-4 w-4" />} helpContent={<PerReasonAucHelp />} helpTitle="About AUC by Rule"
      description="Which scoring rule contributes most to matrix-vs-labels separation. Each bucket shows AUC against sessions where that rule fired."
    >
      {isError ? (
        <CardErrorState icon={<Info className="h-4 w-4" />} title="Failed to load per-rule AUC" message={(error as Error | null)?.message || 'Unknown error'} onRetry={() => refetch()} />
      ) : isLoading || !data ? (
        <div className="space-y-2">{['r1', 'r2', 'r3', 'r4', 'r5'].map((k) => <Skeleton key={k} className="h-10 w-full" />)}</div>
      ) : !data.has_min_samples_overall ? (
        <div className="p-4 border border-dashed rounded-md text-center text-sm text-muted-foreground">
          Per-rule breakdown unlocks once headline AUC is computable —<span className="ml-1 text-foreground font-mono">need {data.min_per_class}+ good / {data.min_per_class}+ bad labels (have {data.n_good}/{data.n_bad})</span>
        </div>
      ) : (
        <div className="space-y-1.5">{data.buckets.map((b: components['schemas']['ScoringPerReasonBucket']) => <BucketRow key={b.reason} bucket={b} />)}</div>
      )}
    </AnalyticsCard>
  )
}

function BucketRow({ bucket }: { bucket: components['schemas']['ScoringPerReasonBucket'] }) {
  if (!bucket.has_min_samples) {
    return (
      <div className="flex items-center justify-between gap-3 p-2 rounded-md border bg-muted/20 text-xs">
        <span className="font-mono">{bucket.reason}</span>
        <span className="text-muted-foreground">need {bucket.min_per_class}+ good / {bucket.min_per_class}+ bad with this reason<span className="ml-1 text-foreground tabular-nums">(have {bucket.n_good}/{bucket.n_bad})</span></span>
      </div>
    )
  }
  const passed = !!bucket.passed
  return (
    <div className="flex items-center justify-between gap-3 p-2 rounded-md border text-xs">
      <span className="font-mono">{bucket.reason}</span>
      <div className="flex items-center gap-2">
        <span className={`font-mono font-semibold tabular-nums ${passed ? 'text-emerald-600' : 'text-amber-600'}`}>{(bucket.auc ?? 0).toFixed(3)}</span>
        <Badge variant={passed ? 'success' : 'secondary'} className="text-[10px]">{passed ? 'PASS' : 'BELOW'}</Badge>
        <span className="text-muted-foreground text-[10px] tabular-nums">n={bucket.n_good}g / {bucket.n_bad}b</span>
      </div>
    </div>
  )
}

// =============================================================================
// 2. DuckLake / Iceberg Status & 90-Day Storage Distribution Calendar
// =============================================================================

export function IcebergStatus() {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const { full, abbr } = useDateFormat()
  const { data: info, isLoading, error } = useQuery({
    queryKey: queryKeys.admin.iceberg(activeServiceId),
    queryFn: async () => unwrapClient(client.GET('/api/admin/iceberg-info')),
    enabled: !!activeServiceId, staleTime: 60_000, refetchInterval: 5 * 60_000, refetchIntervalInBackground: false,
  })
  if (!activeServiceId) return null
  if (error) return <CardErrorState icon={<Info className="h-4 w-4" />} title="Failed to load DuckLake table info" message={error?.message || 'Unknown error'} />

  const stats = [
    { label: 'Snapshots', value: info?.snapshots ?? 0, icon: Layers, tooltip: 'Total number of atomic commits in the DuckLake table history.' },
    { label: 'Data Files', value: info?.data_files ?? 0, icon: FileCode, tooltip: 'Number of Parquet data files currently in the DuckLake table.' },
    { label: 'Total Size', value: formatBytes(info?.size_bytes ?? 0), icon: Database, tooltip: 'Cumulative size of all data files in the DuckLake table.' },
    { label: 'Latest Commit', value: info?.latest_snapshot_at ? formatRelative(info.latest_snapshot_at) : 'Never', icon: Clock, tooltip: info?.latest_snapshot_at ? `${full(info.latest_snapshot_at)} ${abbr()}` : 'No snapshots yet.' },
  ]
  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        {stats.map((stat) => (
          <Card key={stat.label} className="shadow-none border-muted/60">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground flex items-center gap-1.5">
                <LabelWithInfo label={stat.label} info={stat.tooltip} className="gap-1.5" labelClassName="text-[10px] font-bold uppercase tracking-wider text-muted-foreground" />
              </CardTitle>
              <stat.icon className="h-3.5 w-3.5 text-muted-foreground/60" />
            </CardHeader>
            <CardContent>
              {isLoading ? <Skeleton className="h-7 w-20" /> : (
                <TooltipProvider>
                  <Tooltip>
                    <TooltipTrigger render={<div className="text-xl font-mono font-bold tracking-tight rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring" tabIndex={0} role="button" aria-label={`${stat.label}: ${stat.value}`}>{stat.value}</div>} />
                    <TooltipContent className="text-xs">{stat.tooltip}</TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              )}
            </CardContent>
          </Card>
        ))}
      </div>
      <Card className="shadow-none border-muted/60 bg-muted/5">
        <CardContent className="p-4 flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-blue-500/10 rounded-full"><Archive className="h-4 w-4 text-blue-500" /></div>
            <div>
              <h4 className="text-sm font-semibold">Local Buffer Status</h4>
              <p className="text-xs text-muted-foreground">Data ingested but not yet committed to the DuckLake table.</p>
            </div>
          </div>
          <div className="flex items-center gap-6">
            {([['Pending Files', info?.buffer_files ?? 0, 'w-12'], ['Buffer Size', formatBytes(info?.buffer_size_bytes ?? 0), 'w-16']] as const).map(([lbl, val, wCls]) => (
              <div key={lbl} className="flex flex-col">
                <span className="text-[10px] font-bold uppercase text-muted-foreground">{lbl}</span>
                {isLoading ? <Skeleton className={`h-5 ${wCls} mt-1`} /> : <span className="text-sm font-mono font-bold">{val}</span>}
              </div>
            ))}
            <div className="flex items-center gap-2 ml-4">
              {info?.buffer_files && info.buffer_files > 0 ? <Badge variant="outline" className="h-6 px-2 text-[10px] bg-blue-500/10 text-blue-600 border-blue-500/20 animate-pulse shadow-none uppercase font-bold">Pending Commit</Badge> : null}
            </div>
          </div>
        </CardContent>
      </Card>
      {info?.table_location && (
        <div className="px-4 py-2 bg-muted/30 rounded-md border border-dashed flex items-center justify-between">
          <div className="flex items-center gap-2 overflow-hidden">
            <Database className="h-3 w-3 text-muted-foreground shrink-0" />
            <span className="text-[10px] text-muted-foreground font-mono truncate">{info.table_location}</span>
          </div>
          <span className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest ml-4 shrink-0">Hadoop Catalog (Fastly Object Storage {info.region ? `- ${info.region}` : ''})</span>
        </div>
      )}
    </div>
  )
}

export function IcebergCalendar() {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const { data: calendar, isLoading } = useQuery({
    queryKey: queryKeys.admin.icebergCalendar(activeServiceId),
    queryFn: async () => unwrapClient(client.GET('/api/admin/iceberg-calendar')),
    enabled: !!activeServiceId, staleTime: 60_000, refetchInterval: 5 * 60_000, refetchIntervalInBackground: false,
  })
  const days = React.useMemo(() => {
    const today = new Date()
    today.setHours(0, 0, 0, 0)
    const pad = (n: number) => n.toString().padStart(2, '0')
    return Array.from({ length: 90 }, (_, i) => {
      const d = new Date(today)
      d.setDate(d.getDate() - i)
      return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
    }).reverse()
  }, [])
  if (!activeServiceId) return null

  return (
    <Card className="shadow-none border-muted/60">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-semibold">DuckLake Storage Distribution</CardTitle>
        <CardDescription className="text-xs">Physical data file counts and sizes partitioned by day.</CardDescription>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="flex items-center gap-2 py-6 text-sm text-muted-foreground animate-pulse"><Loader2 className="h-3 w-3 animate-spin" /> Loading distribution data...</div>
        ) : (
          <div className="flex flex-wrap gap-1">
            {days.map((date) => {
              const entry = calendar?.[date]
              const dayData = Array.isArray(entry) ? undefined : entry
              const hasData = !!dayData
              return (
                <TooltipProvider key={date}>
                  <Tooltip>
                    <TooltipTrigger render={<span className="inline-block rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring" tabIndex={0} role="button" aria-label={hasData ? `${date}: ${dayData.data_files} files, ${formatBytes(dayData.size_bytes)}` : `${date}: no data`} />}>
                      <div className={cn('h-4 w-4 rounded-sm transition-colors border', hasData ? 'bg-blue-500 border-blue-600/20 hover:bg-blue-400' : 'bg-muted/30 border-border/50 hover:bg-muted/50')} />
                    </TooltipTrigger>
                    <TooltipContent className="text-xs max-w-[200px] p-2 space-y-1">
                      <div className="font-bold border-b border-muted/20 pb-1 mb-1">{date}</div>
                      {hasData ? (
                        <>
                          <div className="flex justify-between gap-4"><span className="text-muted-foreground">Files:</span><span className="font-mono">{dayData.data_files}</span></div>
                          <div className="flex justify-between gap-4"><span className="text-muted-foreground">Size:</span><span className="font-mono">{formatBytes(dayData.size_bytes)}</span></div>
                        </>
                      ) : <div className="text-muted-foreground italic">No data files</div>}
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              )
            })}
          </div>
        )}
        <div className="mt-4 flex items-center gap-4 text-[10px] text-muted-foreground uppercase font-bold tracking-wider">
          <div className="flex items-center gap-1.5"><div className="h-2 w-2 rounded-sm bg-blue-500" /><span>Committed Data</span></div>
          <div className="flex items-center gap-1.5"><div className="h-2 w-2 rounded-sm bg-muted/30 border" /><span>No Data</span></div>
        </div>
      </CardContent>
    </Card>
  )
}

// =============================================================================
// 3. Metadata Storage Card
// =============================================================================

type TableStat = { rows: number; bytes: number | null }
type StorageResponse = {
  tables: Record<string, TableStat>; db_bytes: number | null; db_path: string
  retention: { usage_log_days: number; ingested_files_days: number; cron_runs_days: number }; ingested_files_locked?: boolean
}
type _TableStatParity = Expect<WireParity<TableStat, components['schemas']['MetadataTableStat']>>
type _RetentionParity = Expect<WireParity<StorageResponse['retention'], components['schemas']['MetadataRetentionValues']>>
type _StorageParity = Expect<WireParity<StorageResponse, components['schemas']['MetadataStorageResponse']>>

const TRIMMABLE = ['usage_log', 'ingested_files', 'cron_runs'] as const
const REFERENCE = ['alerts', 'saved_views', 'audit_log', 'in_flight_buffers', 'locally_compacted_files'] as const

const TABLE_LABELS: Record<string, { label: string; sub: string }> = {
  usage_log: { label: 'FOS / CDN usage log', sub: 'every Class A/B + CDN read recorded for cost analytics' },
  ingested_files: { label: 'Ingested files index', sub: 'one row per parsed .gz so we never re-ingest' },
  cron_runs: { label: 'Cron run history', sub: 'audit trail for sync, optimize, cleanup, etc.' },
  alerts: { label: 'Saved alerts', sub: 'user-configured alert rules' },
  saved_views: { label: 'Saved dashboard views', sub: 'user-saved filter combinations' },
  audit_log: { label: 'Admin audit log', sub: 'admin actions (settings, teardown, share invites)' },
  in_flight_buffers: { label: 'In-flight ingest buffers', sub: 'pending parquet writes mid-sync' },
  locally_compacted_files: { label: 'Local-compaction registry', sub: 'tracks compacted source files to block re-download' },
}

const fmtRetentionDays = (n: number): string => (n <= 0 ? 'disabled' : n === 1 ? '1 day' : `${n} days`)
const LOCKED_REASON = 'cron_sync.delete_after=false on this service. Raw .gz files stay in FOS forever, so ingested_files is the only thing stopping the daily full_sync from re-ingesting every aged-out file. Cleanup of this table is force-disabled regardless of the configured retention.'

type MetadataStorageRow = { name: string; trimmable: boolean; stat: TableStat }

function MetadataStorageTable({ rows, retention, dbBytes }: { rows: MetadataStorageRow[]; retention: StorageResponse['retention']; dbBytes: number | null }) {
  const columns = React.useMemo<ColumnDef<MetadataStorageRow>[]>(() => [
    {
      id: 'table', header: 'Table',
      cell: ({ row }) => {
        const r = row.original
        const { label, sub } = TABLE_LABELS[r.name] ?? { label: r.name, sub: '' }
        return (
          <>
            <div className="text-sm font-medium">{label}</div>
            <div className="text-[11px] sm:text-[10px] text-muted-foreground font-mono">{r.name}</div>
            {sub && <div className="text-[11px] sm:text-[10px] text-muted-foreground mt-0.5">{sub}</div>}
          </>
        )
      },
    },
    { id: 'rows', header: 'Rows', cell: ({ row }) => formatCountMaybe(row.original.stat.rows) ?? '–' },
    { id: 'size', header: 'Size', cell: ({ row }) => formatBytesTiered(row.original.stat.bytes, { maxUnit: 'GB' }) },
    {
      id: 'policy', header: 'Trim policy',
      cell: ({ row }) => {
        const r = row.original
        return r.trimmable ? (
          <Badge variant="secondary" className="text-[11px] sm:text-[10px]">
            {fmtRetentionDays(r.name === 'usage_log' ? retention.usage_log_days : r.name === 'ingested_files' ? retention.ingested_files_days : retention.cron_runs_days)}
          </Badge>
        ) : (
          <span className="text-xs text-muted-foreground" title="Not subject to retention cleanup — these are user/admin data, not append-only telemetry.">—</span>
        )
      },
    },
  ], [retention])
  const table = useReactTable({ data: rows, columns, getCoreRowModel: getCoreRowModel() })
  return (
    <Table className="w-full text-sm">
      <TableHeader className="bg-muted/50">
        {table.getHeaderGroups().map((hg) => (
          <TableRow key={hg.id} className="text-left">
            {hg.headers.map((header) => (
              <TableHead key={header.id} className={header.column.id === 'rows' || header.column.id === 'size' ? 'px-3 py-2 font-medium text-right' : header.column.id === 'policy' ? 'px-3 py-2 font-medium text-center' : 'px-3 py-2 font-medium'}>
                {flexRender(header.column.columnDef.header, header.getContext())}
              </TableHead>
            ))}
          </TableRow>
        ))}
      </TableHeader>
      <TableBody>
        {table.getRowModel().rows.map((row) => (
          <TableRow key={row.original.name} className="border-t">
            {row.getVisibleCells().map((cell) => (
              <TableCell key={cell.id} className={cell.column.id === 'rows' || cell.column.id === 'size' ? 'px-3 py-2 text-right tabular-nums align-top' : cell.column.id === 'policy' ? 'px-3 py-2 text-center align-top' : 'px-3 py-2'}>
                {flexRender(cell.column.columnDef.cell, cell.getContext())}
              </TableCell>
            ))}
          </TableRow>
        ))}
      </TableBody>
      <tfoot className="bg-muted/30 border-t">
        <TableRow>
          <TableCell className="px-3 py-2 font-medium text-xs uppercase tracking-wider">Total DB file</TableCell>
          <TableCell className="px-3 py-2" />
          <TableCell className="px-3 py-2 text-right font-semibold tabular-nums">{formatBytesTiered(dbBytes, { maxUnit: 'GB' })}</TableCell>
          <TableCell className="px-3 py-2" />
        </TableRow>
      </tfoot>
    </Table>
  )
}

export function MetadataStorageCard() {
  const qc = useQueryClient()
  const mounted = useMounted()
  const { data, isLoading, isFetching, error } = useQuery<StorageResponse>({
    queryKey: queryKeys.admin.metadataStorage(),
    queryFn: async () => (await unwrapClient(client.GET('/api/admin/metadata-storage'))) as StorageResponse,
    staleTime: 30_000, refetchInterval: 5 * 60_000, refetchIntervalInBackground: false, retry: 1,
  })
  const [editing, setEditing] = React.useState(false)
  const [draft, setDraft] = React.useState({ usage_log_days: '', ingested_files_days: '', cron_runs_days: '' })
  const beginEdit = () => {
    if (!data) return
    setDraft({ usage_log_days: String(data.retention.usage_log_days), ingested_files_days: String(data.retention.ingested_files_days), cron_runs_days: String(data.retention.cron_runs_days) })
    setEditing(true)
  }
  const saveRetention = useMutation({
    mutationFn: async () =>
      unwrapClient(client.PATCH('/api/admin/metadata-retention', {
        body: { usage_log_days: Math.max(0, parseInt(draft.usage_log_days, 10) || 0), ingested_files_days: Math.max(0, parseInt(draft.ingested_files_days, 10) || 0), cron_runs_days: Math.max(0, parseInt(draft.cron_runs_days, 10) || 0) },
      })),
    onSuccess: () => { setEditing(false); qc.invalidateQueries({ queryKey: queryKeys.admin.metadataStorage() }) },
  })
  const rows = data ? [...TRIMMABLE.map((name) => ({ name, trimmable: true, stat: data.tables[name] })), ...REFERENCE.map((name) => ({ name, trimmable: false, stat: data.tables[name] }))].filter((r) => r.stat) : []
  const headerAction = data ? (
    <SSEModal
      title="Run metadata cleanup now" endpoint="/api/admin/metadata-cleanup" body={{}}
      onClose={() => qc.invalidateQueries({ queryKey: queryKeys.admin.metadataStorage() })}
      description={
        <div className="space-y-2 text-sm">
          <p>Trims rows from <code className="text-xs">usage_log</code>, <code className="text-xs">ingested_files</code>, and <code className="text-xs">cron_runs</code> older than the active retention windows ({fmtRetentionDays(data.retention.usage_log_days)} / {fmtRetentionDays(data.retention.ingested_files_days)} / {fmtRetentionDays(data.retention.cron_runs_days)}), then VACUUMs the SQLite file so the on-disk size drops to reflect the deletions.</p>
          <p className="text-muted-foreground">Current file: <span className="tabular-nums font-medium">{formatBytesTiered(data.db_bytes, { maxUnit: 'GB' })}</span>. VACUUM is single-threaded and rewrites the whole file — on a multi-GB DB it can take several minutes. The run is also written to <code className="text-xs">cron_runs</code> and surfaces on the Data Management history grid.</p>
        </div>
      }
      trigger={<Button size="sm" variant="outline"><Trash2 className="h-3.5 w-3.5 mr-1.5" /> Cleanup now</Button>}
    />
  ) : undefined
  const retentionFields = data ? ([
    { key: 'usage_log_days', label: 'usage_log retention', value: data.retention.usage_log_days },
    { key: 'ingested_files_days', label: 'ingested_files retention', value: data.retention.ingested_files_days, locked: data.ingested_files_locked, lockedReason: LOCKED_REASON },
    { key: 'cron_runs_days', label: 'cron_runs retention', value: data.retention.cron_runs_days },
  ] as const) : []

  return (
    <AnalyticsCard
      title="Metadata Storage" icon={<Database className="h-4 w-4" />}
      description="Per-table row counts + on-disk size for this service's metadata.db. Daily auto-cleanup at 03:15 UTC."
      isLoading={!mounted || isLoading} isFetching={mounted && isFetching}
      error={mounted ? (error as AnalyticsCardError | null) : null} headerAction={headerAction}
    >
      {mounted && data ? (
        <div className="space-y-4">
          <div className="p-3 border rounded-lg bg-muted/30 space-y-2">
            <div className="flex items-center justify-between">
              <div className="text-[11px] sm:text-[10px] uppercase tracking-wider text-muted-foreground font-medium">Retention policy</div>
              {editing ? (
                <div className="flex items-center gap-1">
                  <Button size="sm" variant="ghost" className="h-6 px-2 text-xs" onClick={() => saveRetention.mutate()} disabled={saveRetention.isPending}><Check className="h-3 w-3 mr-1" /> {saveRetention.isPending ? 'Saving…' : 'Save'}</Button>
                  <Button size="sm" variant="ghost" className="h-6 px-2 text-xs" onClick={() => setEditing(false)} disabled={saveRetention.isPending}><X className="h-3 w-3 mr-1" /> Cancel</Button>
                </div>
              ) : (
                <Button size="sm" variant="ghost" className="h-6 px-2 text-xs" onClick={beginEdit}><Pencil className="h-3 w-3 mr-1" /> Edit</Button>
              )}
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              {retentionFields.map((f) => (
                <RetentionField key={f.key} label={f.label} editing={editing} value={f.value} draft={draft[f.key]} onDraftChange={(v) => setDraft((d) => ({ ...d, [f.key]: v }))} locked={'locked' in f ? f.locked : undefined} lockedReason={'lockedReason' in f ? f.lockedReason : undefined} />
              ))}
            </div>
            {saveRetention.isError && <div className="text-xs text-destructive pt-1">Save failed: {(saveRetention.error as Error)?.message ?? 'unknown error'}</div>}
            {editing && <div className="text-[11px] sm:text-[10px] text-muted-foreground pt-1">Set a value to 0 to disable cleanup for that table.</div>}
          </div>
          <div className="border rounded-lg overflow-x-auto">
            <MetadataStorageTable rows={rows} retention={data.retention} dbBytes={data.db_bytes} />
          </div>
        </div>
      ) : null}
    </AnalyticsCard>
  )
}

function RetentionField({ label, editing, value, draft, onDraftChange, locked, lockedReason }: {
  label: string; editing: boolean; value: number; draft: string; onDraftChange: (v: string) => void; locked?: boolean; lockedReason?: string
}) {
  return (
    <div>
      <div className="text-[11px] sm:text-[10px] uppercase tracking-wider text-muted-foreground flex items-center gap-1">
        {label}
        {locked && <span title={lockedReason} className="inline-flex items-center text-amber-500" aria-label="locked"><Lock className="h-3 w-3" aria-hidden="true" /></span>}
      </div>
      {editing ? (
        <div className="flex items-center gap-1.5 mt-1">
          <Input type="number" min={0} value={locked ? '0' : draft} onChange={(e) => onDraftChange(e.target.value)} disabled={locked} className="h-7 w-20 text-sm tabular-nums" title={locked ? lockedReason : undefined} aria-label={`${label} retention in days`} />
          <span className="text-xs text-muted-foreground">days</span>
        </div>
      ) : (
        <div className={`text-sm font-semibold tabular-nums ${locked ? 'text-muted-foreground italic' : ''}`} title={locked ? lockedReason : undefined}>
          {locked ? 'disabled (delete_after=false)' : fmtRetentionDays(value)}
        </div>
      )}
    </div>
  )
}

// =============================================================================
// 4. Scorer Time-Series & Stacked Hourly Bar Charts (folded from ScorerSeriesCard)
// =============================================================================

interface HourlyRow { hour: string; count: number }

interface StackedHourlyBarChartProps<R extends HourlyRow> {
  title: string; description: string; isLoading?: boolean; isFetching?: boolean; isEmpty?: boolean
  error?: AnalyticsCardError | null; rows: R[]; categoryKey: keyof R & string; colors: Record<string, string>
  categoryOrder?: readonly string[]; helpContent?: React.ReactNode; helpTitle?: string
}

export function StackedHourlyBarChart<R extends HourlyRow>({
  title, description, isLoading, isFetching, isEmpty, error, rows, categoryKey, colors, categoryOrder, helpContent, helpTitle,
}: StackedHourlyBarChartProps<R>) {
  const traces = React.useMemo((): PlotlyTrace[] => {
    const presentHours = Array.from(new Set(rows.map((r) => r.hour))).sort()
    const xGrid = denseTimeGrid(presentHours, 3600) ?? presentHours
    const cats = categoryOrder ?? Array.from(new Set(rows.map((r) => String(r[categoryKey]))))
    return cats.map((cat) =>
      denseBarTrace({ name: cat, rows: rows.filter((r) => String(r[categoryKey]) === cat), getTime: (r) => r.hour, getValue: (r) => r.count, intervalSeconds: 3600, color: colors[cat] ?? '#64748b', grid: xGrid }),
    )
  }, [rows, categoryKey, colors, categoryOrder])

  return (
    <AnalyticsCard title={title} description={description} isLoading={isLoading} isFetching={isFetching} isEmpty={isEmpty} error={error} className="min-h-[320px]" helpContent={helpContent} helpTitle={helpTitle}>
      <PlotlyChart data={traces} height="100%" layout={{ barmode: 'stack', showlegend: true, margin: { l: 50, r: 20, t: 10, b: 40 }, xaxis: { title: '', type: 'date' }, yaxis: { title: 'Requests', separatethousands: true, exponentformat: 'none' } }} />
    </AnalyticsCard>
  )
}

type CompRow = components['schemas']['ScoringComplianceRow']
const COMPLIANCE_COLORS: Record<string, string> = { ok: CHART_COLORS.success, missing: CHART_COLORS.neutral, tampered: '#e11d48', expired: CHART_COLORS.warning, unknown: '#7c3aed' }

export function ComplianceChart({ serviceId, sinceHours = 24 }: { serviceId: string; sinceHours?: number }) {
  const { data, isLoading, isFetching, isError, error } = useScoringQuery(['scoring-compliance', serviceId, sinceHours], () =>
    client.GET('/api/services/{service_id}/scoring/compliance-breakdown', { params: { path: { service_id: serviceId }, query: { since_hours: sinceHours } } }),
  )
  return (
    <StackedHourlyBarChart<CompRow>
      title={`Cookie compliance — last ${sinceHours}h`} helpContent={<ComplianceHelp />} helpTitle="About Cookie Compliance"
      description="Breakdown of edge_cookie_compliance per hour. 'missing' is the canonical bot signal (no cookie at all); 'tampered' is the post-cookie threat (someone modified the payload)."
      isLoading={isLoading} isFetching={isFetching} error={isError ? (error as AnalyticsCardError) : null}
      rows={data?.rows ?? []} categoryKey="compliance" colors={COMPLIANCE_COLORS}
    />
  )
}

type DistRow = components['schemas']['ScoringDistributionRow']
const BUCKETS = ['0-25', '25-50', '50-75', '75-100'] as const
const BUCKET_COLORS: Record<(typeof BUCKETS)[number], string> = { '0-25': CHART_COLORS.neutral, '25-50': '#facc15', '50-75': '#fb923c', '75-100': '#e11d48' }

export function ScoreDistChart({ serviceId, sinceHours = 24 }: { serviceId: string; sinceHours?: number }) {
  const { data, isLoading, isFetching, isError, error } = useScoringQuery(['scoring-score-dist', serviceId, sinceHours], () =>
    client.GET('/api/services/{service_id}/scoring/score-distribution', { params: { path: { service_id: serviceId }, query: { since_hours: sinceHours } } }),
  )
  return (
    <StackedHourlyBarChart<DistRow>
      title={`Score distribution — last ${sinceHours}h`} helpContent={<ScoreDistHelp />} helpTitle="About Score Distribution"
      description="Hourly counts of scored requests, bucketed by edge_score. Heavy red is the alarming bucket — sessions VCL would block under a strict policy."
      isLoading={isLoading} isFetching={isFetching} isEmpty={(data?.rows?.length ?? 0) === 0} error={isError ? (error as AnalyticsCardError) : null}
      rows={data?.rows ?? []} categoryKey="bucket" colors={BUCKET_COLORS} categoryOrder={BUCKETS}
    />
  )
}

type Derived = ReturnType<typeof deriveScorerSeries>

interface ScorerSeriesCardProps {
  serviceId: string; sinceHours: number; title: string; description: (perMinute: boolean) => string
  help: ReactNode; helpTitle: string; a11yTitle: string; toggle: { id: string; label: string }
  build: (series: Derived & { data: LatencyResponse | undefined }, toggleOn: boolean) => { traces: PlotlyTrace[]; layout: PlotlyLayout }
  showToggle?: (series: Derived & { data: LatencyResponse | undefined }) => boolean
  placeholder?: (series: Derived & { data: LatencyResponse | undefined }) => ReactNode | null
}

export function ScorerSeriesCard({
  serviceId, sinceHours, title, description, help, helpTitle, a11yTitle, toggle, build, showToggle, placeholder,
}: ScorerSeriesCardProps) {
  const { data, isLoading, isFetching, isError, error } = useScorerTimeseries(serviceId, sinceHours)
  const [toggleOn, setToggleOn] = useState(true)

  const series = { ...deriveScorerSeries(data), data }
  const { rows, perMinute } = series
  const { traces, layout } = build(series, toggleOn)
  const offerToggle = rows.length > 0 && (showToggle ? showToggle(series) : true)
  const replacement = placeholder?.(series) ?? null

  return (
    <AnalyticsCard
      title={title} description={description(perMinute)} helpContent={help} helpTitle={helpTitle}
      isLoading={isLoading} isFetching={isFetching} isEmpty={rows.length === 0}
      error={isError ? (error as AnalyticsCardError) : null} className="min-h-[320px]"
      headerAction={offerToggle ? (
        <div className="flex items-center gap-2 mr-1">
          <Switch id={toggle.id} checked={toggleOn} onCheckedChange={setToggleOn} />
          <Label htmlFor={toggle.id} className="text-xs font-normal cursor-pointer select-none text-muted-foreground hover:text-foreground">{toggle.label}</Label>
        </div>
      ) : undefined}
    >
      {replacement ?? <PlotlyChart data={traces} layout={layout} height="100%" a11yTitle={a11yTitle} />}
    </AnalyticsCard>
  )
}

const roundToTwo = (num: number): number => Math.round((num + Number.EPSILON) * 100) / 100

export function ScorerErrorsChart({ serviceId, sinceHours = 24 }: { serviceId: string; sinceHours?: number }) {
  return (
    <ScorerSeriesCard
      serviceId={serviceId} sinceHours={sinceHours} title={`Scorer errors — last ${sinceHours}h`}
      description={(perMinute) => `Fail-open errors — requests that timed out or failed auth and were let through unscored.${perMinute ? ' Bucketed per minute.' : ''}`}
      help={<ScorerErrorsHelp />} helpTitle="About Scorer Errors" a11yTitle={`Scorer fail-open errors over the last ${sinceHours} hours`}
      toggle={{ id: 'show-rate', label: 'Show rate (%)' }}
      build={({ rows, hours, perMinute }, showRate) => {
        const errorTrace = denseBarTrace({ name: 'Fail-opens', rows, getTime: (r) => r.hour, getValue: (r) => r.fail_open_count, intervalSeconds: perMinute ? 60 : 3600, color: 'rgba(225,29,72,0.45)' })
        const rateTrace = lineTrace({ name: 'Error rate (%)', x: hours, y: rows.map((r) => { const total = r.total_count ?? 0; return total > 0 ? roundToTwo(((r.fail_open_count ?? 0) / total) * 100) : 0 }), color: '#e11d48', yaxis: 'y2' })
        const base = scorerHourlyLayout(perMinute, 'Fail-opens', showRate)
        return { traces: [errorTrace, ...(showRate ? [rateTrace] : [])], layout: showRate ? withSecondaryAxis(base, 'Error rate (%)', { ticksuffix: '%' }) : base }
      }}
    />
  )
}

const LATENCY_SERIES: { name: string; get: (r: LatencyRow) => number | null | undefined; color: string; dash?: 'dot' | 'dash' }[] = [
  { name: 'p50 rtt', get: (r) => r.rtt_p50_us, color: '#60a5fa' },
  { name: 'p95 rtt', get: (r) => r.rtt_p95_us, color: CHART_COLORS.info },
  { name: 'p99 rtt', get: (r) => r.rtt_p99_us, color: '#1d4ed8' },
  { name: 'p50 exec', get: (r) => r.exec_p50_us, color: '#34d399', dash: 'dot' },
  { name: 'p95 exec', get: (r) => r.exec_p95_us, color: CHART_COLORS.success, dash: 'dot' },
]

export function ScorerLatencyChart({ serviceId, sinceHours = 24 }: { serviceId: string; sinceHours?: number }) {
  const hasLatency = ({ data, rows }: { data?: { has_latency?: boolean }; rows: LatencyRow[] }) =>
    (data?.has_latency ?? false) && rows.some((r) => r.rtt_p95_us != null)

  return (
    <ScorerSeriesCard
      serviceId={serviceId} sinceHours={sinceHours} title={`Scorer latency — last ${sinceHours}h`}
      description={(perMinute) => `Edge round-trip (p50/p95/p99) vs Wasm exec (dashed) for the Compute scorer leg.${perMinute ? ' Bucketed per minute.' : ''}`}
      help={<ScorerLatencyHelp />} helpTitle="About Scorer Latency" a11yTitle={`Scorer latency percentiles over the last ${sinceHours} hours`}
      toggle={{ id: 'show-volume', label: 'Show volume' }} showToggle={hasLatency}
      placeholder={(series) =>
        series.rows.length > 0 && !hasLatency(series) ? (
          <div className="flex h-full items-center justify-center p-4 text-center">
            <p className="max-w-xs text-xs text-muted-foreground">
              Waiting for traffic logged with the scorer latency fields (<code>edge_score_rtt_us</code> / <code>edge_score_exec_us</code>). These are added to the log format on enable and fill in once scored requests flow through and are ingested.
            </p>
          </div>
        ) : null}
      build={({ rows, hours, perMinute }, showVolume) => {
        const volumeTrace = denseBarTrace({ name: 'Inspected requests', rows, getTime: (r) => r.hour, getValue: (r) => r.total_count, intervalSeconds: perMinute ? 60 : 3600, color: 'rgba(148, 163, 184, 0.12)', yaxis: 'y2', showlegend: true })
        const latencyTraces = LATENCY_SERIES.map((s) => lineTrace({ name: s.name, x: hours, y: rows.map((r) => usToMs(s.get(r))), color: s.color, dash: s.dash, connectgaps: true }))
        const base = scorerHourlyLayout(perMinute, 'Round-trip / exec (ms)', true)
        return { traces: [...(showVolume ? [volumeTrace] : []), ...latencyTraces], layout: showVolume ? withSecondaryAxis(base, 'Inspected requests') : base }
      }}
    />
  )
}
