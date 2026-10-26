'use client'
import * as React from 'react'
import { queryOptions, useMutation, useQuery, useQueryClient, type QueryClient, type QueryKey, type UseQueryOptions } from '@tanstack/react-query'
import { useForm } from 'react-hook-form'
import { z } from 'zod'
import { Activity, AlertTriangle, Bookmark, Brain, Check, CheckCircle, CheckCircle2, Clock, Filter, Gauge, History, Info, KeyRound, LineChart, ListChecks, Loader2, Lock, Pencil, RotateCcw, ScrollText, ServerCrash, Shield, ShieldAlert, ShieldCheck, ShieldOff, SlidersHorizontal, Trash2, Users, XCircle } from 'lucide-react'
import type { ColumnDef } from '@tanstack/react-table'
import { AnalyticsCard, CardErrorState } from '@/components/AnalyticsCard'
import { DataTableReadonly, DateTimeCell } from '@/components/DataTable'
import { SessionEventsDialog } from './TopFlaggedTable'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { ConfirmDialog, CopyButton } from '@/components/ui/confirm-dialog'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Skeleton, SkeletonGrid } from '@/components/ui/skeleton'
import { Slider } from '@/components/ui/slider'
import { useDebounce } from '@/hooks/useDashboardCards'
import { Textarea } from '@/components/ui/textarea'
import { useAutoFlash, useDeferredDeleteWithUndo, useDerivedState, useOpenGeneration } from '@/hooks/useSSE'
import { client, extractApiError, unwrapClient, queryKeys } from "@/lib/api"
import { formatBytes, formatCount, formatDeterministicUTC, formatPercent, formatTimeAgo, keyByOccurrence } from "@/lib/utils"
import type { Expect, Simplify, WireParity, WithRequired, WithRequiredNullable, components } from '@/types/api'
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
interface ThresholdSliderControlsProps { thresholdRaw: number
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
export function ThresholdSliderControls({ thresholdRaw, onThresholdRawChange, committed, enforce, statusCode, effectiveStatusCode, isAlreadyCommitted,
  isEnforcingThis, commitPending, enforcePending, statusCodePending, codeDraft, onCodeDraftChange, codeDraftValid, codeDraftIsDirty, codeDraftNum, onCommitClick,
  onEnforceClick, onApplyStatusCode, onResetStatusCode, }: ThresholdSliderControlsProps) { return ( <div className="space-y-2">
      <div className="flex items-baseline justify-between">
        <label className="text-xs font-medium text-muted-foreground">
          Score threshold
          {committed?.threshold != null && ( <span className="ml-2 text-[10px] text-muted-foreground">
              · committed: <span className="font-mono">{committed.threshold}</span>
            </span>
          )}
        </label>
        <div className="flex items-center gap-2">
          <span className="font-mono text-lg font-semibold tabular-nums">
            {thresholdRaw}
          </span>
          <Button variant={isAlreadyCommitted ? 'outline' : 'default'} size="sm" disabled={commitPending || isAlreadyCommitted} onClick={onCommitClick}
            className="h-7 text-xs"
            title="Persist this as your committed threshold (preview only — does NOT push to Compute)" >
            {commitPending ? ( <Loader2 className="h-3 w-3 animate-spin mr-1" />
            ) : isAlreadyCommitted ? ( <Check className="h-3 w-3 mr-1" />
            ) : null}
            {isAlreadyCommitted ? 'Committed' : 'Commit'}
          </Button>
          <Button variant={isEnforcingThis ? 'outline' : 'destructive'} size="sm" disabled={enforcePending} onClick={onEnforceClick} className="h-7 text-xs" title={
              isEnforcingThis
                ? 'Currently ENFORCING this threshold. Click to disable enforcement.'
                : `Push this threshold to Compute. Live requests with score >= threshold will be blocked (HTTP ${effectiveStatusCode}).`
            } >
            {enforcePending ? ( <Loader2 className="h-3 w-3 animate-spin mr-1" />
            ) : isEnforcingThis ? ( <ShieldOff className="h-3 w-3 mr-1" />
            ) : ( <ShieldCheck className="h-3 w-3 mr-1" />
            )}
            {isEnforcingThis ? 'Disable' : 'Enforce'}
          </Button>
        </div>
      </div>
      {enforce?.enforced && ( <div className="text-[10px] text-destructive">
          ⚠ LIVE: enforcing at threshold{' '}
          <span className="font-mono">{enforce.threshold}</span> — requests with score ≥ threshold are returning HTTP{' '}
          <span className="font-mono">{effectiveStatusCode}</span>.
        </div>
      )}
      <div className="flex items-center gap-2 flex-wrap text-[11px]">
        <label className="text-muted-foreground" htmlFor="enforce-status-code">
          Enforce response code:
        </label>
        <input id="enforce-status-code" type="number" min={statusCode?.min ?? 400} max={statusCode?.max ?? 599} step={1} value={codeDraft}
          onChange={(e) => onCodeDraftChange(e.target.value)}
          disabled={statusCodePending}
          className="h-6 w-16 rounded border bg-background px-1.5 text-[11px] font-mono"
          title="Any HTTP 4xx/5xx code (e.g. 403 Forbidden, 429 Too Many Requests, 451 Legal, 503 Service Unavailable). Reason phrase auto-mapped from the HTTP standard."
          aria-label="Enforce response code" />
        <Button size="sm" variant={codeDraftIsDirty ? 'default' : 'outline'} className="h-6 text-[11px]" disabled={!codeDraftIsDirty || statusCodePending}
          onClick={onApplyStatusCode}
          title={ codeDraftIsDirty
              ? `Re-deploy the enforce snippet so flagged requests return HTTP ${codeDraftNum}`
              : 'No change to publish'
          } >
          Apply
        </Button>
        {statusCode && !statusCode.is_default && ( <button
            type="button"
            disabled={statusCodePending}
            onClick={onResetStatusCode}
            className="text-[10px] text-muted-foreground underline hover:text-foreground"
            title={`Reset to default (${statusCode.default})`}
          >
            reset
          </button>
        )}
        {statusCodePending && ( <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />
        )}
        {codeDraft !== '' && !codeDraftValid && ( <span className="text-[10px] text-destructive">
            must be {statusCode?.min ?? 400}–{statusCode?.max ?? 599}
          </span>
        )}
      </div>
      <Slider value={[thresholdRaw]} onValueChange={(v) => onThresholdRawChange(v[0] ?? 75)} min={0} max={100} step={5} className="w-full"
        aria-label="Score threshold" />
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
  components['schemas']['ScoringThresholdBucket'], 'total' | 'good' | 'bad' | 'unlabeled' >
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
