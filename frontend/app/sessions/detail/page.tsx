'use client'

import { Suspense, useMemo, useState } from 'react'
import Link from 'next/link'
import { useSearchParams } from 'next/navigation'
import { useQuery } from '@tanstack/react-query'
import { Activity, ArrowDown, ArrowLeft } from 'lucide-react'

import { DataTable } from '@/components/DataTable'
import { NoServiceSelected } from '@/components/ReportLayout'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button, buttonVariants } from '@/components/ui/button'
import { PageHeader } from '@/components/ui/confirm-dialog'
import { Skeleton } from '@/components/ui/skeleton'
import { useBootstrapResolved, useEffectiveServiceId, useFieldLabel } from '@/hooks/useBootstrap'
import { useDateFormat } from '@/hooks/useSSE'
import { client, extractApiError, unwrapClient } from '@/lib/api'
import { readCloudSessionReference } from '@/lib/cloud-engine/session-reference'
import type { ColumnDef } from '@/lib/table'
import { buildServiceHref, buildSessionDashboardUrl, formatCount } from '@/lib/utils'
import type { SessionDetailResponse } from '@/types/api'
import { analyzeTrace, durationMs, type Lane, type RequestRow } from './trace'

const LANES: Lane[] = ['Edge', 'Shield', 'Unspecified']
const VISIBLE_COLUMNS = ['timestamp', 'host', 'url', 'method', 'edge', 'status', 'cache', 'ua', 'pop', 'elapsed', 'tcp_rtt']
const DETAIL_LIMIT = 500

function offset(ms: number) {
  return `+${Number((ms / 1000).toFixed(3))}s`
}

function Loading() {
  return <div role="status" aria-label="Loading session details" className="space-y-4">
    <Skeleton className="h-24 w-full" />
    <Skeleton className="h-64 w-full" />
  </div>
}

function Waveform({ trace }: { trace: ReturnType<typeof analyzeTrace> }) {
  if (trace.start === undefined || trace.end === undefined) {
    return <p className="text-sm text-muted-foreground">No request timestamps were logged.</p>
  }
  return (
    <div role="img" aria-label="Request activity over time by hop. Error evidence is listed below." className="space-y-2">
      {LANES.map((lane) => {
        const buckets = trace.buckets.get(lane)
        if (!buckets) return null
        return (
          <div key={lane} className="grid grid-cols-[5.5rem_1fr] items-center gap-2 sm:gap-4">
            <span className="font-mono text-xs text-muted-foreground">{lane}</span>
            <div className="flex h-9 gap-0.5 border-b border-border bg-muted/40 px-1 sm:gap-1">
              {buckets.map(({ slot, count, errors }) => (
                <div key={slot} className="flex min-w-0 flex-1 items-end">
                  {count > 0 && (
                    <div className="flex w-full flex-col overflow-hidden rounded-t-sm bg-primary/65"
                      style={{ height: `${Math.max(14, 100 * count / trace.peak)}%` }}>
                      {errors > 0 && <div className="min-h-0.5 bg-destructive" style={{ height: `${100 * errors / count}%` }} />}
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        )
      })}
      <div className="ml-24 flex justify-between font-mono text-xs text-muted-foreground sm:ml-26">
        <span>+0s</span><span>{offset(trace.end - trace.start)}</span>
      </div>
    </div>
  )
}

function SessionTrace({ detail, serviceId }: { detail: SessionDetailResponse; serviceId: string }) {
  const trace = useMemo(() => analyzeTrace(detail.data), [detail.data])
  const [scope, setScope] = useState<'all' | 'failures' | 'recovery' | 'slowest'>('all')
  const [edgeOnly, setEdgeOnly] = useState(false)
  const { full, abbr } = useDateFormat()
  const fieldLabel = useFieldLabel()
  const columns = useMemo<ColumnDef<RequestRow>[]>(() => detail.columns.map((key) => ({
    id: key,
    header: key === 'elapsed' ? 'Elapsed (ms)' : key === 'tcp_rtt' ? 'TCP RTT (ms)' : fieldLabel(key),
    accessorFn: (row) => row[key],
    cell: ({ row }) => {
      const value = row.original[key]
      if (key === 'elapsed' || key === 'tcp_rtt') return durationMs(value)?.toFixed(1) ?? '—'
      const label = typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean' ? String(value) : ''
      return <span className="inline-block max-w-[24rem] truncate align-middle" title={label}>{label || '—'}</span>
    },
  })), [detail.columns, fieldLabel])
  const visibility = useMemo(() => Object.fromEntries(detail.columns.map((key) => [key, VISIBLE_COLUMNS.includes(key)])), [detail.columns])

  const observed = trace.requests[0].row
  const start = trace.start === undefined ? null : new Date(trace.start).toISOString()
  const end = trace.end === undefined ? null : new Date(trace.end).toISOString()
  const pivotEnd = trace.end === undefined ? null : new Date(trace.end + 1).toISOString()
  const { firstFailure, recovery, slowest } = trace
  const scoped = scope === 'failures' ? trace.failures
    : scope === 'recovery' ? trace.evidence
      : scope === 'slowest' && slowest ? [slowest] : trace.requests
  const visible = scope === 'all' && edgeOnly ? scoped.filter(({ lane }) => lane === 'Edge') : scoped
  const summary = [
    ['Requests shown', formatCount(detail.data.length)],
    ['Unique URLs shown', detail.columns.includes('url') ? formatCount(trace.urls) : '—'],
    ['HTTP errors shown', trace.statuses ? `${formatCount(trace.failures.length)} / ${formatCount(trace.statuses)}` : '—'],
    ['Median TCP RTT', trace.medianRtt === null ? '—' : `${trace.medianRtt.toFixed(1)} ms`],
  ]

  return (
    <>
      {detail.data.length >= DETAIL_LIMIT && (
        <Alert role="status">
          <AlertTitle>Partial data</AlertTitle>
          <AlertDescription>The request limit is {DETAIL_LIMIT}. Later activity may not be shown; all findings describe the records below.</AlertDescription>
        </Alert>
      )}
      <section aria-labelledby="request-trace" className="overflow-hidden rounded-xl border bg-card">
        <div className="border-b bg-muted/30 px-4 py-4 sm:px-6">
          <div className="flex flex-wrap items-baseline justify-between gap-2">
            <h2 id="request-trace" className="flex items-center gap-2 text-lg font-semibold tracking-tight"><Activity className="size-4 text-primary" />Request trace</h2>
            {start && end && (
              <p className="font-mono text-xs text-muted-foreground">{full(start)}{end !== start && <> → {full(end)}</>} {abbr()}</p>
            )}
          </div>
          <dl className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
            {summary.map(([label, value]) => (
              <div key={label}>
                <dt className="text-xs text-muted-foreground">{label}</dt>
                <dd className="mt-0.5 font-mono text-xl font-semibold tabular-nums">{value}</dd>
              </div>
            ))}
          </dl>
        </div>
        <div className="space-y-4 px-4 py-5 sm:px-6">
          <Waveform trace={trace} />
          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
            <span><span className="mr-1.5 inline-block size-2 rounded-sm bg-primary/70" />Requests</span>
            <span><span className="mr-1.5 inline-block size-2 rounded-sm bg-destructive" />4xx / 5xx</span>
            <span>Time since first recorded request</span>
          </div>
          <div className="flex flex-wrap items-center gap-2 border-t pt-4">
            {trace.failures.length > 0 ? (
              <Button variant="outline" aria-pressed={scope === 'failures'} onClick={() => setScope('failures')}>
                {firstFailure && trace.start !== undefined
                  ? `First HTTP error: ${firstFailure.status} at ${offset(firstFailure.time - trace.start)}`
                  : 'Show HTTP errors'} <ArrowDown className="size-3" />
              </Button>
            ) : (
              <p className="text-sm text-muted-foreground">
                {trace.statuses ? 'No HTTP errors observed in logged statuses.' : 'No HTTP statuses were logged.'}
              </p>
            )}
            {firstFailure?.endpoint && (
              <Button variant="outline" aria-pressed={scope === 'recovery'} onClick={() => setScope('recovery')}>
                {recovery && trace.start !== undefined
                  ? trace.completionOrdered && recovery.elapsed !== null
                    ? `Later 2xx completed at ${offset(recovery.time + recovery.elapsed - trace.start)}`
                    : `Later 2xx started at ${offset(recovery.time - trace.start)}`
                  : 'No later 2xx for first error'} <ArrowDown className="size-3" />
              </Button>
            )}
            {slowest && slowest.elapsed !== null && (
              <Button variant="outline" aria-pressed={scope === 'slowest'} onClick={() => setScope('slowest')}>
                Slowest request: {slowest.elapsed.toFixed(1)} ms <ArrowDown className="size-3" />
              </Button>
            )}
          </div>
          {firstFailure?.label && (
            <p className="break-all text-xs text-muted-foreground">
              First error: <span className="font-mono text-foreground">{firstFailure.label}</span>
              {trace.retryCount > 0 && ` · ${trace.retryCount} subsequent ${trace.retryCount === 1 ? 'error' : 'errors'} on the same request target before the first later-starting 2xx, or the end of the available records.`}
            </p>
          )}
        </div>
      </section>

      <details className="rounded-lg border bg-card px-4 py-3">
        <summary className="cursor-pointer text-sm font-medium">Client and network</summary>
        <dl className="mt-3 grid grid-cols-2 gap-3 text-sm sm:grid-cols-3">
          {([['IP address', 'ip'], ['JA4 fingerprint', 'ja4'], ['User agent', 'ua']] as const).map(([label, field]) => {
            const value = observed[field]
            return <div key={field} className="min-w-0">
              <dt className="text-xs text-muted-foreground">{label}</dt>
              <dd className="break-all">{typeof value === 'string' && value && value !== '0.0.0.0' && value !== '[redacted]'
                ? <Link className="underline underline-offset-2" href={buildSessionDashboardUrl(serviceId, field, value, start, pivotEnd)}>{value}</Link>
                : value ? 'Masked' : '—'}</dd>
            </div>
          })}
          <div><dt className="text-xs text-muted-foreground">Country</dt><dd>{observed.country || '—'}</dd></div>
          <div><dt className="text-xs text-muted-foreground">ASN</dt><dd>{observed.asn ? `AS${observed.asn}` : '—'}</dd></div>
          <div>
            <dt className="text-xs text-muted-foreground">Edge / shield shown</dt>
            <dd>{trace.edgeCount || trace.shieldCount
              ? `${trace.edgeCount} / ${trace.shieldCount}`
              : '—'}</dd>
          </div>
        </dl>
      </details>

      <section aria-labelledby="request-records" className="min-w-0 space-y-3">
        <div className="flex flex-wrap items-center gap-3">
          <h2 id="request-records" className="text-lg font-semibold tracking-tight">Request records</h2>
          <span aria-live="polite" className="text-xs text-muted-foreground">{formatCount(visible.length)} shown</span>
          {scope !== 'all' && (
            <Button variant="outline" size="sm" onClick={() => { setEdgeOnly(false); setScope('all') }}>Show all requests</Button>
          )}
          {scope === 'all' && trace.edgeCount > 0 && (
            <label className="flex items-center gap-1.5 text-sm">
              <input type="checkbox" checked={edgeOnly} onChange={(event) => setEdgeOnly(event.target.checked)} />
              Edge only
            </label>
          )}
        </div>
        <DataTable columns={columns} data={visible.map(({ row }) => row)} initialVisibility={visibility} initialColumnOrder={VISIBLE_COLUMNS} />
      </section>
    </>
  )
}

function SessionDetailClient() {
  const params = useSearchParams()
  const token = params.get('token')
  const reference = params.get('ref')
  const hasSession = Boolean(token || reference)
  const activeServiceId = useEffectiveServiceId()
  const serviceId = params.get('service') || activeServiceId
  const bootstrapResolved = useBootstrapResolved()
  const detail = useQuery({
    queryKey: ['sessions', 'detail-page', serviceId, reference, token],
    queryFn: ({ signal }) => {
      const sessionToken = reference ? readCloudSessionReference(reference) : token
      if (!sessionToken || !serviceId) {
        throw new Error('Session link unavailable. Reopen Session Details from the Sessions list in this tab.')
      }
      return unwrapClient(client.POST('/api/sessions/detail', {
        signal, headers: { 'x-service-id': serviceId }, body: { session_token: sessionToken },
      }))
    },
    enabled: !!serviceId && hasSession,
  })

  if (hasSession && !serviceId && bootstrapResolved) {
    return <NoServiceSelected icon={Activity} message="Select a service to view session details." />
  }

  return (
    <div className="space-y-5">
      <PageHeader title="Session Details" icon={Activity} description="Inspect the requests and responses recorded for one session.">
        <Link href={buildServiceHref('/sessions', serviceId)} className={buttonVariants({ variant: 'outline', size: 'sm' })}>
          <ArrowLeft className="size-4" /> Sessions
        </Link>
      </PageHeader>
      {!hasSession ? (
        <Alert role="status"><AlertTitle>No session token</AlertTitle><AlertDescription>Open Session Details from a session in the Sessions list.</AlertDescription></Alert>
      ) : !serviceId || detail.isPending ? (
        <Loading />
      ) : detail.isError ? (
        <Alert variant="destructive">
          <AlertTitle>Could not load session details</AlertTitle>
          <AlertDescription>
            <p>{extractApiError(detail.error)}</p>
            <Button className="mt-2" variant="outline" size="sm" onClick={() => void detail.refetch()}>Retry</Button>
          </AlertDescription>
        </Alert>
      ) : detail.data?.data.length ? (
        <SessionTrace key={JSON.stringify([serviceId, reference, token])} detail={detail.data} serviceId={serviceId} />
      ) : (
        <Alert role="status"><AlertTitle>No requests found</AlertTitle><AlertDescription>No records are available for this session in your accessible time window.</AlertDescription></Alert>
      )}
    </div>
  )
}

export default function SessionDetailPage() {
  return <Suspense fallback={<Loading />}><SessionDetailClient /></Suspense>
}
