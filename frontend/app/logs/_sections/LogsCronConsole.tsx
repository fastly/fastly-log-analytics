'use client'

import React, { useEffect, useRef } from 'react'
import { Database, RefreshCw, Loader2, ChevronDown, X } from 'lucide-react'
import { useVirtualizer } from '@tanstack/react-virtual'
import { cn } from '@/lib/utils'
import { useSSE, useDateFormat } from '@/hooks/useSSE'
import { useCapabilities } from '@/hooks/useBootstrap'
import { isCloudMode } from '@/lib/cloud-engine/mode'
import type { CronRun } from './useLogsPageState'

type ConsoleLine = { _id?: string; type: string; message?: string; file_name?: string; [key: string]: unknown }

function formatCloudCronRunLines(job?: CronRun): ConsoleLine[] {
  if (!job) {
    return [{ _id: 'cloud-empty', type: 'status', message: 'No details recorded for this Cloud cron run.' }]
  }
  const out: ConsoleLine[] = []
  const raw = job as Record<string, unknown>
  const summary = typeof job.summary === 'string' && job.summary.trim() ? job.summary : `${job.task}: ${job.status ?? 'unknown'}`
  out.push({ _id: 'cloud-summary', type: job.status === 'error' ? 'error' : 'status', message: summary })

  const meta: string[] = []
  if (typeof raw.run_id === 'string' && raw.run_id) meta.push(`run_id=${raw.run_id}`)
  if (typeof raw.outcome === 'string' && raw.outcome) meta.push(`outcome=${raw.outcome}`)
  if (typeof job.duration_s === 'number') meta.push(`duration=${Math.round(job.duration_s * 1000)}ms`)
  if (typeof raw.vcpu_ms === 'number') meta.push(`vcpu=${raw.vcpu_ms}ms`)
  if (typeof raw.trigger === 'string' && raw.trigger) meta.push(`trigger=${raw.trigger}`)
  if (meta.length > 0) {
    out.push({ _id: 'cloud-meta', type: 'log', message: meta.join('  ') })
  }

  const counts: string[] = []
  if (typeof raw.listed_files === 'number') counts.push(`listed=${raw.listed_files}`)
  const converted = job.files_downloaded ?? (typeof raw.converted_files === 'number' ? raw.converted_files : null)
  if (typeof converted === 'number') counts.push(`converted=${converted}`)
  const deleted = job.files_deleted_fos ?? (typeof raw.deleted_files === 'number' ? raw.deleted_files : null)
  if (typeof deleted === 'number') counts.push(`deleted=${deleted}`)
  if (typeof job.rows_ingested === 'number') counts.push(`rows=${job.rows_ingested}`)
  const parquet = job.parquet_files_created ?? (typeof raw.parquet_files_written === 'number' ? raw.parquet_files_written : null)
  if (typeof parquet === 'number') counts.push(`parquet=${parquet}`)
  if (typeof job.corrupt_rows === 'number' && job.corrupt_rows > 0) counts.push(`bad_lines=${job.corrupt_rows}`)
  if (typeof raw.more_pending === 'boolean') counts.push(`more_pending=${raw.more_pending}`)
  if (counts.length > 0) {
    out.push({ _id: 'cloud-counts', type: 'file_done', message: counts.join('  ') })
  }

  const resourceBits: string[] = []
  if (typeof raw.backend_requests_used === 'number') resourceBits.push(`backend_calls=${raw.backend_requests_used}`)
  if (typeof raw.kv_ops === 'number') resourceBits.push(`kv_ops=${raw.kv_ops}`)
  let details: Record<string, unknown> | null = null
  if (typeof raw.details_json === 'string' && raw.details_json.trim()) {
    try {
      const parsed = JSON.parse(raw.details_json)
      if (parsed && typeof parsed === 'object') details = parsed as Record<string, unknown>
    } catch {
      // Ignore malformed details_json
    }
  }
  const exec = details?.execution_metrics as Record<string, unknown> | undefined
  if (exec && typeof exec.heap_mib === 'number') resourceBits.push(`heap=${exec.heap_mib}MiB`)
  if (resourceBits.length > 0) {
    out.push({ _id: 'cloud-res', type: 'log', message: resourceBits.join('  ') })
  }

  const workers = Array.isArray(details?.worker_executions_metrics)
    ? (details.worker_executions_metrics as Array<Record<string, unknown>>)
    : []
  workers.forEach((w, idx) => {
    const parts = [
      `worker [${String(w.kind ?? 'worker')}${w.shard != null ? ` shard=${String(w.shard)}` : ''}]`,
      `status=${String(w.status ?? 'unknown')}`,
    ]
    if (typeof w.duration_ms === 'number') parts.push(`duration=${w.duration_ms}ms`)
    if (typeof w.vcpu_ms === 'number') parts.push(`vcpu=${w.vcpu_ms}ms`)
    if (typeof w.calls === 'number') parts.push(`calls=${w.calls}`)
    out.push({
      _id: `cloud-worker-${idx}`,
      type: w.status === 'error' ? 'error' : 'file_done',
      message: parts.join('  '),
    })
  })

  if (typeof job.error_message === 'string' && job.error_message.trim()) {
    out.push({ _id: 'cloud-error', type: 'error', message: `Error: ${job.error_message}` })
  }
  out.push({ _id: 'cloud-done', type: 'done', message: 'Completed.' })
  return out
}

export function CronLiveLog({
  runId,
  job,
  singleLine = false,
  terminalMode = !singleLine,
  startedAt,
  onDone,
}: {
  runId: number | string | undefined
  job?: CronRun
  singleLine?: boolean
  terminalMode?: boolean
  startedAt?: string
  onDone?: () => void
}) {
  const capabilities = useCapabilities()
  const cloudMode = capabilities.edition === 'cloud' || isCloudMode()
  const isPersistedCloudRun = cloudMode && runId !== undefined && runId !== 'cloud-tick'
  const { lines: sseLines, status: sseStatus, start, stop } = useSSE()
  const startedForRunId = useRef<number | string | undefined>(undefined)
  const doneFiredForRunId = useRef<number | string | undefined>(undefined)
  const { full, abbr } = useDateFormat()

  useEffect(() => {
    if (isPersistedCloudRun) return
    if (runId && startedForRunId.current !== runId) {
      startedForRunId.current = runId
      doneFiredForRunId.current = undefined
      start(`/api/cron-runs/${runId}/stream`)
    }
    return () => {
      if (startedForRunId.current !== undefined) {
        stop()
        startedForRunId.current = undefined
      }
    }
  }, [isPersistedCloudRun, runId, start, stop])

  const lines: ConsoleLine[] = isPersistedCloudRun ? formatCloudCronRunLines(job) : (sseLines as ConsoleLine[])
  const status = isPersistedCloudRun ? 'done' : sseStatus

  useEffect(() => {
    if ((status === 'done' || status === 'error') && onDone && doneFiredForRunId.current !== runId) {
      doneFiredForRunId.current = runId
      onDone()
    }
  }, [status, onDone, runId])

  const recentLines = singleLine ? lines.slice(-1) : terminalMode ? lines : lines.slice(-2)
  const scrollParentRef = useRef<HTMLDivElement>(null)
  const rowVirtualizer = useVirtualizer({
    count: terminalMode && !singleLine ? recentLines.length : 0,
    getScrollElement: () => scrollParentRef.current,
    estimateSize: () => 22,
    overscan: 20,
  })
  const renderLine = (line: (typeof lines)[number], truncateAt80: boolean) => {
    let text =
      (line.message as string) ||
      (line.type === 'file_done'
        ? `Processed ${line.file_name}`
        : line.type === 'done'
          ? 'Done.'
          : JSON.stringify(line))
    if (truncateAt80 && text.length > 80) text = text.substring(0, 80) + '...'
    const cls =
      line.type === 'error'
        ? truncateAt80
          ? 'text-red-500'
          : 'text-red-400 font-medium'
        : line.type === 'done'
          ? truncateAt80
            ? 'text-emerald-500'
            : 'text-emerald-400 font-semibold'
          : line.type === 'status'
            ? 'text-blue-400'
            : line.type === 'file_done'
              ? truncateAt80
                ? 'text-muted-foreground'
                : 'text-zinc-400'
              : undefined
    return {
      text,
      node: (
        <span className={cls}>
          {line.type === 'file_done' && line.file_name ? `Processed ${String(line.file_name)}` : text}
        </span>
      ),
    }
  }

  if (recentLines.length === 0) {
    if (singleLine) {
      return (
        <div className="flex items-center gap-1.5 text-[9px] font-mono text-muted-foreground truncate h-full w-full">
          {status === 'streaming' ? (
            <>
              <Loader2 className="w-3 h-3 animate-spin shrink-0" /> <span className="truncate">Loading logs...</span>
            </>
          ) : status === 'error' ? (
            <span className="text-red-500 truncate">Failed to load logs.</span>
          ) : (
            <span className="text-muted-foreground/50 truncate italic">Waiting...</span>
          )}
        </div>
      )
    }
    return (
      <div className="flex flex-col gap-1 w-full font-mono text-xs text-zinc-400 py-1">
        {startedAt && (
          <div className="text-zinc-500 border-b border-zinc-900 pb-1.5 mb-1.5">
            [SESSION INITIATED AT: {full(startedAt)} {abbr()}]
          </div>
        )}
        <div className="flex items-center gap-2">
          {status === 'streaming' ? (
            <>
              <Loader2 className="w-3.5 h-3.5 animate-spin shrink-0 text-blue-400" />{' '}
              <span>Streaming console output...</span>
            </>
          ) : status === 'error' ? (
            <span className="text-red-400">Failed to stream logs.</span>
          ) : (
            <span className="text-zinc-500 italic">Waiting for stream...</span>
          )}
        </div>
      </div>
    )
  }

  if (singleLine) {
    return (
      <div className="flex flex-col text-[9px] font-mono text-muted-foreground truncate w-full h-full justify-center">
        {recentLines.map((line, i) => {
          const { text, node } = renderLine(line, true)
          return (
            <div key={line._id ?? `idx-${i}`} className="truncate w-full" title={typeof line.message === 'string' ? line.message : text}>
              {node}
            </div>
          )
        })}
      </div>
    )
  }

  const virtualItems = terminalMode ? rowVirtualizer.getVirtualItems() : []
  const displayLines =
    virtualItems.length > 0 ? virtualItems.map((v) => ({ line: recentLines[v.index], index: v.index })) : recentLines.map((line, index) => ({ line, index }))

  return (
    <div
      ref={scrollParentRef}
      role="log"
      aria-live="polite"
      aria-atomic="false"
      aria-label={terminalMode ? 'Cron job output stream' : 'Recent cron output'}
      className={
        terminalMode
          ? 'flex flex-col gap-1.5 w-full font-mono text-xs leading-relaxed text-zinc-200 max-h-[360px] overflow-y-auto'
          : 'flex flex-col gap-0.5 mt-1.5 bg-background/50 border border-border/50 p-2 rounded text-[10px] font-mono text-muted-foreground w-[280px] max-w-sm overflow-hidden'
      }
    >
      {terminalMode && startedAt && (
        <div className="text-zinc-500 border-b border-zinc-900 pb-1.5 mb-1.5">
          [SESSION INITIATED AT: {full(startedAt)} {abbr()}]
        </div>
      )}
      {displayLines.map(({ line, index: i }) => {
        const { text, node } = renderLine(line, false)
        return (
          <div
            key={line._id ?? `idx-${i}`}
            className={terminalMode ? 'whitespace-pre-wrap break-all w-full text-zinc-300' : 'truncate w-full'}
            title={typeof line.message === 'string' ? line.message : text}
          >
            {node}
          </div>
        )
      })}
    </div>
  )
}

export function FloatingOperationsDock({
  displayedJobs,
  setDisplayedJobs,
  isOpen,
  setIsOpen,
  selectedJobId,
  setSelectedJobId,
  onDismiss,
}: {
  displayedJobs: CronRun[]
  setDisplayedJobs: React.Dispatch<React.SetStateAction<CronRun[]>>
  isOpen: boolean
  setIsOpen: (open: boolean) => void
  selectedJobId: number | string | null
  setSelectedJobId: (id: number | string | null) => void
  onDismiss: (id: number) => void
}) {
  const { full, abbr } = useDateFormat()
  if (displayedJobs.length === 0) return null
  const activeJob = displayedJobs.find((j) => j.id === selectedJobId) || displayedJobs[0]
  const runningCount = displayedJobs.filter((j) => j.status === 'running').length

  return (
    <div className="fixed bottom-6 right-6 z-50 flex flex-col items-end gap-2 pointer-events-auto">
      {!isOpen ? (
        <button
          onClick={() => setIsOpen(true)}
          aria-expanded={false}
          aria-controls="floating-ops-panel"
          aria-label={
            runningCount > 0
              ? `${runningCount} active operation${runningCount > 1 ? 's' : ''} running, open console`
              : `${displayedJobs.length} completed operation${displayedJobs.length > 1 ? 's' : ''}, open console`
          }
          className={cn(
            'px-4 py-2.5 w-[260px] justify-center rounded-full text-xs font-semibold flex items-center gap-2.5 shadow-2xl transition-colors duration-200 cursor-pointer border',
            runningCount > 0
              ? 'bg-blue-600 hover:bg-blue-700 text-white border-blue-500/20'
              : 'bg-zinc-850 hover:bg-zinc-800 text-zinc-300 border-zinc-700/50',
          )}
        >
          {runningCount > 0 ? (
            <RefreshCw className="h-3.5 w-3.5 animate-spin text-blue-200" />
          ) : (
            <Database className="h-3.5 w-3.5 text-zinc-400" />
          )}
          <span>
            {runningCount > 0
              ? `${runningCount} active operation${runningCount > 1 ? 's' : ''} running...`
              : `${displayedJobs.length} completed operation${displayedJobs.length > 1 ? 's' : ''} (logs)`}
          </span>
        </button>
      ) : (
        <div
          id="floating-ops-panel"
          className="bg-zinc-950 text-zinc-100 border border-zinc-800 rounded-lg shadow-2xl w-[440px] sm:w-[500px] h-[380px] flex flex-col overflow-hidden animate-in slide-in-from-bottom-5 duration-300"
        >
          <div className="flex items-center justify-between px-3 py-2 bg-zinc-900 border-b border-zinc-800 shrink-0">
            <div className="flex items-center gap-2 text-xs font-semibold text-zinc-300">
              <Database className="h-3.5 w-3.5 text-blue-500" />
              <span>Console Log Terminal</span>
            </div>
            <button
              onClick={() => setIsOpen(false)}
              aria-expanded={true}
              aria-controls="floating-ops-panel"
              aria-label="Minimize console"
              className="text-zinc-400 hover:text-zinc-200 p-1 hover:bg-zinc-800 rounded cursor-pointer transition-colors"
            >
              <ChevronDown className="h-4 w-4" />
            </button>
          </div>
          {displayedJobs.length > 1 && (
            <div className="flex border-b border-zinc-800 bg-zinc-900/50 overflow-x-auto scrollbar-none shrink-0 px-2 pt-1 gap-1">
              {displayedJobs.map((job) => {
                const isActive = job.id === selectedJobId
                const taskLabel = job.task
                return (
                  <div
                    key={job.id}
                    className={cn(
                      'rounded-t text-[10px] font-mono uppercase tracking-wider flex items-center gap-1.5 border-t border-x transition-all shrink-0',
                      isActive
                        ? 'bg-zinc-950 text-blue-400 border-zinc-800 border-b-zinc-950 font-bold'
                        : 'bg-transparent text-zinc-400 border-transparent hover:text-zinc-200 hover:bg-zinc-800/30',
                    )}
                  >
                    <button
                      type="button"
                      onClick={() => setSelectedJobId(job.id)}
                      className="pl-3 py-1.5 flex items-center gap-1.5 cursor-pointer uppercase tracking-wider"
                    >
                      <span
                        className={cn(
                          'w-1.5 h-1.5 rounded-full transition-colors duration-300',
                          job.status === 'running' ? 'bg-blue-500 animate-pulse' : 'bg-zinc-600',
                        )}
                      />
                      {taskLabel}
                    </button>
                    <button
                      type="button"
                      onClick={() => onDismiss(job.id)}
                      className="mr-3 hover:bg-zinc-800 p-0.5 rounded text-zinc-500 hover:text-zinc-300 cursor-pointer"
                      title="Dismiss task"
                      aria-label={`Dismiss ${taskLabel} task`}
                    >
                      <X className="h-2.5 w-2.5" />
                    </button>
                  </div>
                )
              })}
            </div>
          )}
          <div className="flex-1 p-3 font-mono bg-zinc-950 overflow-y-auto flex flex-col justify-between">
            <div className="flex-1 overflow-hidden flex flex-col">
              <div className="text-[10px] text-zinc-500 border-b border-zinc-900 pb-1 mb-2 flex items-center justify-between shrink-0">
                <span>
                  STREAM ID: {activeJob?.id}
                  {activeJob?.started_at && ` • STARTED: ${full(activeJob.started_at)} ${abbr()}`}
                </span>
                {activeJob?.status === 'running' ? (
                  <span className="text-emerald-500 font-bold uppercase animate-pulse">● LIVE STREAMING</span>
                ) : (
                  <span className="text-zinc-500 font-bold uppercase">● COMPLETED</span>
                )}
              </div>
              <div className="flex-1 overflow-y-auto min-h-0 bg-black/30 rounded border border-zinc-900 p-2">
                <CronLiveLog
                  key={activeJob?.id}
                  runId={activeJob?.id}
                  job={activeJob}
                  singleLine={false}
                  startedAt={activeJob?.started_at}
                  onDone={() => {
                    if (activeJob?.id) {
                      setDisplayedJobs((prev) =>
                        prev.map((j) => (j.id === activeJob.id ? { ...j, status: 'completed' } : j)),
                      )
                    }
                  }}
                />
              </div>
            </div>
            <div className="mt-2 pt-2 border-t border-zinc-900 flex items-center justify-between text-[10px] text-zinc-500 shrink-0">
              <span>Task: {activeJob?.task}</span>
              <button
                onClick={() => onDismiss(activeJob?.id)}
                className="text-red-400 hover:text-red-300 hover:underline cursor-pointer"
              >
                Dismiss Active View
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
