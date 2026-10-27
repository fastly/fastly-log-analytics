'use client'

import React, { useEffect, useRef } from 'react'
import { Database, RefreshCw, Loader2, ChevronDown, X } from 'lucide-react'
import { cn } from '@/lib/utils'
import { useSSE, useDateFormat } from '@/hooks/useSSE'
import type { CronRun } from './useLogsPageState'

export function CronLiveLog({
  runId,
  singleLine = false,
  terminalMode = !singleLine,
  startedAt,
  onDone,
}: {
  runId: number | string | undefined
  singleLine?: boolean
  terminalMode?: boolean
  startedAt?: string
  onDone?: () => void
}) {
  const { lines, status, start, stop } = useSSE()
  const startedForRunId = useRef<number | string | undefined>(undefined)
  const doneFiredForRunId = useRef<number | string | undefined>(undefined)
  const { full, abbr } = useDateFormat()

  useEffect(() => {
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
  }, [runId, start, stop])

  useEffect(() => {
    if ((status === 'done' || status === 'error') && onDone && doneFiredForRunId.current !== runId) {
      doneFiredForRunId.current = runId
      onDone()
    }
  }, [status, onDone, runId])

  const recentLines = singleLine ? lines.slice(-1) : terminalMode ? lines : lines.slice(-2)
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
      node: <span className={cls}>{line.type === 'file_done' ? `Processed ${String(line.file_name)}` : text}</span>,
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

  return (
    <div
      role="log"
      aria-live="polite"
      aria-atomic="false"
      aria-label={terminalMode ? 'Cron job output stream' : 'Recent cron output'}
      className={
        terminalMode
          ? 'flex flex-col gap-1.5 w-full font-mono text-xs leading-relaxed text-zinc-200'
          : 'flex flex-col gap-0.5 mt-1.5 bg-background/50 border border-border/50 p-2 rounded text-[10px] font-mono text-muted-foreground w-[280px] max-w-sm overflow-hidden'
      }
    >
      {terminalMode && startedAt && (
        <div className="text-zinc-500 border-b border-zinc-900 pb-1.5 mb-1.5">
          [SESSION INITIATED AT: {full(startedAt)} {abbr()}]
        </div>
      )}
      {recentLines.map((line, i) => {
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
            'px-4 py-2.5 rounded-full text-xs font-semibold flex items-center gap-2.5 shadow-2xl transition-all hover:scale-105 duration-200 cursor-pointer border',
            runningCount > 0
              ? 'bg-blue-600 hover:bg-blue-700 text-white border-blue-500/20 animate-bounce'
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
