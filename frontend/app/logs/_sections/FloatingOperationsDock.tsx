'use client'

import React from 'react'
import {
  Database,
  RefreshCw,
  Check,
  ChevronDown,
  X,
  Terminal,
} from 'lucide-react'
import { Button } from "@/components/ui/button"
import { CronLiveLog } from '@/components/CronLiveLog'
import { useDateFormat } from '@/hooks/useDateFormat'
import { cn } from '@/lib/utils'

export function FloatingOperationsDock({
  displayedJobs,
  setDisplayedJobs,
  isOpen,
  setIsOpen,
  selectedJobId,
  setSelectedJobId,
  onDismiss,
  backgroundCronToast,
  setBackgroundCronToast
}: {
  displayedJobs: any[];
  setDisplayedJobs: React.Dispatch<React.SetStateAction<any[]>>;
  isOpen: boolean;
  setIsOpen: (open: boolean) => void;
  selectedJobId: number | string | null;
  setSelectedJobId: (id: number | string | null) => void;
  onDismiss: (id: number) => void;
  backgroundCronToast: any;
  setBackgroundCronToast: (toast: any) => void;
}) {
  const { full, abbr } = useDateFormat()

  if (displayedJobs.length === 0 && !backgroundCronToast) return null

  const activeJob = displayedJobs.find(j => j.id === selectedJobId) || displayedJobs[0]
  const runningJobs = displayedJobs.filter(j => j.status === 'running')
  const runningCount = runningJobs.length

  return (
    <div className="fixed bottom-6 right-6 z-50 flex flex-col items-end gap-2 pointer-events-auto">
      {/* Integrated cool, premium, bottom-right notification toast stacked above minimized button */}
      {!isOpen && backgroundCronToast && (
        // a11y: status toasts that appear without user action need aria-live.
        // role="alert" + aria-live="assertive" on failure so SRs interrupt;
        // role="status" + polite for normal start/complete events.
        <div
          role={backgroundCronToast.status === 'error' ? 'alert' : 'status'}
          aria-live={backgroundCronToast.status === 'error' ? 'assertive' : 'polite'}
          aria-atomic="true"
          className="w-80 sm:w-96 bg-zinc-950/90 backdrop-blur-md text-zinc-100 border border-zinc-800 rounded-lg shadow-2xl overflow-hidden animate-in fade-in slide-in-from-bottom-2 duration-300 pointer-events-auto"
        >
          <div className="p-3.5 flex gap-3">
            {/* Live Indicator or Check/Error Icon */}
            <div className="shrink-0 pt-0.5">
              {backgroundCronToast.status === 'running' ? (
                <div className="relative flex h-3 w-3 mt-0.5">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75"></span>
                  <span className="relative inline-flex rounded-full h-3 w-3 bg-blue-500"></span>
                </div>
              ) : backgroundCronToast.status === 'error' ? (
                <div className="h-3.5 w-3.5 rounded-full bg-red-950/40 border border-red-500/30 flex items-center justify-center text-red-500">
                  <X className="h-2 w-2" />
                </div>
              ) : (
                <div className="h-3.5 w-3.5 rounded-full bg-emerald-900/40 border border-emerald-500/30 flex items-center justify-center text-emerald-400">
                  <Check className="h-2 w-2" />
                </div>
              )}
            </div>

            {/* Content Details */}
            <div className="flex-1 min-w-0">
              <div className="flex items-center justify-between gap-2">
                <p className="text-xs font-semibold text-zinc-200">
                  {backgroundCronToast.status === 'running' ? 'Background Sync Started' :
                   backgroundCronToast.status === 'error' ? 'Background Sync Failed' : 'Background Sync Completed'}
                </p>
                <button
                  onClick={() => setBackgroundCronToast(null)}
                  className="text-zinc-500 hover:text-zinc-300 p-0.5 hover:bg-zinc-900 rounded transition-all cursor-pointer"
                  title="Close notification"
                >
                  <X className="h-3 w-3" />
                </button>
              </div>
              <p className="text-[10px] text-zinc-400 mt-1 font-mono uppercase tracking-wider">
                Task: {backgroundCronToast.task === 'metadata_sync' ? 'sync' : backgroundCronToast.task}
              </p>

              {/* Optional completed job statistics */}
              {backgroundCronToast.status !== 'running' && (
                <div className="mt-2 pt-2 border-t border-zinc-900 flex flex-wrap gap-x-4 gap-y-1 text-[10px] text-zinc-500 font-mono">
                  {backgroundCronToast.rows_ingested !== undefined && (
                    <span>Ingested: <strong className="text-zinc-300">{backgroundCronToast.rows_ingested.toLocaleString()} rows</strong></span>
                  )}
                  {backgroundCronToast.duration_s !== undefined && (
                    <span>Duration: <strong className="text-zinc-300">{backgroundCronToast.duration_s.toFixed(1)}s</strong></span>
                  )}
                </div>
              )}

              {/* Action Trigger Button */}
              <div className="mt-2.5 flex justify-end">
                <Button
                  size="sm"
                  variant="secondary"
                  className="h-6.5 text-[9px] font-medium bg-zinc-900 hover:bg-zinc-850 text-zinc-300 border border-zinc-800 cursor-pointer px-2"
                  onClick={() => {
                    setSelectedJobId(backgroundCronToast.id)
                    setIsOpen(true)
                    setBackgroundCronToast(null)
                  }}
                >
                  <Terminal className="h-2.5 w-2.5 mr-1" /> View Console Logs
                </Button>
              </div>
            </div>
          </div>
        </div>
      )}

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
            "px-4 py-2.5 rounded-full text-xs font-semibold flex items-center gap-2.5 shadow-2xl transition-all hover:scale-105 duration-200 cursor-pointer border",
            runningCount > 0
              ? "bg-blue-600 hover:bg-blue-700 text-white border-blue-500/20 animate-bounce"
              : "bg-zinc-850 hover:bg-zinc-800 text-zinc-300 border-zinc-700/50"
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
        <div id="floating-ops-panel" className="bg-zinc-950 text-zinc-100 border border-zinc-800 rounded-lg shadow-2xl w-[440px] sm:w-[500px] h-[380px] flex flex-col overflow-hidden animate-in slide-in-from-bottom-5 duration-300">
          {/* Header */}
          <div className="flex items-center justify-between px-3 py-2 bg-zinc-900 border-b border-zinc-800 shrink-0">
            <div className="flex items-center gap-2 text-xs font-semibold text-zinc-300">
              <Database className="h-3.5 w-3.5 text-blue-500" />
              <span>Console Log Terminal</span>
            </div>
            <div className="flex items-center gap-1.5">
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
          </div>

          {/* Tab Bar for jobs */}
          {displayedJobs.length > 1 && (
            <div className="flex border-b border-zinc-800 bg-zinc-900/50 overflow-x-auto scrollbar-none shrink-0 px-2 pt-1 gap-1">
              {displayedJobs.map((job) => {
                const isActive = job.id === selectedJobId
                return (
                  <button
                    key={job.id}
                    onClick={() => setSelectedJobId(job.id)}
                    className={cn(
                      "px-3 py-1.5 rounded-t text-[10px] font-mono uppercase tracking-wider flex items-center gap-1.5 cursor-pointer border-t border-x transition-all shrink-0",
                      isActive
                        ? "bg-zinc-950 text-blue-400 border-zinc-800 border-b-zinc-950 font-bold"
                        : "bg-transparent text-zinc-400 border-transparent hover:text-zinc-200 hover:bg-zinc-800/30"
                    )}
                  >
                    <span className={cn(
                      "w-1.5 h-1.5 rounded-full transition-colors duration-300",
                      job.status === 'running'
                        ? "bg-blue-500 animate-pulse"
                        : "bg-zinc-600"
                    )} />
                    {job.task === 'metadata_sync' ? 'sync' : job.task}
                    <span
                      onClick={(e) => {
                        e.stopPropagation()
                        onDismiss(job.id)
                      }}
                      className="ml-1 hover:bg-zinc-800 p-0.5 rounded text-zinc-500 hover:text-zinc-300"
                      title="Dismiss task"
                    >
                      <X className="h-2.5 w-2.5" />
                    </span>
                  </button>
                )
              })}
            </div>
          )}

          {/* Terminal Body */}
          <div className="flex-1 p-3 font-mono bg-zinc-950 overflow-y-auto flex flex-col justify-between">
            <div className="flex-1 overflow-hidden flex flex-col">
              <div className="text-[10px] text-zinc-500 border-b border-zinc-900 pb-1 mb-2 flex items-center justify-between shrink-0">
                <span>STREAM ID: {activeJob?.id}{activeJob?.started_at && ` • STARTED: ${full(activeJob.started_at)} ${abbr()}`}</span>
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
                      setDisplayedJobs(prev => prev.map(j => j.id === activeJob.id ? { ...j, status: 'completed' } : j))
                    }
                  }}
                />
              </div>
            </div>

            {/* Terminal Footer Actions */}
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
