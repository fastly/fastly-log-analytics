'use client'

import React, { useState, useRef, useEffect } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { cronCacheBust } from '@/lib/admin-stream-apply'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Loader2 } from 'lucide-react'
import { useSSE, SSELine, SSEStatus } from '@/hooks/useSSE'
import {
  cn,
  panelDialogContent,
  panelDialogFooter,
  panelDialogHeaderSolid,
} from '@/lib/utils'

interface SSEProgressViewProps {
  lines: SSELine[]
  status: SSEStatus
  error: string | null
  description?: string | React.ReactNode
  onStart?: () => void
  renderLine?: (line: SSELine, index: number) => React.ReactNode
  className?: string
  progressLabel?: string
  doneMessage?: string
}

export function SSEProgressView({
  lines,
  status,
  error,
  description,
  onStart,
  renderLine,
  className,
  progressLabel,
  doneMessage = "Process completed successfully."
}: SSEProgressViewProps) {
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [lines, status])

  // Surface an actionable link the backend may attach to a terminal error
  // event (e.g. a manage.fastly.com deep link when a required product isn't
  // enabled). The error event is captured in `lines` even though it's filtered
  // out of the main feed below, so read its `link` here.
  const errorLine = [...lines].reverse().find(l => l.type === 'error')
  const errorLink = typeof errorLine?.link === 'string' ? errorLine.link : null

  const lastProgressLine = [...lines].reverse().find(l => l.type === 'progress')
  const lastStepStatusLine = [...lines].reverse().find(l =>
    l.type === 'status' && typeof l.message === 'string' && l.message.trim() !== ''
  )
  const currentStepMessage = lastStepStatusLine?.message || ""

  let progressCurrent = 0
  let progressTotal = 1
  let progressPercent = 0

  if (lastProgressLine) {
    progressCurrent = typeof lastProgressLine.current === 'number' ? lastProgressLine.current : 0
    progressTotal = typeof lastProgressLine.total === 'number' ? lastProgressLine.total : 1
    progressPercent = Math.min(100, Math.max(0, Math.round((progressCurrent / progressTotal) * 100)))
  }

  return (
    <div className={cn("flex flex-col bg-muted/30 border border-border/50 text-foreground rounded-lg font-mono text-xs relative overflow-hidden shadow-inner", className)}>
      {status === 'idle' && description && (
        <div className="absolute inset-0 bg-background/80 backdrop-blur-[2px] z-10 flex flex-col items-center overflow-y-auto p-8 text-center">
          {/* my-auto centers the block when it fits and collapses to allow
              scrolling when the description is taller than the overlay —
              justify-center alone clips tall content with no way to reach it. */}
          <div className="max-w-md space-y-6 my-auto">
            <div className="text-muted-foreground leading-relaxed font-sans text-sm">{description}</div>
            {onStart && (
              <button
                onClick={onStart}
                className="w-full font-sans font-semibold bg-primary text-primary-foreground h-11 px-8 rounded-md transition-colors hover:bg-primary/90"
              >
                Start Process
              </button>
            )}
          </div>
        </div>
      )}

      <ScrollArea className="flex-1 p-4 h-full">
        <div className="space-y-1.5 pb-4">
          {lines
            .filter(line => line.type !== 'progress' && line.type !== 'error')
            .map((line, i) => {
            let isDoneFile = false;
            if (line.type === 'file_done' || (line.message && line.message.includes('[') && line.message.includes('] Read'))) {
              isDoneFile = true;
            }

            // Section-header convention: a status line whose message starts
            // with "▸" (e.g. the orchestrator's two-phase "▸ Phase 1/2 …"
            // markers) renders as a bold, spaced header so the operator can
            // see at a glance where one phase ends and the next begins. Other
            // SSE producers don't emit "▸"-prefixed lines, so this is a no-op
            // for them.
            const isHeader =
              typeof line.message === 'string' && line.message.trimStart().startsWith('▸')

            return (
              <div
                key={line._id ?? i}
                className={cn(
                  "transition-colors leading-relaxed",
                  isHeader
                    ? "font-sans font-semibold text-foreground mt-3 first:mt-0"
                    : isDoneFile
                      ? "text-muted-foreground"
                      : "text-foreground",
                )}
              >
                {((renderLine ? renderLine(line, i) : null) || (
                  line.message ?? line.summary ?? (
                    line.type === 'file_done' ? `Processed ${line.file_name}` :
                    (line.type === 'done' ? null : JSON.stringify(line))
                  )
                )) as React.ReactNode}
              </div>
            )
          })}
          {status === 'streaming' && (
            <div className="flex items-center gap-2 mt-4 text-muted-foreground font-sans text-sm font-medium">
              <Loader2 className="w-4 h-4 animate-spin" /> Processing...
            </div>
          )}
          {error && (
            <div className="text-destructive mt-4 border-t border-destructive/20 pt-4 font-sans font-medium text-sm">
              Error: {error}
              {errorLink && (
                <>
                  {' '}
                  <a
                    href={errorLink}
                    target="_blank"
                    rel="noreferrer"
                    className="underline underline-offset-2 hover:opacity-80"
                  >
                    Open Fastly to enable it
                  </a>
                </>
              )}
            </div>
          )}
          {status === 'done' && (
            <div className="text-emerald-600 dark:text-emerald-400 mt-4 border-t border-emerald-500/20 pt-4 font-sans font-medium text-sm">{doneMessage}</div>
          )}
          <div ref={bottomRef} />
        </div>
      </ScrollArea>

      {lastProgressLine && (
        <div className="p-4 bg-muted/50 border-t shrink-0">
          <div className="flex justify-between items-end text-xs text-muted-foreground mb-2 font-sans font-medium">
            <div className="flex flex-col gap-1">
              {(currentStepMessage || progressLabel) && (
                <span className="text-foreground font-semibold truncate max-w-[400px]">
                  {currentStepMessage || progressLabel}
                </span>
              )}
            </div>
            <span className="mb-1">{progressPercent}%</span>
          </div>
          <div className="h-2 w-full bg-border/50 rounded-full overflow-hidden">
            <div className="h-full bg-primary transition-all duration-500 ease-out" style={{ width: `${progressPercent}%` }} />
          </div>
        </div>
      )}
    </div>
  )
}

interface SSEExecutionSectionProps {
  title: string
  subtitle: string
  lines: SSELine[]
  status: SSEStatus
  error: string | null
  heightClassName?: string
  progressLabel?: string
  doneMessage?: string
}

export function SSEExecutionSection({
  title,
  subtitle,
  lines,
  status,
  error,
  heightClassName = 'h-[300px]',
  progressLabel = 'Progress',
  doneMessage,
}: SSEExecutionSectionProps) {
  return (
    <div className="p-8 space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
      <div className="text-center space-y-2">
        <h3 className="text-lg font-semibold tracking-tight">{title}</h3>
        <p className="text-sm text-muted-foreground">{subtitle}</p>
      </div>

      <SSEProgressView
        lines={lines}
        status={status}
        error={error}
        className={heightClassName}
        progressLabel={progressLabel}
        doneMessage={doneMessage}
      />
    </div>
  )
}

interface SSEDialogFooterProps {
  status: SSEStatus
  onClose: () => void
  onStop?: () => void
  stopDisabled?: boolean
  doneLabel?: string
  idleOrErrorLabel?: string
}

export function SSEDialogFooter({
  status,
  onClose,
  onStop,
  stopDisabled = false,
  doneLabel = 'Close',
  idleOrErrorLabel = 'Cancel',
}: SSEDialogFooterProps) {
  return (
    <>
      {status !== 'streaming' && (
        <Button variant="outline" onClick={onClose} className="h-10 px-6">
          {status === 'done' ? doneLabel : idleOrErrorLabel}
        </Button>
      )}
      {status === 'streaming' && (
        <Button variant="outline" onClick={onStop} disabled={stopDisabled} className="h-10 px-6">
          Stop
        </Button>
      )}
    </>
  )
}

function makeSSECloseHandler({
  status,
  onOpenChange,
  onComplete,
  reloadOnDone = true,
}: {
  status: SSEStatus
  onOpenChange: (open: boolean) => void
  onComplete?: () => void
  reloadOnDone?: boolean
}) {
  return (isOpen: boolean) => {
    if (status === 'streaming') return
    onOpenChange(isOpen)
    if (!isOpen && status === 'done') {
      if (onComplete) onComplete()
      if (reloadOnDone) window.location.reload()
    }
  }
}

export function SSEActionDialogShell({
  open,
  onOpenChange,
  onComplete,
  reloadOnDone = true,
  status,
  onStop,
  stopDisabled = false,
  isExecuting,
  maxWidthClassName = 'sm:max-w-xl',
  titleClassName = 'text-destructive',
  title,
  headerRight,
  headerExtra,
  executionSection,
  confirmButton,
  doneLabel = 'Close',
  idleOrErrorLabel = 'Cancel',
  children,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  onComplete?: () => void
  reloadOnDone?: boolean
  status: SSEStatus
  onStop?: () => void
  stopDisabled?: boolean
  isExecuting: boolean
  maxWidthClassName?: string
  titleClassName?: string
  title: React.ReactNode
  headerRight?: React.ReactNode
  headerExtra?: React.ReactNode
  executionSection: React.ReactNode
  confirmButton: React.ReactNode
  doneLabel?: string
  idleOrErrorLabel?: string
  children: React.ReactNode
}) {
  const handleClose = makeSSECloseHandler({ status, onOpenChange, onComplete, reloadOnDone })
  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className={cn(maxWidthClassName, panelDialogContent)} showCloseButton={status !== 'streaming'}>
        <DialogHeader className={panelDialogHeaderSolid}>
          {headerRight ? (
            <div className="flex items-center justify-between w-full">
              <DialogTitle className={cn('flex items-center gap-2 text-xl font-bold', titleClassName)}>{title}</DialogTitle>
              {headerRight}
            </div>
          ) : (
            <DialogTitle className={cn('flex items-center gap-2 text-xl font-bold', titleClassName)}>{title}</DialogTitle>
          )}
          {headerExtra}
        </DialogHeader>
        <div className="flex-1 overflow-y-auto min-h-0">{isExecuting ? executionSection : children}</div>
        <DialogFooter className={panelDialogFooter}>
          {!isExecuting ? (
            <>
              <Button variant="ghost" onClick={() => onOpenChange(false)} className="h-10 px-6 text-xs">Cancel</Button>
              {confirmButton}
            </>
          ) : (
            <SSEDialogFooter
              status={status}
              onClose={() => handleClose(false)}
              onStop={onStop}
              stopDisabled={stopDisabled}
              doneLabel={doneLabel}
              idleOrErrorLabel={idleOrErrorLabel}
            />
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

interface ControlledSSEModalProps {
  isSSEModalOpen: boolean
  setIsSSEModalOpen: (open: boolean) => void
  sseStatus: SSEStatus
  sseTitle: string
  sseError: string | null
  sseDescription: string
  lines: SSELine[]
  stop: () => void
}

export function ControlledSSEModal({
  isSSEModalOpen,
  setIsSSEModalOpen,
  sseStatus,
  sseTitle,
  sseError,
  sseDescription,
  lines,
  stop,
}: ControlledSSEModalProps) {
  const queryClient = useQueryClient()

  return (
    <Dialog open={isSSEModalOpen} onOpenChange={(open) => {
      if (sseStatus === 'streaming') return
      setIsSSEModalOpen(open)
      if (!open) {
        stop()
        cronCacheBust(queryClient)
      }
    }}>
      <DialogContent className="sm:max-w-4xl max-h-[85vh] min-h-[50vh] flex flex-col p-0 overflow-hidden" showCloseButton={sseStatus !== 'streaming'}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <DialogTitle>{sseTitle}</DialogTitle>
        </DialogHeader>

        <SSEProgressView
          lines={lines}
          status={sseStatus}
          error={sseError}
          description={sseDescription}
          className="flex-1 mx-6 my-4"
        />

        <DialogFooter className="px-6 py-4 bg-muted/10 border-t shrink-0">
          <SSEDialogFooter
            status={sseStatus}
            onClose={() => {
              setIsSSEModalOpen(false)
              stop()
              cronCacheBust(queryClient)
            }}
            onStop={stop}
          />
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

interface SSEModalProps {
  trigger: React.ReactNode
  title: string
  description?: string | React.ReactNode
  endpoint: string
  body?: Record<string, unknown>
  /** Render a custom line; defaults to line.message or JSON stringify */
  renderLine?: (line: SSELine, index: number) => React.ReactNode
  onClose?: () => void
}

export function SSEModal({ trigger, title, description, endpoint, body, renderLine, onClose }: SSEModalProps) {
  const [open, setOpen] = useState(false)
  const { lines, status, error, start, stop, reset } = useSSE()

  const handleOpenChange = (newOpen: boolean) => {
    if (status === 'streaming') return // Prevent closing while streaming
    setOpen(newOpen)
    if (!newOpen) {
      stop()
      if (onClose) onClose()
    } else {
      // If we are opening a modal that was previously 'done' or 'error',
      // reset the SSE state so we can start fresh!
      if (status !== 'idle') {
        reset()
      }
    }
  }

  const handleStart = () => {
    start(endpoint, body)
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      {/* base-ui uses `render` (not Radix's `asChild`) to wrap a custom
          element with the DialogTrigger's open-toggling behaviour.
          The trigger is typically a <Button>, so this avoids the
          nested-interactive-element a11y violation the old div onClick
          wrapper had. */}
      <DialogTrigger render={trigger as React.ReactElement} />
      <DialogContent className="sm:max-w-4xl max-h-[85vh] min-h-[50vh] flex flex-col p-0 overflow-hidden" showCloseButton={status !== 'streaming'}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <DialogTitle>{title}</DialogTitle>
        </DialogHeader>

        <SSEProgressView
          lines={lines}
          status={status}
          error={error}
          description={description}
          onStart={handleStart}
          renderLine={renderLine}
          className="flex-1 mx-6 my-4"
        />

        <DialogFooter className="px-6 py-4 bg-muted/10 border-t shrink-0">
          {status === 'idle' && !description && (
            <Button onClick={handleStart}>Start</Button>
          )}
          <SSEDialogFooter
            status={status}
            onClose={() => handleOpenChange(false)}
            onStop={stop}
          />
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
