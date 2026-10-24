'use client'

import { useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { cronCacheBust } from '@/lib/cron-cache-bust'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { useSSE, SSELine, SSEStatus } from '@/hooks/useSSE'
import { SSEProgressView } from './SSEProgressView'
import { cn } from '@/lib/utils'
import {
  panelDialogContent,
  panelDialogFooter,
  panelDialogHeaderSolid,
} from '@/lib/panel-dialog'

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

export function makeSSECloseHandler({
  status,
  onOpenChange,
  onComplete,
}: {
  status: SSEStatus
  onOpenChange: (open: boolean) => void
  onComplete?: () => void
}) {
  return (isOpen: boolean) => {
    if (status === 'streaming') return
    onOpenChange(isOpen)
    if (!isOpen && status === 'done') {
      if (onComplete) onComplete()
      window.location.reload()
    }
  }
}

export function SSEActionDialogShell({
  open,
  onOpenChange,
  onComplete,
  status,
  onStop,
  isExecuting,
  maxWidthClassName = 'sm:max-w-xl',
  titleClassName = 'text-destructive',
  title,
  executionSection,
  confirmButton,
  doneLabel = 'Close',
  idleOrErrorLabel = 'Cancel',
  children,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  onComplete?: () => void
  status: SSEStatus
  onStop?: () => void
  isExecuting: boolean
  maxWidthClassName?: string
  titleClassName?: string
  title: React.ReactNode
  executionSection: React.ReactNode
  confirmButton: React.ReactNode
  doneLabel?: string
  idleOrErrorLabel?: string
  children: React.ReactNode
}) {
  const handleClose = makeSSECloseHandler({ status, onOpenChange, onComplete })
  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className={cn(maxWidthClassName, panelDialogContent)} showCloseButton={status !== 'streaming'}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <DialogTitle className={cn('flex items-center gap-2 text-xl font-bold', titleClassName)}>{title}</DialogTitle>
        </DialogHeader>
        <div className="flex-1 overflow-y-auto min-h-0">{isExecuting ? executionSection : children}</div>
        <DialogFooter className={panelDialogFooter}>
          {!isExecuting ? (
            <>
              <Button variant="ghost" onClick={() => onOpenChange(false)} className="h-10 px-6 text-xs">Cancel</Button>
              {confirmButton}
            </>
          ) : (
            <SSEDialogFooter status={status} onClose={() => handleClose(false)} onStop={onStop} doneLabel={doneLabel} idleOrErrorLabel={idleOrErrorLabel} />
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
