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
import { panelDialogHeaderSolid } from '@/lib/panel-dialog'

export interface SSEExecutionSectionProps {
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

export interface SSEDialogFooterProps {
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

export interface ControlledSSEModalProps {
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
          {sseStatus !== 'streaming' && (
            <Button variant="outline" onClick={() => {
              setIsSSEModalOpen(false)
              stop()
              cronCacheBust(queryClient)
            }}>
              {sseStatus === 'done' ? 'Close' : 'Cancel'}
            </Button>
          )}
          {sseStatus === 'streaming' && (
            <Button variant="outline" onClick={stop}>Stop</Button>
          )}
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
          {status !== 'streaming' && (
            <Button variant="outline" onClick={() => handleOpenChange(false)}>
              {status === 'done' ? 'Close' : 'Cancel'}
            </Button>
          )}
          {status === 'streaming' && (
            <Button variant="outline" onClick={stop}>Stop</Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
