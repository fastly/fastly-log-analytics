'use client'

import { useState, useEffect, useRef } from 'react'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { useSSE, SSELine } from '@/hooks/useSSE'
import { SSEProgressView } from './SSEProgressView'
import { panelDialogHeaderSolid } from '@/lib/panel-dialog'

interface SSEModalProps {
  trigger: React.ReactNode
  title: string
  description?: string | React.ReactNode
  endpoint: string
  body?: Record<string, unknown>
  /** Render a custom line; defaults to line.message or JSON stringify */
  renderLine?: (line: SSELine, index: number) => React.ReactNode
  autoStart?: boolean
  onClose?: () => void
}

export function SSEModal({ trigger, title, description, endpoint, body, renderLine, autoStart = false, onClose }: SSEModalProps) {
  const [open, setOpen] = useState(false)
  const { lines, status, isDone, error, start, stop, reset } = useSSE()
  const hasAutoStarted = useRef(false)

  useEffect(() => {
    if (open) {
      if (autoStart && status === 'idle' && !hasAutoStarted.current) {
        hasAutoStarted.current = true
        start(endpoint, body)
      }
    } else {
      hasAutoStarted.current = false
    }
  }, [open, autoStart, status, start, endpoint, body])

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
