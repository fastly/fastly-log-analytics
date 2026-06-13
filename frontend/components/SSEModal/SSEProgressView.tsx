'use client'

import React, { useRef, useEffect } from 'react'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Loader2 } from 'lucide-react'
import { cn } from '@/lib/utils'
import { SSELine, SSEStatus } from '@/hooks/useSSE'

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
  progressLabel = "Progress",
  doneMessage = "Process completed successfully."
}: SSEProgressViewProps) {
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [lines, status])

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
        <div className="absolute inset-0 bg-background/80 backdrop-blur-[2px] z-10 flex flex-col items-center justify-center p-8 text-center">
          <div className="max-w-md space-y-6">
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

            return (
              <div key={i} className={cn("transition-colors leading-relaxed", isDoneFile ? "text-muted-foreground" : "text-foreground")}>
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
            <div className="text-destructive mt-4 border-t border-destructive/20 pt-4 font-sans font-medium text-sm">Error: {error}</div>
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
              {currentStepMessage && <span className="text-foreground font-semibold truncate max-w-[400px]">{currentStepMessage}</span>}
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
