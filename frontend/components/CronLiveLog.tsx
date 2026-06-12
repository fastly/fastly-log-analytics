import React, { useEffect, useRef } from 'react'
import { useSSE } from '@/hooks/useSSE'
import { Loader2 } from 'lucide-react'
import { useDateFormat } from '@/hooks/useDateFormat'

export function CronLiveLog({
  runId,
  singleLine = false,
  terminalMode = !singleLine,
  startedAt,
  onDone
}: {
  runId: number | string | undefined,
  singleLine?: boolean,
  terminalMode?: boolean,
  startedAt?: string,
  onDone?: () => void
}) {
  const { lines, status, start, stop } = useSSE()
  const started = useRef(false)
  const doneFired = useRef(false)
  const { full, abbr } = useDateFormat()

  useEffect(() => {
    if (runId && !started.current) {
      started.current = true
      start(`/api/cron-runs/${runId}/stream`)
    }
    return () => {
      if (started.current) {
        stop()
        started.current = false
      }
    }
  }, [runId, start, stop])

  useEffect(() => {
    if ((status === 'done' || status === 'error') && onDone && !doneFired.current) {
      doneFired.current = true
      onDone()
    }
  }, [status, onDone])

  // Under singleLine, only show the last line.
  // Otherwise under terminalMode show all lines. Fallback to last 2 lines.
  const recentLines = singleLine
    ? lines.slice(-1)
    : terminalMode
      ? lines
      : lines.slice(-2)

  if (recentLines.length === 0) {
    if (singleLine) {
      return (
        <div className="flex items-center gap-1.5 text-[9px] font-mono text-muted-foreground truncate h-full w-full">
          {status === 'streaming' ? (
            <><Loader2 className="w-3 h-3 animate-spin shrink-0" /> <span className="truncate">Loading logs...</span></>
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
            <><Loader2 className="w-3.5 h-3.5 animate-spin shrink-0 text-blue-400" /> <span>Streaming console output...</span></>
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
          let text = (line.message as string) || (line.type === 'file_done' ? `Processed ${line.file_name}` : JSON.stringify(line))
          if (text.length > 80) text = text.substring(0, 80) + '...'

          return (
            <div key={i} className="truncate w-full" title={typeof line.message === 'string' ? line.message : text}>
              {line.type === 'error' ? (
                <span className="text-red-500">{text}</span>
              ) : line.type === 'done' ? (
                <span className="text-emerald-500">{text}</span>
              ) : line.type === 'status' ? (
                <span className="text-blue-400">{text}</span>
              ) : line.type === 'file_done' ? (
                <span className="text-muted-foreground">Processed {String(line.file_name)}</span>
              ) : (
                <span>{text}</span>
              )}
            </div>
          )
        })}
      </div>
    )
  }

  return (
    <div className={terminalMode
      ? "flex flex-col gap-1.5 w-full font-mono text-xs leading-relaxed text-zinc-200"
      : "flex flex-col gap-0.5 mt-1.5 bg-background/50 border border-border/50 p-2 rounded text-[10px] font-mono text-muted-foreground w-[280px] max-w-sm overflow-hidden"
    }>
      {terminalMode && startedAt && (
        <div className="text-zinc-500 border-b border-zinc-900 pb-1.5 mb-1.5">
          [SESSION INITIATED AT: {full(startedAt)} {abbr()}]
        </div>
      )}
      {recentLines.map((line, i) => {
        const text = (line.message as string) || (line.type === 'file_done' ? `Processed ${line.file_name}` : JSON.stringify(line))

        return (
          <div
            key={i}
            className={terminalMode
              ? "whitespace-pre-wrap break-all w-full text-zinc-300"
              : "truncate w-full"
            }
            title={typeof line.message === 'string' ? line.message : text}
          >
            {line.type === 'error' ? (
              <span className="text-red-400 font-medium">{text}</span>
            ) : line.type === 'done' ? (
              <span className="text-emerald-400 font-semibold">{text}</span>
            ) : line.type === 'status' ? (
              <span className="text-blue-400">{text}</span>
            ) : line.type === 'file_done' ? (
              <span className="text-zinc-400">Processed {String(line.file_name)}</span>
            ) : (
              <span>{text}</span>
            )}
          </div>
        )
      })}
    </div>
  )
}
