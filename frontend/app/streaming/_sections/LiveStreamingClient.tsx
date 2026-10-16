'use client'

import React, { useState, useEffect, useMemo, useRef, useCallback } from 'react'
import {
  Radio,
  Play,
  Pause,
  Search,
  X,
  Copy,
  Check,
  Table as TableIcon,
  FileCode,
  RotateCcw,
  ArrowDown,
  Loader2,
  RefreshCw,
  Terminal,
} from 'lucide-react'
import { ReportLayout } from '@/components/ReportLayout'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { ScrollArea } from '@/components/ui/scroll-area'
import { useEventStream, type EventStreamRequest } from '@/hooks/useEventStream'
import { useAdminTokenStore } from '@/stores/adminTokenStore'
import { getApiBase } from '@/lib/api'
import { statusBadgeClass } from '@/lib/status-style'
import { cn } from '@/lib/utils'

export interface StreamLogEvent {
  event_id: string
  timestamp: string
  client_ip: string
  method: string
  url: string
  status: number
  pop: string
  time_elapsed: number
  attributes: Record<string, unknown>
}

const MAX_BUFFER_LINES = 1000

function getMethodBadgeVariant(method: string | undefined): string {
  switch ((method || '').toUpperCase()) {
    case 'GET':
      return 'bg-blue-500/15 text-blue-400 border-blue-500/25'
    case 'POST':
      return 'bg-emerald-500/15 text-emerald-400 border-emerald-500/25'
    case 'PUT':
    case 'PATCH':
      return 'bg-amber-500/15 text-amber-400 border-amber-500/25'
    case 'DELETE':
      return 'bg-rose-500/15 text-rose-400 border-rose-500/25'
    case 'HEAD':
    case 'OPTIONS':
      return 'bg-purple-500/15 text-purple-400 border-purple-500/25'
    default:
      return 'bg-zinc-500/15 text-zinc-400 border-zinc-500/25'
  }
}

function formatTimestamp(ts: string): string {
  if (!ts) return '—'
  try {
    const d = new Date(ts)
    if (isNaN(d.getTime())) return ts
    return d.toISOString().substring(11, 23) // HH:mm:ss.sss
  } catch {
    return ts
  }
}

function formatElapsed(elapsed: number): string {
  if (!elapsed || isNaN(elapsed)) return '0.0ms'
  if (elapsed < 1) {
    return `${(elapsed * 1000).toFixed(1)}ms`
  }
  return `${elapsed.toFixed(3)}s`
}

export function LiveStreamingClient() {
  return (
    <ReportLayout
      title="Live Log Streaming"
      description="Real-time CDN edge log tail via Server-Sent Events — interactive pause/resume, regex search, and deep JSON inspection."
      icon={Radio}
    >
      {({ activeServiceId }) => (
        <LiveStreamingBody activeServiceId={activeServiceId} />
      )}
    </ReportLayout>
  )
}

interface LiveStreamingBodyProps {
  activeServiceId: string | null
}

function LiveStreamingBody({ activeServiceId }: LiveStreamingBodyProps) {
  const [logs, setLogs] = useState<StreamLogEvent[]>([])
  const [isPaused, setIsPaused] = useState(false)
  const [pausedBuffer, setPausedBuffer] = useState<StreamLogEvent[]>([])
  const [searchFilter, setSearchFilter] = useState('')
  const [statusFilter, setStatusFilter] = useState<'all' | '2xx' | '3xx' | '4xx' | '5xx'>('all')
  const [autoScroll, setAutoScroll] = useState(true)
  const [selectedLog, setSelectedLog] = useState<StreamLogEvent | null>(null)
  const [detailFilter, setDetailFilter] = useState('')
  const [copied, setCopied] = useState(false)

  const isPausedRef = useRef(isPaused)
  useEffect(() => {
    isPausedRef.current = isPaused
  }, [isPaused])

  const terminalScrollRef = useRef<HTMLDivElement>(null)

  // Construct SSE request
  const streamRequest = useMemo<EventStreamRequest | null>(() => {
    if (!activeServiceId) return null
    const url = `${getApiBase()}/api/stream/${encodeURIComponent(activeServiceId)}`
    const adminToken = useAdminTokenStore.getState().token
    const headers: Record<string, string> = {
      Accept: 'text/event-stream',
      'Cache-Control': 'no-cache',
    }
    if (adminToken) {
      headers['X-Admin-Token'] = adminToken
    }
    return { url, method: 'GET', headers }
  }, [activeServiceId])

  // Process incoming SSE log events
  const handleEvent = useCallback((raw: string) => {
    try {
      const event = JSON.parse(raw) as StreamLogEvent
      if (!event || !event.event_id) return

      if (isPausedRef.current) {
        setPausedBuffer((prev) => [...prev.slice(-(MAX_BUFFER_LINES - 1)), event])
      } else {
        setLogs((prev) => {
          const next = [...prev, event]
          return next.length > MAX_BUFFER_LINES ? next.slice(next.length - MAX_BUFFER_LINES) : next
        })
      }
    } catch {
      // Discard unparseable frames
    }
  }, [])

  const { state: connectionState } = useEventStream(
    Boolean(activeServiceId),
    streamRequest,
    handleEvent,
  )

  // Flush paused buffer when resumed
  const togglePause = useCallback(() => {
    setIsPaused((prev) => {
      const next = !prev
      if (!next && pausedBuffer.length > 0) {
        setLogs((cur) => {
          const combined = [...cur, ...pausedBuffer]
          return combined.length > MAX_BUFFER_LINES ? combined.slice(combined.length - MAX_BUFFER_LINES) : combined
        })
        setPausedBuffer([])
      }
      return next
    })
  }, [pausedBuffer])

  // Spacebar shortcut for pause/resume
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.code === 'Space') {
        const target = e.target as HTMLElement | null
        const tagName = target?.tagName?.toLowerCase()
        if (
          tagName === 'input' ||
          tagName === 'textarea' ||
          target?.isContentEditable ||
          target?.closest('[role="dialog"]')
        ) {
          return
        }
        e.preventDefault()
        togglePause()
      }
    }
    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [togglePause])

  // Auto-scroll when new logs arrive and autoScroll is active
  useEffect(() => {
    if (autoScroll && !isPaused && terminalScrollRef.current) {
      terminalScrollRef.current.scrollTop = terminalScrollRef.current.scrollHeight
    }
  }, [logs, autoScroll, isPaused])

  // Clear all buffered logs
  const handleClearLogs = useCallback(() => {
    setLogs([])
    setPausedBuffer([])
  }, [])

  // Filter logs by search query and status code
  const filteredLogs = useMemo(() => {
    return logs.filter((log) => {
      // Status code check
      if (statusFilter !== 'all') {
        const status = log.status
        if (statusFilter === '2xx' && (status < 200 || status >= 300)) return false
        if (statusFilter === '3xx' && (status < 300 || status >= 400)) return false
        if (statusFilter === '4xx' && (status < 400 || status >= 500)) return false
        if (statusFilter === '5xx' && status < 500) return false
      }

      // Search filter check
      if (!searchFilter.trim()) return true
      const term = searchFilter.trim()

      try {
        const regex = new RegExp(term, 'i')
        if (
          regex.test(log.url) ||
          regex.test(log.method) ||
          regex.test(String(log.status)) ||
          regex.test(log.client_ip) ||
          regex.test(log.pop)
        ) {
          return true
        }
        // Match in stringified attributes
        return regex.test(JSON.stringify(log.attributes))
      } catch {
        const lowerTerm = term.toLowerCase()
        return (
          log.url.toLowerCase().includes(lowerTerm) ||
          log.method.toLowerCase().includes(lowerTerm) ||
          String(log.status).includes(lowerTerm) ||
          log.client_ip.toLowerCase().includes(lowerTerm) ||
          log.pop.toLowerCase().includes(lowerTerm) ||
          JSON.stringify(log.attributes).toLowerCase().includes(lowerTerm)
        )
      }
    })
  }, [logs, searchFilter, statusFilter])

  // Filtered keys for the inspector modal
  const filteredDetailKeys = useMemo(() => {
    if (!selectedLog) return []
    const allAttributes = {
      event_id: selectedLog.event_id,
      timestamp: selectedLog.timestamp,
      method: selectedLog.method,
      url: selectedLog.url,
      status: selectedLog.status,
      pop: selectedLog.pop,
      client_ip: selectedLog.client_ip,
      time_elapsed: selectedLog.time_elapsed,
      ...selectedLog.attributes,
    }
    const keys = Object.keys(allAttributes).sort()
    if (!detailFilter.trim()) return keys
    const term = detailFilter.toLowerCase()
    return keys.filter(
      (k) =>
        k.toLowerCase().includes(term) ||
        String((allAttributes as Record<string, unknown>)[k] ?? '')
          .toLowerCase()
          .includes(term),
    )
  }, [selectedLog, detailFilter])

  const handleCopyJson = () => {
    if (!selectedLog) return
    const payload = {
      event_id: selectedLog.event_id,
      timestamp: selectedLog.timestamp,
      method: selectedLog.method,
      url: selectedLog.url,
      status: selectedLog.status,
      pop: selectedLog.pop,
      client_ip: selectedLog.client_ip,
      time_elapsed: selectedLog.time_elapsed,
      attributes: selectedLog.attributes,
    }
    navigator.clipboard.writeText(JSON.stringify(payload, null, 2))
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <div data-testid="live-streaming-explorer" className="space-y-4">
      {/* Stream Controls Toolbar */}
      <div
        data-testid="stream-controls-toolbar"
        className="flex flex-wrap items-center justify-between gap-3 p-3 rounded-lg border bg-card text-card-foreground shadow-sm [contain-intrinsic-size:52px]"
      >
        <div className="flex flex-wrap items-center gap-2">
          {/* Pause / Resume Button */}
          <Button
            data-testid="stream-pause-toggle"
            variant={isPaused ? 'default' : 'outline'}
            size="sm"
            onClick={togglePause}
            className="h-8 gap-1.5 text-xs font-medium"
          >
            {isPaused ? (
              <>
                <Play className="h-3.5 w-3.5 fill-current text-emerald-500" />
                <span>Resume (Space)</span>
              </>
            ) : (
              <>
                <Pause className="h-3.5 w-3.5 text-amber-500" />
                <span>Pause (Space)</span>
              </>
            )}
          </Button>

          {/* Connection Status Badge */}
          <div data-testid="stream-connection-status" className="flex items-center px-2 py-1 rounded bg-muted/60 text-xs font-mono">
            {connectionState === 'connecting' && (
              <span className="flex items-center gap-1.5 text-amber-500">
                <Loader2 className="h-3.5 w-3.5 animate-spin" /> Connecting...
              </span>
            )}
            {connectionState === 'reconnecting' && (
              <span className="flex items-center gap-1.5 text-yellow-500">
                <RefreshCw className="h-3.5 w-3.5 animate-spin" /> Reconnecting...
              </span>
            )}
            {connectionState === 'open' && !isPaused && (
              <span className="flex items-center gap-1.5 text-emerald-500">
                <span className="h-2 w-2 rounded-full bg-emerald-500 animate-pulse" /> Live
              </span>
            )}
            {connectionState === 'open' && isPaused && (
              <span className="flex items-center gap-1.5 text-amber-400">
                <span className="h-2 w-2 rounded-full bg-amber-400" /> Paused{' '}
                {pausedBuffer.length > 0 && `(+${pausedBuffer.length})`}
              </span>
            )}
            {connectionState === 'idle' && (
              <span className="flex items-center gap-1.5 text-muted-foreground">
                <span className="h-2 w-2 rounded-full bg-zinc-400" /> Idle
              </span>
            )}
          </div>

          {/* Status Filter Badges */}
          <div className="flex items-center rounded-md border p-0.5 bg-muted/30">
            {(['all', '2xx', '3xx', '4xx', '5xx'] as const).map((st) => (
              <button
                key={st}
                onClick={() => setStatusFilter(st)}
                className={cn(
                  'px-2 py-0.5 text-xs font-mono rounded transition-colors',
                  statusFilter === st
                    ? 'bg-background text-foreground shadow-xs font-bold'
                    : 'text-muted-foreground hover:text-foreground',
                )}
              >
                {st.toUpperCase()}
              </button>
            ))}
          </div>
        </div>

        {/* Right side controls: Search, Auto-scroll, Clear */}
        <div className="flex flex-wrap items-center gap-2">
          {/* Live Search Input */}
          <div className="relative">
            <Search className="absolute left-2.5 top-2 h-3.5 w-3.5 text-muted-foreground pointer-events-none" />
            <Input
              data-testid="stream-search-input"
              type="text"
              placeholder="Filter logs (regex or string)..."
              value={searchFilter}
              onChange={(e) => setSearchFilter(e.target.value)}
              className="h-8 w-56 pl-8 pr-7 text-xs font-mono"
            />
            {searchFilter && (
              <button
                onClick={() => setSearchFilter('')}
                className="absolute right-2 top-2 text-muted-foreground hover:text-foreground"
              >
                <X className="h-3.5 w-3.5" />
              </button>
            )}
          </div>

          {/* Auto-scroll toggle */}
          <Button
            data-testid="stream-autoscroll-toggle"
            variant={autoScroll ? 'secondary' : 'ghost'}
            size="sm"
            onClick={() => setAutoScroll(!autoScroll)}
            className="h-8 text-xs gap-1"
            title={autoScroll ? 'Auto-scroll enabled' : 'Auto-scroll disabled'}
          >
            <ArrowDown className={cn('h-3.5 w-3.5', autoScroll ? 'text-primary' : 'text-muted-foreground')} />
            <span className="hidden sm:inline">Scroll</span>
          </Button>

          {/* Clear logs */}
          <Button
            data-testid="stream-clear-logs"
            variant="ghost"
            size="sm"
            onClick={handleClearLogs}
            className="h-8 text-xs gap-1 text-muted-foreground hover:text-foreground"
          >
            <RotateCcw className="h-3.5 w-3.5" />
            <span className="hidden sm:inline">Clear</span>
          </Button>

          {/* Total log counter badge */}
          <Badge variant="outline" className="h-8 px-2 font-mono text-xs">
            {filteredLogs.length}
            {logs.length !== filteredLogs.length && ` / ${logs.length}`}
            <span className="text-muted-foreground ml-1">/ 1k</span>
          </Badge>
        </div>
      </div>

      {/* Terminal Stream Viewport with Zero CLS layout reservation */}
      <div
        data-testid="stream-terminal-container"
        className="relative flex flex-col h-[650px] w-full rounded-lg border bg-zinc-950 text-zinc-100 shadow-sm overflow-hidden [contain-intrinsic-size:650px] [content-visibility:auto]"
      >
        {/* Terminal Header Bar */}
        <div className="flex items-center justify-between px-3 py-2 border-b border-zinc-800 bg-zinc-900/80 text-[11px] font-mono text-zinc-400 select-none shrink-0">
          <div className="flex items-center gap-2">
            <Terminal className="h-3.5 w-3.5 text-emerald-400" />
            <span className="text-zinc-200 font-semibold">fastly-log-stream</span>
            <span className="text-zinc-600">|</span>
            <span>service: {activeServiceId || '—'}</span>
          </div>
          <div className="text-zinc-500 text-[10px]">Click any row for JSON inspect drawer</div>
        </div>

        {/* Column Titles Bar */}
        <div className="grid grid-cols-12 gap-2 px-3 py-1.5 border-b border-zinc-800/80 bg-zinc-900/40 text-[11px] font-mono text-zinc-400 select-none shrink-0">
          <div className="col-span-2">TIMESTAMP</div>
          <div className="col-span-1">STATUS</div>
          <div className="col-span-1">METHOD</div>
          <div className="col-span-1">POP</div>
          <div className="col-span-1 text-right">ELAPSED</div>
          <div className="col-span-2">CLIENT IP</div>
          <div className="col-span-4">URL PATH</div>
        </div>

        {/* Terminal Rows / Content Area */}
        <div
          ref={terminalScrollRef}
          data-testid="stream-terminal-scroll"
          className="flex-1 overflow-y-auto overflow-x-hidden p-2 space-y-0.5 font-mono text-xs"
        >
          {/* Skeleton state if connecting and zero logs */}
          {logs.length === 0 && connectionState === 'connecting' && (
            <div data-testid="stream-connecting-skeleton" className="space-y-2 p-4">
              <div className="flex items-center gap-2 text-amber-400 text-xs">
                <Loader2 className="h-4 w-4 animate-spin" />
                <span>Connecting to live Fastly log stream...</span>
              </div>
              <div className="text-zinc-500 text-xs">
                Listening for incoming edge requests for {activeServiceId}...
              </div>
              <div className="space-y-1.5 pt-2 opacity-50">
                {[1, 2, 3, 4, 5, 6].map((i) => (
                  <div key={i} className="h-5 bg-zinc-800/50 rounded animate-pulse w-full" />
                ))}
              </div>
            </div>
          )}

          {/* Empty state if connected but no logs received yet */}
          {logs.length === 0 && connectionState === 'open' && (
            <div data-testid="stream-waiting-state" className="flex flex-col items-center justify-center h-full text-center p-6 text-zinc-500">
              <Radio className="h-8 w-8 text-emerald-500/60 animate-pulse mb-2" />
              <p className="text-zinc-300 font-medium">Waiting for edge log events...</p>
              <p className="text-xs text-zinc-500 mt-1">
                Connected to service {activeServiceId}. Requests will appear here in real time as they hit the edge.
              </p>
            </div>
          )}

          {/* Rendered Log Lines */}
          {filteredLogs.map((log) => (
            <div
              key={log.event_id}
              data-testid="stream-log-row"
              onClick={() => setSelectedLog(log)}
              className="grid grid-cols-12 gap-2 px-2 py-1 rounded cursor-pointer hover:bg-zinc-800/70 border border-transparent hover:border-zinc-700/50 transition-colors items-center text-[12px]"
            >
              {/* Timestamp */}
              <div className="col-span-2 text-zinc-400 truncate">
                {formatTimestamp(log.timestamp)}
              </div>

              {/* Status */}
              <div className="col-span-1">
                <span
                  className={cn(
                    'px-1.5 py-0.5 rounded text-[10px] font-bold border inline-block text-center',
                    statusBadgeClass(log.status),
                  )}
                >
                  {log.status}
                </span>
              </div>

              {/* Method */}
              <div className="col-span-1">
                <span
                  className={cn(
                    'px-1.5 py-0.5 rounded text-[10px] font-bold border inline-block',
                    getMethodBadgeVariant(log.method),
                  )}
                >
                  {log.method}
                </span>
              </div>

              {/* POP */}
              <div className="col-span-1 text-zinc-300 font-semibold truncate">
                {log.pop || '—'}
              </div>

              {/* Elapsed */}
              <div className="col-span-1 text-right text-zinc-400">
                {formatElapsed(log.time_elapsed)}
              </div>

              {/* Client IP */}
              <div className="col-span-2 text-zinc-400 truncate">
                {log.client_ip || '—'}
              </div>

              {/* URL */}
              <div className="col-span-4 text-zinc-200 truncate font-normal" title={log.url}>
                {log.url}
              </div>
            </div>
          ))}

          {/* Empty search results state */}
          {logs.length > 0 && filteredLogs.length === 0 && (
            <div className="flex flex-col items-center justify-center p-8 text-zinc-500">
              <Search className="h-6 w-6 mb-2 opacity-50" />
              <p>No log events match filter &ldquo;{searchFilter}&rdquo;</p>
              <Button
                variant="link"
                size="sm"
                onClick={() => {
                  setSearchFilter('')
                  setStatusFilter('all')
                }}
                className="text-xs text-primary mt-1"
              >
                Reset filters
              </Button>
            </div>
          )}
        </div>
      </div>

      {/* Row Inspection Detail Modal / Drawer */}
      <Dialog open={!!selectedLog} onOpenChange={(open) => !open && setSelectedLog(null)}>
        <DialogContent
          data-testid="stream-row-inspector-modal"
          className="max-w-3xl max-h-[85vh] flex flex-col p-6 overflow-hidden"
        >
          {selectedLog && (
            <>
              <DialogHeader className="border-b pb-4">
                <div className="flex items-start justify-between gap-4">
                  <div className="space-y-1">
                    <DialogTitle className="text-base font-semibold flex items-center gap-2">
                      <span
                        className={cn(
                          'px-2 py-0.5 rounded text-xs font-mono font-bold border',
                          getMethodBadgeVariant(selectedLog.method),
                        )}
                      >
                        {selectedLog.method}
                      </span>
                      <span
                        className={cn(
                          'px-2 py-0.5 rounded text-xs font-semibold border',
                          statusBadgeClass(selectedLog.status),
                        )}
                      >
                        {selectedLog.status}
                      </span>
                      <span className="font-mono text-xs text-muted-foreground truncate max-w-sm">
                        {selectedLog.url}
                      </span>
                    </DialogTitle>
                    <DialogDescription className="text-xs text-muted-foreground font-mono">
                      Timestamp: {selectedLog.timestamp} | POP: {selectedLog.pop || '—'} | Latency:{' '}
                      {formatElapsed(selectedLog.time_elapsed)} | IP: {selectedLog.client_ip || '—'}
                    </DialogDescription>
                  </div>

                  <Button
                    data-testid="stream-copy-json"
                    variant="outline"
                    size="sm"
                    onClick={handleCopyJson}
                    className="h-8 text-xs gap-1.5 shrink-0"
                  >
                    {copied ? <Check className="h-3.5 w-3.5 text-emerald-500" /> : <Copy className="h-3.5 w-3.5" />}
                    <span>{copied ? 'Copied' : 'Copy JSON'}</span>
                  </Button>
                </div>
              </DialogHeader>

              <Tabs defaultValue="formatted" className="flex-1 flex flex-col min-h-0 pt-2">
                <div className="flex items-center justify-between pb-2 border-b">
                  <TabsList className="h-8">
                    <TabsTrigger value="formatted" className="text-xs gap-1.5">
                      <TableIcon className="h-3.5 w-3.5" /> Formatted
                    </TabsTrigger>
                    <TabsTrigger value="raw" className="text-xs gap-1.5">
                      <FileCode className="h-3.5 w-3.5" /> Raw JSON
                    </TabsTrigger>
                  </TabsList>

                  <Input
                    type="text"
                    placeholder="Filter attributes..."
                    value={detailFilter}
                    onChange={(e) => setDetailFilter(e.target.value)}
                    className="h-7 w-48 text-xs"
                  />
                </div>

                <TabsContent value="formatted" className="flex-1 min-h-0 pt-2">
                  <ScrollArea className="h-[52vh] pr-4">
                    <table className="w-full text-xs font-mono border-collapse">
                      <thead>
                        <tr className="border-b text-muted-foreground">
                          <th className="py-2 text-left font-semibold w-1/3">Attribute</th>
                          <th className="py-2 text-left font-semibold w-2/3">Value</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-border/40">
                        {filteredDetailKeys.map((key) => {
                          const allAttrs = {
                            event_id: selectedLog.event_id,
                            timestamp: selectedLog.timestamp,
                            method: selectedLog.method,
                            url: selectedLog.url,
                            status: selectedLog.status,
                            pop: selectedLog.pop,
                            client_ip: selectedLog.client_ip,
                            time_elapsed: selectedLog.time_elapsed,
                            ...selectedLog.attributes,
                          }
                          const val = (allAttrs as Record<string, unknown>)[key]
                          const displayVal =
                            val === null || val === undefined
                              ? 'null'
                              : typeof val === 'object'
                              ? JSON.stringify(val)
                              : String(val)

                          return (
                            <tr key={key} className="hover:bg-muted/30">
                              <td className="py-1.5 font-semibold text-primary/80 align-top select-all">
                                {key}
                              </td>
                              <td className="py-1.5 text-foreground/90 break-all select-all">
                                {displayVal}
                              </td>
                            </tr>
                          )
                        })}
                      </tbody>
                    </table>
                  </ScrollArea>
                </TabsContent>

                <TabsContent value="raw" className="flex-1 min-h-0 pt-2">
                  <ScrollArea className="h-[52vh]">
                    <pre className="p-4 rounded-lg bg-muted/40 text-xs font-mono text-foreground/90 overflow-x-auto select-all">
                      {JSON.stringify(
                        {
                          event_id: selectedLog.event_id,
                          timestamp: selectedLog.timestamp,
                          method: selectedLog.method,
                          url: selectedLog.url,
                          status: selectedLog.status,
                          pop: selectedLog.pop,
                          client_ip: selectedLog.client_ip,
                          time_elapsed: selectedLog.time_elapsed,
                          attributes: selectedLog.attributes,
                        },
                        null,
                        2,
                      )}
                    </pre>
                  </ScrollArea>
                </TabsContent>
              </Tabs>
            </>
          )}
        </DialogContent>
      </Dialog>
    </div>
  )
}
export default LiveStreamingClient
