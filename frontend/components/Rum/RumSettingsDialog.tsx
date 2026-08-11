'use client'

import React, { useState, useEffect, useRef, useMemo } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { formatBytes } from '@/lib/format'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter
} from '@/components/ui/dialog'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { ArrowRight, Settings2, AlertCircle, Sparkles } from 'lucide-react'
import { useSSE, type SSELine } from '@/hooks/useSSE'
import { SSEProgressView } from '@/components/SSEModal'
import { cn } from '@/lib/utils'
import { adminFetch } from '@/lib/api'
import {
  panelDialogContent,
  panelDialogFooter,
  panelDialogHeaderSolid,
} from '@/lib/panel-dialog'

const RUM_FIELDS = [
  { name: 'rum_cid', bytes: 12, categories: ['base'] },
  { name: 'fastly_req_id', bytes: 12, categories: ['base'] },
  { name: 'rum_pathname', bytes: 256, categories: ['base'] },
  { name: 'rum_connection_speed', bytes: 10, categories: ['base'] },
  { name: 'rum_trace_id', bytes: 32, categories: ['base'] },
  { name: 'rum_span_id', bytes: 16, categories: ['base'] },
  { name: 'rum_metric_name', bytes: 12, categories: ['vitals', 'performance', 'events'] },
  { name: 'rum_metric_value', bytes: 8, categories: ['vitals', 'performance', 'events'] },
  { name: 'rum_metric_rating', bytes: 18, categories: ['vitals'] },
  { name: 'rum_dns_ms', bytes: 5, categories: ['performance'] },
  { name: 'rum_tcp_ms', bytes: 5, categories: ['performance'] },
  { name: 'rum_tls_ms', bytes: 5, categories: ['performance'] },
  { name: 'rum_ttfb_ms', bytes: 5, categories: ['performance'] },
  { name: 'rum_error_message', bytes: 200, categories: ['errors'] },
  { name: 'rum_error_stack', bytes: 400, categories: ['errors'] },
  { name: 'rum_raw_query', bytes: 800, categories: ['events'] },
  { name: 'rum_body', bytes: 1024, categories: ['events'] },
]

interface RumSettingsDialogProps {
  serviceId: string | null
  open: boolean
  onOpenChange: (open: boolean) => void
  availableVersions: string[]
  currentVersion: string | null
  latestVersion: string | null
  onComplete?: () => void
}

export function RumSettingsDialog({
  serviceId,
  open,
  onOpenChange,
  availableVersions,
  currentVersion,
  latestVersion,
  onComplete,
}: RumSettingsDialogProps) {
  const queryClient = useQueryClient()
  const [isExecuting, setIsExecuting] = useState(false)
  const [selectedVersion, setSelectedVersion] = useState<string>(
    latestVersion ?? availableVersions[0] ?? ''
  )

  // Local settings toggles
  const [captureVitals, setCaptureVitals] = useState(true)
  const [capturePerformance, setCapturePerformance] = useState(true)
  const [captureErrors, setCaptureErrors] = useState(true)
  const [captureEvents, setCaptureEvents] = useState(true)
  const [customCondition, setCustomCondition] = useState('')

  // Local log lines to show during synchronous settings saving
  const [localLogs, setLocalLogs] = useState<string[]>([])
  const [syncStatus, setSyncStatus] = useState<'idle' | 'streaming' | 'done' | 'error'>('idle')
  const [syncError, setSyncError] = useState<string | null>(null)

  const completedRef = useRef(false)

  // Calculate estimated bytes based on active capture toggles
  const estimatedBytes = useMemo(() => {
    const activeCategories = new Set<string>()
    if (captureVitals) activeCategories.add('vitals')
    if (capturePerformance) activeCategories.add('performance')
    if (captureErrors) activeCategories.add('errors')
    if (captureEvents) activeCategories.add('events')

    if (activeCategories.size > 0) {
      activeCategories.add('base')
    }

    const activeFields = RUM_FIELDS.filter(field =>
      field.categories.some(cat => activeCategories.has(cat))
    )

    if (activeFields.length === 0) return 0

    const fieldBytes = activeFields.reduce((sum, field) => sum + field.bytes, 0)
    const structural = 2 + activeFields.length * 5
    return fieldBytes + structural
  }, [captureVitals, capturePerformance, captureErrors, captureEvents])

  // Fetch full status which includes capture toggles
  const { data: statusData, isLoading: isStatusLoading } = useQuery({
    queryKey: ['rum-status', serviceId],
    queryFn: async () => {
      if (!serviceId) return null
      const res = await adminFetch(`/api/services/${serviceId}/rum/status`)
      return res.ok ? res.json() : null
    },
    enabled: !!serviceId && open,
  })

  // Set initial toggles when loaded
  useEffect(() => {
    if (statusData) {
      setCaptureVitals(statusData.capture_vitals ?? true)
      setCapturePerformance(statusData.capture_performance ?? true)
      setCaptureErrors(statusData.capture_errors ?? true)
      setCaptureEvents(statusData.capture_events ?? true)
      setCustomCondition(statusData.custom_condition ?? '')
    }
  }, [
    statusData?.capture_vitals,
    statusData?.capture_performance,
    statusData?.capture_errors,
    statusData?.capture_events,
    statusData?.custom_condition,
  ])

  const { lines: sseLines, status: sseStatus, error: sseError, start: sseStart, stop: sseStop, reset: sseReset } = useSSE()

  // Reset open props
  const [prevOpen, setPrevOpen] = useState(open)
  if (open !== prevOpen) {
    setPrevOpen(open)
    if (open) {
      setSelectedVersion(latestVersion ?? availableVersions[0] ?? '')
      setLocalLogs([])
      setSyncStatus('idle')
      setSyncError(null)
    }
  }

  // Clean up state on close
  useEffect(() => {
    if (!open) {
      const id = setTimeout(() => {
        setIsExecuting(false)
        sseReset()
      }, 300)
      return () => clearTimeout(id)
    }
  }, [open, sseReset])

  // Invalidate queries when done
  useEffect(() => {
    const isDone = sseStatus === 'done' || syncStatus === 'done'
    if (isDone && !completedRef.current) {
      completedRef.current = true
      onComplete?.()
      queryClient.invalidateQueries({ queryKey: ['rum-status', serviceId] })
      queryClient.invalidateQueries({ queryKey: ['rum-versions', serviceId] })
    }
    if (!isDone) {
      completedRef.current = false
    }
  }, [sseStatus, syncStatus, onComplete, serviceId, queryClient])

  if (!serviceId) return null

  const noVersions = availableVersions.length === 0
  const isNoOp = !selectedVersion

  const handleExecute = async () => {
    if (noVersions || isNoOp || isExecuting) return

    setIsExecuting(true)
    setLocalLogs(['⚙️  Updating client-side RUM settings...'])
    setSyncStatus('streaming')

    try {
      // 1. Save settings toggles synchronously first
      const settingsRes = await adminFetch(`/api/services/${serviceId}/rum/settings`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          capture_vitals: captureVitals,
          capture_performance: capturePerformance,
          capture_errors: captureErrors,
          capture_events: captureEvents,
          custom_condition: customCondition,
        }),
      })

      if (!settingsRes.ok) {
        const errorJson = await settingsRes.json().catch(() => ({}))
        const errorMsg =
          (typeof errorJson.detail === 'object' && errorJson.detail?.error) ||
          (typeof errorJson.detail === 'string' && errorJson.detail) ||
          errorJson.error ||
          'Failed to update RUM settings'
        throw new Error(errorMsg)
      }

      setLocalLogs((prev) => [
        ...prev,
        '✅ RUM capture settings saved successfully!',
        '✅ Client-side rum-tracker.js wrapper compiled and deployed to FOS.',
      ])

      // 2. Check if version needs to be upgraded/changed too
      if (selectedVersion !== currentVersion) {
        setLocalLogs((prev) => [...prev, `⏳ Version change detected (${currentVersion || 'unpinned'} ➔ ${selectedVersion}). Starting streamed bundle upgrade...`])
        // Trigger the SSE streamed version upgrade
        sseStart(`/api/services/${serviceId}/rum/upgrade`, {
          version: selectedVersion,
          activate: true,
        })
      } else {
        // No version change, we are done!
        setSyncStatus('done')
      }
    } catch (err: any) {
      setSyncStatus('error')
      setSyncError(err.message || 'An error occurred while updating settings')
      setLocalLogs((prev) => [...prev, `❌ Error: ${err.message || 'An error occurred'}`])
    }
  }

  const handleClose = (isOpen: boolean) => {
    if (sseStatus === 'streaming' || syncStatus === 'streaming') return
    onOpenChange(isOpen)
  }

  // Combine local sync logs with SSE upgrade log streams
  const displayedLines: SSELine[] = [
    ...localLogs.map((log, index) => ({
      _id: -1000 - index,
      message: log,
      type: log.startsWith('❌') ? 'error' : log.startsWith('⏳') ? 'info' : 'log'
    })),
    ...sseLines
  ]
  const currentStatus = selectedVersion !== currentVersion ? (sseStatus === 'idle' ? syncStatus : sseStatus) : syncStatus
  const currentError = selectedVersion !== currentVersion ? (sseError || syncError) : syncError

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className={cn("sm:max-w-xl", panelDialogContent)} showCloseButton={currentStatus !== 'streaming'}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <div className="flex items-center justify-between w-full">
            <DialogTitle className="flex items-center gap-2 text-primary text-xl font-bold">
              <Settings2 className="h-6 w-6 text-primary" />
              RUM Tracking Settings
            </DialogTitle>
            {!isExecuting && !isStatusLoading && (
              <div className="text-xs font-mono text-muted-foreground bg-muted/50 px-2.5 py-1 rounded-md border shrink-0">
                Est. ~{formatBytes(estimatedBytes)} / line
              </div>
            )}
          </div>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto min-h-0">
          {isExecuting ? (
            <div className="p-8 space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
              <div className="text-center space-y-2">
                <h3 className="text-lg font-semibold tracking-tight">Applying RUM Settings</h3>
                <p className="text-sm text-muted-foreground">
                  Configuring capture options and publishing the revised tracking wrapper to Object Storage...
                </p>
              </div>

              <SSEProgressView
                lines={displayedLines}
                status={currentStatus}
                error={currentError}
                className="h-[300px]"
                progressLabel="Task Log"
                doneMessage={
                  selectedVersion !== currentVersion
                    ? `Faro Web SDK upgraded to v${selectedVersion} and RUM capture settings updated successfully!`
                    : "RUM capture settings and tracker JS wrapper updated successfully!"
                }
              />
            </div>
          ) : (
            <div className="p-8 space-y-6">
              <Alert className="border-blue-300 bg-blue-50/60 dark:bg-blue-950/20 text-blue-900 dark:text-blue-300">
                <Sparkles className="h-4 w-4 text-blue-600 dark:text-blue-400" />
                <AlertTitle className="text-sm font-bold">Custom Client-Side Telemetry</AlertTitle>
                <AlertDescription className="text-[13px] ml-1 font-medium mt-1">
                  Adjust which classes of browser-side events are unrolled and recorded. Deployed updates apply to newly loaded page sessions instantly.
                </AlertDescription>
              </Alert>

              {/* RUM capture toggles section */}
              <div className="space-y-4 rounded-lg border bg-muted/20 p-5">
                <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground">Capture Rules</h4>

                {isStatusLoading ? (
                  <div className="text-xs text-muted-foreground animate-pulse py-4 text-center">Loading current settings...</div>
                ) : (
                  <div className="space-y-4">
                    <div className="flex items-start justify-between gap-4">
                      <div className="space-y-1">
                        <Label htmlFor="toggle-vitals" className="text-sm font-semibold leading-none cursor-pointer">Core Web Vitals</Label>
                        <p className="text-xs text-muted-foreground">Track page paint, shift, and response metrics (LCP, CLS, INP, FCP).</p>
                      </div>
                      <Switch
                        id="toggle-vitals"
                        checked={captureVitals}
                        onCheckedChange={setCaptureVitals}
                        size="default"
                      />
                    </div>

                    <div className="flex items-start justify-between gap-4 border-t pt-4">
                      <div className="space-y-1">
                        <Label htmlFor="toggle-perf" className="text-sm font-semibold leading-none cursor-pointer">Performance Timings</Label>
                        <p className="text-xs text-muted-foreground">Record navigation timelines and page load/rendering budgets.</p>
                      </div>
                      <Switch
                        id="toggle-perf"
                        checked={capturePerformance}
                        onCheckedChange={setCapturePerformance}
                        size="default"
                      />
                    </div>

                    <div className="flex items-start justify-between gap-4 border-t pt-4">
                      <div className="space-y-1">
                        <Label htmlFor="toggle-errors" className="text-sm font-semibold leading-none cursor-pointer">JavaScript Errors</Label>
                        <p className="text-xs text-muted-foreground">Capture uncaught script exceptions and runtime crash details.</p>
                      </div>
                      <Switch
                        id="toggle-errors"
                        checked={captureErrors}
                        onCheckedChange={setCaptureErrors}
                        size="default"
                      />
                    </div>

                    <div className="flex items-start justify-between gap-4 border-t pt-4">
                      <div className="space-y-1">
                        <Label htmlFor="toggle-events" className="text-sm font-semibold leading-none cursor-pointer">Custom Events</Label>
                        <p className="text-xs text-muted-foreground">Record standard button clicks, user sessions, and custom events.</p>
                      </div>
                      <Switch
                        id="toggle-events"
                        checked={captureEvents}
                        onCheckedChange={setCaptureEvents}
                        size="default"
                      />
                    </div>
                  </div>
                )}
              </div>

              {/* Optional RUM VCL Condition */}
              <div className="space-y-4 rounded-lg border bg-muted/20 p-5">
                <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground">RUM Routing Exclusions</h4>
                <div className="space-y-2">
                  <div className="space-y-1">
                    <Label htmlFor="rum-custom-condition" className="text-sm font-semibold leading-none">
                      Optional Log Condition
                    </Label>
                    <p className="text-xs text-muted-foreground leading-normal">
                      An additional VCL condition to filter RUM beacons (e.g. <code>req.url.path !~ "^/api/"</code>).
                    </p>
                  </div>
                  <Input
                    id="rum-custom-condition"
                    placeholder="e.g. client.ip != '192.168.1.1' && req.url.path !~ '^/api/'"
                    value={customCondition}
                    onChange={(e) => setCustomCondition(e.target.value)}
                    className="h-9 font-mono text-xs mt-2"
                  />
                </div>
              </div>

              {/* Version picker section */}
              <div className="space-y-4 border-t pt-5">
                <div className="space-y-1.5">
                  <Label htmlFor="upgrade-faro-version" className="text-xs font-bold uppercase tracking-wider text-muted-foreground block mb-2">
                    Faro Web SDK Version
                  </Label>
                  <Select value={selectedVersion} onValueChange={(v) => setSelectedVersion(v ?? '')}>
                    <SelectTrigger
                      id="upgrade-faro-version"
                      className="h-9 text-sm w-full"
                      aria-label="Target Faro Web SDK version"
                    >
                      <SelectValue placeholder="Select a version" />
                    </SelectTrigger>
                    <SelectContent>
                      {availableVersions.map((v) => {
                        const tags = [v === latestVersion && 'latest', v === currentVersion && 'current'].filter(Boolean)
                        return (
                          <SelectItem key={v} value={v}>
                            {v}
                            {tags.length > 0 ? ` (${tags.join(', ')})` : ''}
                          </SelectItem>
                        )
                      })}
                    </SelectContent>
                  </Select>
                </div>

                {selectedVersion !== currentVersion && (
                  <div className="flex items-center justify-center gap-3 rounded-md bg-muted/40 border p-4 text-sm font-mono animate-in fade-in duration-300">
                    <span className="text-muted-foreground">{currentVersion ?? 'unpinned'}</span>
                    <ArrowRight className="h-4 w-4 text-muted-foreground shrink-0" />
                    <span className="font-semibold text-primary">{selectedVersion || '—'}</span>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>

        <DialogFooter className={panelDialogFooter}>
          {!isExecuting ? (
            <>
              <Button variant="ghost" onClick={() => onOpenChange(false)} className="h-10 px-6 text-xs">Cancel</Button>
              <Button
                variant="default"
                className="h-10 px-8 font-bold bg-primary hover:bg-primary/90"
                onClick={handleExecute}
                disabled={noVersions || isNoOp || isStatusLoading}
              >
                Apply Settings
              </Button>
            </>
          ) : (
            <>
              {currentStatus !== 'streaming' && (
                <Button variant="outline" onClick={() => handleClose(false)} className="h-10 px-6">
                  Close
                </Button>
              )}
              {currentStatus === 'streaming' && (
                <Button variant="outline" onClick={selectedVersion !== currentVersion ? sseStop : undefined} disabled={selectedVersion === currentVersion} className="h-10 px-6">
                  Stop
                </Button>
              )}
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
