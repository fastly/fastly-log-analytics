'use client'

import React, { useState, useEffect, useRef, useMemo } from 'react'
import { useQueryClient } from '@tanstack/react-query'
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
import { ArrowRight, Settings2, Sparkles } from 'lucide-react'
import { RumVersionPicker } from '@/components/ProvisionWizard/RumVersionPicker'
import { useSSE, type SSELine } from '@/hooks/useSSE'
import { useDerivedState } from '@/hooks/useDerivedState'
import { SSEDialogFooter, SSEExecutionSection } from '@/components/SSEModal'
import { cn } from '@/lib/utils'
import { adminFetch } from '@/lib/api'
import {
  panelDialogContent,
  panelDialogFooter,
  panelDialogHeaderSolid,
} from '@/lib/panel-dialog'
import { useRumStatus } from '@/hooks/useRumStatus'
import { queryKeys } from '@/lib/query-keys'

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

/**
 * Remounts the body on every open so toggles, logs and execution state start fresh
 * without an effect that resets state.
 */
export function RumSettingsDialog(props: RumSettingsDialogProps) {
  const { open } = props
  const [generation, setGeneration] = useState(0)
  const [wasOpen, setWasOpen] = useState(open)
  if (open !== wasOpen) {
    setWasOpen(open)
    if (open) setGeneration((g) => g + 1)
  }
  return <RumSettingsDialogBody key={generation} {...props} />
}

function RumSettingsDialogBody({
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
  const [selectedVersion, setSelectedVersion] = useDerivedState<string>(
    currentVersion ?? latestVersion ?? availableVersions[0] ?? ''
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
  const { data: statusData, isLoading: isStatusLoading } = useRumStatus(serviceId, { enabled: open })

  const initializedRef = useRef<string | null>(null)

  // Set initial toggles when loaded
  useEffect(() => {
    if (statusData && !statusData.unauthorized && serviceId && initializedRef.current !== serviceId) {
      setCaptureVitals(statusData.capture_vitals ?? true)
      setCapturePerformance(statusData.capture_performance ?? true)
      setCaptureErrors(statusData.capture_errors ?? true)
      setCaptureEvents(statusData.capture_events ?? true)
      setCustomCondition(statusData.custom_condition ?? '')
      initializedRef.current = serviceId
    }
  }, [statusData, serviceId])

  const { lines: sseLines, status: sseStatus, error: sseError, start: sseStart, stop: sseStop } = useSSE()

  // Invalidate queries when done
  useEffect(() => {
    const isDone = sseStatus === 'done' || syncStatus === 'done'
    if (isDone && !completedRef.current) {
      completedRef.current = true
      onComplete?.()
      queryClient.invalidateQueries({ queryKey: queryKeys.rumStatus(serviceId) })
      queryClient.invalidateQueries({ queryKey: queryKeys.rumVersions(serviceId) })
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
      const payload = {
        capture_vitals: captureVitals,
        capture_performance: capturePerformance,
        capture_errors: captureErrors,
        capture_events: captureEvents,
        custom_condition: customCondition,
      }
      const settingsRes = await adminFetch(`/api/services/${serviceId}/rum/settings`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
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
        '✅ Fastly Edge VCL reconciled and deployed successfully.',
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
    } catch (err) {
      setSyncStatus('error')
      const errorMsg = err instanceof Error ? err.message : String(err)
      setSyncError(errorMsg || 'An error occurred while updating settings')
      setLocalLogs((prev) => [...prev, `❌ Error: ${errorMsg || 'An error occurred'}`])
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
            <SSEExecutionSection
              title="Applying RUM Settings"
              subtitle="Configuring capture options and publishing the revised tracking wrapper to Object Storage..."
              lines={displayedLines}
              status={currentStatus}
              error={currentError}
              heightClassName="h-[300px]"
              progressLabel="Task Log"
              doneMessage={
                selectedVersion !== currentVersion
                  ? `Faro Web SDK upgraded to v${selectedVersion} and RUM capture settings updated successfully!`
                  : "RUM capture settings and tracker JS wrapper updated successfully!"
              }
            />
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
                      An additional VCL condition, AND-ed onto{" "}
                      <code>req.url.path == &quot;/rum-beacon&quot;</code>. It is evaluated on the beacon
                      request itself, so <strong>a condition on <code>req.url.path</code> can never work</strong> —
                      the path is always <code>/rum-beacon</code>, making it either always true (a no-op) or
                      always false (which silently disables all RUM logging). The page URL lives in the beacon&apos;s
                      POST body, which VCL cannot read; filter by page in the tracker instead. Useful here:
                      client, geo, or header predicates, e.g.{" "}
                      <code>client.geo.country_code != &quot;US&quot;</code>. Strings must use double quotes.
                    </p>
                  </div>
                  <Input
                    id="rum-custom-condition"
                    placeholder='e.g. req.http.User-Agent !~ "(bot|crawler)"'
                    value={customCondition}
                    onChange={(e) => setCustomCondition(e.target.value)}
                    className="h-9 font-mono text-xs mt-2"
                  />
                </div>
              </div>

              {/* Version picker section */}
              <div className="space-y-4 border-t pt-5">
                <RumVersionPicker
                  serviceId={serviceId}
                  value={selectedVersion || null}
                  onChange={(v) => setSelectedVersion(v ?? '')}
                  availableVersions={availableVersions}
                  currentVersion={currentVersion}
                  latestVersion={latestVersion}
                />

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
            <SSEDialogFooter
              status={currentStatus}
              onClose={() => handleClose(false)}
              onStop={selectedVersion !== currentVersion ? sseStop : undefined}
              stopDisabled={selectedVersion === currentVersion}
              doneLabel="Close"
              idleOrErrorLabel="Close"
            />
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
