'use client'

import React, { useState, useEffect, useRef, useMemo } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { formatBytes, cn } from "@/lib/utils";
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { Input } from '@/components/ui/input'
import { ArrowRight, Settings2, Sparkles, ShieldCheck, AlertCircle, AlertTriangle, Trash2 } from 'lucide-react'
import { RumVersionPicker } from '@/components/ProvisionWizard/steps/StorageStep'
import { useSSE, type SSELine } from '@/hooks/useSSE'
import { useDerivedState, useOpenGeneration } from '@/hooks/useSSE'
import { SSEActionDialogShell, SSEExecutionSection } from '@/components/SSEModal'
import { adminFetch, queryKeys } from "@/lib/api";
import { useRumStatus } from '@/hooks/useDashboardCards'
import type { ServiceConfig } from '@/types/api'

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
  const generation = useOpenGeneration(open)
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

  const [initializedServiceId, setInitializedServiceId] = useState<string | null>(null)
  if (statusData && !statusData.unauthorized && serviceId && initializedServiceId !== serviceId) {
    setInitializedServiceId(serviceId)
    setCaptureVitals(statusData.capture_vitals ?? true)
    setCapturePerformance(statusData.capture_performance ?? true)
    setCaptureErrors(statusData.capture_errors ?? true)
    setCaptureEvents(statusData.capture_events ?? true)
    setCustomCondition(statusData.custom_condition ?? '')
  }

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
    <SSEActionDialogShell
      open={open}
      onOpenChange={onOpenChange}
      reloadOnDone={false}
      status={currentStatus}
      onStop={selectedVersion !== currentVersion ? sseStop : undefined}
      stopDisabled={selectedVersion === currentVersion}
      isExecuting={isExecuting}
      titleClassName="text-primary"
      title={
        <>
          <Settings2 className="h-6 w-6 text-primary" />
          RUM Tracking Settings
        </>
      }
      headerRight={
        !isExecuting && !isStatusLoading ? (
          <div className="text-xs font-mono text-muted-foreground bg-muted/50 px-2.5 py-1 rounded-md border shrink-0">
            Est. ~{formatBytes(estimatedBytes)} / line
          </div>
        ) : undefined
      }
      executionSection={
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
      }
      confirmButton={
        <Button
          variant="default"
          className="h-10 px-8 font-bold bg-primary hover:bg-primary/90"
          onClick={handleExecute}
          disabled={noVersions || isNoOp || isStatusLoading}
        >
          Apply Settings
        </Button>
      }
      doneLabel="Close"
      idleOrErrorLabel="Close"
    >
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
    </SSEActionDialogShell>
  )
}

interface EnableRumDialogProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
  onComplete?: () => void
}

export function EnableRumDialog(props: EnableRumDialogProps) {
  const { service, open } = props
  const generation = useOpenGeneration(open)
  return <EnableRumDialogBody key={`${service?.service_id ?? ''}|${generation}`} {...props} />
}

function EnableRumDialogBody({ service, open, onOpenChange, onComplete }: EnableRumDialogProps) {
  const [isExecuting, setIsExecuting] = useState(false)

  const { lines, status, error: sseError, start, stop } = useSSE()

  if (!service) return null

  const handleExecute = () => {
    setIsExecuting(true)
    const enableUrl = `/api/services/${service.service_id}/rum/enable`
    start(enableUrl, {})
  }

  return (
    <SSEActionDialogShell
      open={open}
      onOpenChange={onOpenChange}
      onComplete={onComplete}
      status={status}
      onStop={stop}
      isExecuting={isExecuting}
      titleClassName="text-primary"
      doneLabel="Close & Reload"
      title={<><ShieldCheck className="h-6 w-6 text-primary" />Enable RUM: {service.name}</>}
      executionSection={
        <SSEExecutionSection
          title="Enabling Real User Monitoring"
          subtitle="Adding VCL snippets and custom fields to your Fastly service configuration..."
          lines={lines}
          status={status}
          error={sseError}
          heightClassName="h-[300px]"
          progressLabel="Progress"
          doneMessage="Real User Monitoring enabled successfully!"
        />
      }
      confirmButton={
        <Button variant="default" className="h-10 px-8 font-bold bg-primary hover:bg-primary/90" onClick={handleExecute}>
          Confirm & Enable RUM
        </Button>
      }
    >
      <div className="p-8 space-y-6">
        <Alert className="border-blue-300 bg-blue-50/60 dark:bg-blue-950/20 text-blue-900 dark:text-blue-300">
          <AlertCircle className="h-4 w-4 text-blue-600 dark:text-blue-400" />
          <AlertTitle className="text-sm font-bold">Enabling Real User Monitoring</AlertTitle>
          <AlertDescription className="text-[13px] ml-1 font-medium mt-1">
            This will configure your Fastly service to capture and process RUM beacons from your clients.
          </AlertDescription>
        </Alert>

        <div className="space-y-4 text-sm text-muted-foreground leading-relaxed">
          <p>
            Enabling RUM will inject specialized <strong>VCL subroutines</strong> and register
            required <strong>custom log fields</strong> into your service.
          </p>
          <p>
            Once enabled, client beacons hitting the <code>/rum-beacon</code> endpoint will be processed and logged
            for real-time performance analytics, including core web vitals, network latency, and connection quality.
          </p>
          <div className="rounded-md bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-900/50 p-4 text-[13px]">
            <strong className="text-amber-800 dark:text-amber-400">Important Note:</strong>
            <p className="mt-1 text-amber-700 dark:text-amber-300">
              A new version of your Fastly service will be created and deployed. The edge propagation usually takes less than 30 seconds.
            </p>
          </div>
        </div>
      </div>
    </SSEActionDialogShell>
  )
}

interface DisableRumDialogProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
  onComplete?: () => void
}

export function DisableRumDialog(props: DisableRumDialogProps) {
  const { service, open } = props
  const generation = useOpenGeneration(open)
  return <DisableRumDialogBody key={`${service?.service_id ?? ''}|${generation}`} {...props} />
}

function DisableRumDialogBody({ service, open, onOpenChange, onComplete }: DisableRumDialogProps) {
  const [removeCloudFiles, setRemoveCloudFiles] = useState(true)
  const [removeBucket, setRemoveBucket] = useState(false)
  const [isExecuting, setIsExecuting] = useState(false)

  const { lines, status, error: sseError, start, stop } = useSSE()

  if (!service) return null

  // Check if standard request logging is enabled. If so, we gray out bucket deletion.
  const isLoggingEnabled = service.logging_enabled ?? true

  const handleExecute = () => {
    setIsExecuting(true)
    const disableUrl = `/api/services/${service.service_id}/rum/disable`
    start(disableUrl, {
      remove_cloud_files: removeCloudFiles,
      remove_bucket: isLoggingEnabled ? false : removeBucket,
    })
  }

  return (
    <SSEActionDialogShell
      open={open}
      onOpenChange={onOpenChange}
      onComplete={onComplete}
      status={status}
      onStop={stop}
      isExecuting={isExecuting}
      doneLabel="Close & Reload"
      title={<><AlertTriangle className="h-6 w-6" />Disable RUM: {service.name}</>}
      executionSection={
        <SSEExecutionSection
          title="Disabling Real User Monitoring"
          subtitle="Removing VCL snippets and custom fields. Please wait..."
          lines={lines}
          status={status}
          error={sseError}
          heightClassName="h-[300px]"
          progressLabel="Progress"
          doneMessage="Real User Monitoring disabled successfully!"
        />
      }
      confirmButton={
        <Button variant="destructive" className="h-10 px-8 font-bold" onClick={handleExecute}>
          Disable RUM
        </Button>
      }
    >
      <div className="p-8 space-y-8">
        <Alert variant="destructive" className="bg-destructive/5 text-destructive border-destructive/20">
          <AlertCircle className="h-4 w-4" />
          <AlertDescription className="text-[13px] ml-1 font-medium">
            This will deactivate RUM. RUM beacon collection VCL and custom fields will be permanently removed from your Fastly service configuration.
          </AlertDescription>
        </Alert>

        <div className="space-y-4">
          <div className="flex items-center gap-2 mb-1">
            <Trash2 className="h-4 w-4 text-muted-foreground" />
            <h4 className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Resource Removal Options</h4>
          </div>

          <div className="space-y-3 bg-destructive/5 border border-destructive/10 rounded-lg p-4">
            <div className="flex items-center justify-between group">
              <div className="space-y-0.5">
                <Label htmlFor="rum-rem-cloud" className="text-sm font-medium cursor-pointer">Delete RUM cloud files</Label>
                <p className="text-[10px] text-muted-foreground">Permanently deletes RUM logs stored under raw/rum prefix.</p>
              </div>
              <Switch id="rum-rem-cloud" checked={removeCloudFiles} onCheckedChange={setRemoveCloudFiles} />
            </div>

            <div className="flex items-center justify-between group">
              <div className="space-y-0.5">
                <Label htmlFor="rum-rem-buck" className={cn("text-sm font-medium cursor-pointer", isLoggingEnabled && "text-muted-foreground cursor-not-allowed")}>Delete FOS bucket and all data</Label>
                {isLoggingEnabled ? (
                  <p className="text-[10px] text-amber-500 font-medium">Standard Request Logging is still active and using this bucket, so it cannot be deleted.</p>
                ) : (
                  <p className="text-[10px] text-muted-foreground text-destructive font-medium">Warning: This cannot be undone. All logs will be lost.</p>
                )}
              </div>
              <Switch id="rum-rem-buck" checked={isLoggingEnabled ? false : removeBucket} onCheckedChange={setRemoveBucket} disabled={isLoggingEnabled} />
            </div>
          </div>
        </div>
      </div>
    </SSEActionDialogShell>
  )
}
