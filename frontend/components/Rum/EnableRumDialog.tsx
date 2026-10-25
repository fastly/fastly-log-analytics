'use client'

import React, { useState } from 'react'
import { useOpenGeneration } from '@/hooks/useSSE'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import { ShieldCheck, AlertCircle, AlertTriangle, Trash2 } from 'lucide-react'
import { useSSE } from '@/hooks/useSSE'
import { SSEActionDialogShell, SSEExecutionSection } from '@/components/SSEModal'
import { cn } from '@/lib/utils'
import type { ServiceConfig } from '@/types/api'

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
