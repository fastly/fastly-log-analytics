'use client'

import React, { useState } from 'react'
import { useOpenGeneration } from '@/hooks/useDerivedState'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { ShieldCheck, AlertCircle } from 'lucide-react'
import { useSSE } from '@/hooks/useSSE'
import { SSEActionDialogShell, SSEExecutionSection } from '@/components/SSEModal'
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
