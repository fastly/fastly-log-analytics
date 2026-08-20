'use client'

import React, { useState, useEffect } from 'react'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter
} from '@/components/ui/dialog'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { ShieldCheck, AlertCircle } from 'lucide-react'
import { useSSE } from '@/hooks/useSSE'
import { SSEProgressView } from '@/components/SSEModal'
import { cn } from '@/lib/utils'
import {
  panelDialogContent,
  panelDialogFooter,
  panelDialogHeaderSolid,
} from '@/lib/panel-dialog'
import type { components } from '@/types/api.generated'

type ServiceConfig = components["schemas"]["ServiceConfig"]

interface EnableRumDialogProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
  onComplete?: () => void
}

export function EnableRumDialog({ service, open, onOpenChange, onComplete }: EnableRumDialogProps) {
  const [isExecuting, setIsExecuting] = useState(false)

  const { lines, status, error: sseError, start, stop, reset } = useSSE()

  // Reset state when dialog closes
  useEffect(() => {
    if (!open) {
      const id = setTimeout(() => {
        setIsExecuting(false)
        reset()
      }, 300)
      return () => clearTimeout(id)
    }
  }, [open, reset])

  if (!service) return null

  const handleExecute = () => {
    setIsExecuting(true)
    const enableUrl = `/api/services/${service.service_id}/rum/enable`
    start(enableUrl, {})
  }

  const handleClose = (isOpen: boolean) => {
    if (status === 'streaming') return
    onOpenChange(isOpen)
    if (!isOpen && status === 'done') {
      if (onComplete) onComplete()
      window.location.reload()
    }
  }

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className={cn("sm:max-w-xl", panelDialogContent)} showCloseButton={status !== 'streaming'}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <DialogTitle className="flex items-center gap-2 text-primary text-xl font-bold">
            <ShieldCheck className="h-6 w-6 text-primary" />
            Enable RUM: {service.name}
          </DialogTitle>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto min-h-0">
          {isExecuting ? (
            <div className="p-8 space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
               <div className="text-center space-y-2">
                  <h3 className="text-lg font-semibold tracking-tight">Enabling Real User Monitoring</h3>
                  <p className="text-sm text-muted-foreground">Adding VCL snippets and custom fields to your Fastly service configuration...</p>
               </div>

               <SSEProgressView
                 lines={lines}
                 status={status}
                 error={sseError}
                 className="h-[300px]"
                 progressLabel="Progress"
                 doneMessage="Real User Monitoring enabled successfully!"
               />
            </div>
          ) : (
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
              >
                Confirm & Enable RUM
              </Button>
            </>
          ) : (
            <>
              {status !== 'streaming' && (
                 <Button variant="outline" onClick={() => handleClose(false)} className="h-10 px-6">
                   {status === 'done' ? 'Close & Reload' : 'Cancel'}
                 </Button>
              )}
              {status === 'streaming' && (
                <Button variant="outline" onClick={stop} className="h-10 px-6">Stop</Button>
              )}
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
