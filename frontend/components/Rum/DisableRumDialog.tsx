'use client'

import React, { useState } from 'react'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter
} from '@/components/ui/dialog'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import { AlertTriangle, Trash2, AlertCircle } from 'lucide-react'
import { useSSE } from '@/hooks/useSSE'
import { SSEDialogFooter, SSEExecutionSection } from '@/components/SSEModal'
import { cn } from '@/lib/utils'
import {
  panelDialogContent,
  panelDialogFooter,
  panelDialogHeaderSolid,
} from '@/lib/panel-dialog'
import type { ServiceConfig } from '@/types/api'


interface DisableRumDialogProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
  onComplete?: () => void
}

export function DisableRumDialog(props: DisableRumDialogProps) {
  const { service, open } = props
  const [generation, setGeneration] = useState(0)
  const [wasOpen, setWasOpen] = useState(open)
  if (open !== wasOpen) {
    setWasOpen(open)
    if (open) setGeneration((g) => g + 1)
  }
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
          <DialogTitle className="flex items-center gap-2 text-destructive text-xl font-bold">
            <AlertTriangle className="h-6 w-6" />
            Disable RUM: {service.name}
          </DialogTitle>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto min-h-0">
          {isExecuting ? (
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
          ) : (
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
          )}
        </div>

        <DialogFooter className={panelDialogFooter}>
          {!isExecuting ? (
            <>
              <Button variant="ghost" onClick={() => onOpenChange(false)} className="h-10 px-6 text-xs">Cancel</Button>
              <Button
                variant="destructive"
                className="h-10 px-8 font-bold"
                onClick={handleExecute}
              >
                Disable RUM
              </Button>
            </>
          ) : (
            <SSEDialogFooter
              status={status}
              onClose={() => handleClose(false)}
              onStop={stop}
              doneLabel="Close & Reload"
              idleOrErrorLabel="Cancel"
            />
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
