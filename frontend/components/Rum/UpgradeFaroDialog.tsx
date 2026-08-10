'use client'

import React, { useState, useEffect, useRef } from 'react'
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { ArrowRight, ArrowUpCircle, AlertCircle } from 'lucide-react'
import { useSSE } from '@/hooks/useSSE'
import { SSEProgressView } from '@/components/SSEModal'
import { cn } from '@/lib/utils'
import {
  panelDialogContent,
  panelDialogFooter,
  panelDialogHeaderSolid,
} from '@/lib/panel-dialog'

interface UpgradeFaroDialogProps {
  serviceId: string | null
  open: boolean
  onOpenChange: (open: boolean) => void
  availableVersions: string[]
  currentVersion: string | null
  latestVersion: string | null
  onComplete?: () => void
}

export function UpgradeFaroDialog({
  serviceId,
  open,
  onOpenChange,
  availableVersions,
  currentVersion,
  latestVersion,
  onComplete,
}: UpgradeFaroDialogProps) {
  const [isExecuting, setIsExecuting] = useState(false)
  const [selectedVersion, setSelectedVersion] = useState<string>(
    latestVersion ?? availableVersions[0] ?? ''
  )
  // Guards the completion effect below against firing twice for a single
  // 'done' status (e.g. a parent re-render after invalidateQueries changes
  // the onComplete identity), and re-arms once the dialog moves off 'done'
  // so a second upgrade in the same session still invalidates.
  const completedRef = useRef(false)

  const { lines, status, error: sseError, start, stop, reset } = useSSE()

  // Re-default the picker to `latest` every time the dialog opens fresh —
  // the operator asked to *choose* a version, not silently repeat whatever
  // was picked last time. Adjusted during render (React's documented
  // alternative to an effect for "reset state when a prop changes") rather
  // than in a useEffect, since setState synchronously inside an effect body
  // triggers react-hooks/set-state-in-effect (cascading-render risk).
  const [prevOpen, setPrevOpen] = useState(open)
  if (open !== prevOpen) {
    setPrevOpen(open)
    if (open) {
      setSelectedVersion(latestVersion ?? availableVersions[0] ?? '')
    }
  }

  // Reset isExecuting/SSE state on close, same as EnableRumDialog/DisableRumDialog.
  useEffect(() => {
    if (!open) {
      const id = setTimeout(() => {
        setIsExecuting(false)
        reset()
      }, 300)
      return () => clearTimeout(id)
    }
  }, [open, reset])

  // Invalidate the now-stale rum-versions/rum-status queries as soon as the
  // stream reports success, rather than gating it behind the operator
  // manually closing the dialog (unlike Enable/DisableRumDialog's
  // reload-on-close) — the version card should reflect the new pin
  // immediately, with the dialog still open showing the completed log.
  useEffect(() => {
    if (status === 'done' && !completedRef.current) {
      completedRef.current = true
      onComplete?.()
    }
    if (status !== 'done') {
      completedRef.current = false
    }
  }, [status, onComplete])

  if (!serviceId) return null

  const noVersions = availableVersions.length === 0
  const isNoOp = !selectedVersion

  const handleExecute = () => {
    if (noVersions || isNoOp || status === 'streaming') return
    setIsExecuting(true)
    start(`/api/services/${serviceId}/rum/upgrade`, {
      version: selectedVersion,
      activate: true,
    })
  }

  const handleClose = (isOpen: boolean) => {
    if (status === 'streaming') return
    onOpenChange(isOpen)
  }

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className={cn("sm:max-w-xl", panelDialogContent)} showCloseButton={status !== 'streaming'}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <DialogTitle className="flex items-center gap-2 text-primary text-xl font-bold">
            <ArrowUpCircle className="h-6 w-6 text-primary" />
            Upgrade Faro Web SDK
          </DialogTitle>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto min-h-0">
          {isExecuting ? (
            <div className="p-8 space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
              <div className="text-center space-y-2">
                <h3 className="text-lg font-semibold tracking-tight">Upgrading Faro Web SDK</h3>
                <p className="text-sm text-muted-foreground">
                  Fetching v{selectedVersion} and reconciling the deployed bundle in Object Storage...
                </p>
              </div>

              <SSEProgressView
                lines={lines}
                status={status}
                error={sseError}
                className="h-[300px]"
                progressLabel="Progress"
                doneMessage={`Faro Web SDK upgraded to v${selectedVersion} successfully!`}
              />
            </div>
          ) : (
            <div className="p-8 space-y-6">
              <Alert className="border-blue-300 bg-blue-50/60 dark:bg-blue-950/20 text-blue-900 dark:text-blue-300">
                <AlertCircle className="h-4 w-4 text-blue-600 dark:text-blue-400" />
                <AlertTitle className="text-sm font-bold">Choose a Faro Web SDK version</AlertTitle>
                <AlertDescription className="text-[13px] ml-1 font-medium mt-1">
                  Pin this service to a different self-hosted bundle and re-sync it to Object Storage.
                </AlertDescription>
              </Alert>

              {noVersions ? (
                <p className="text-sm text-muted-foreground">
                  No versions available from the registry right now.
                </p>
              ) : (
                <>
                  <div className="space-y-1.5">
                    <Label htmlFor="upgrade-faro-version" className="text-xs font-semibold">
                      Target version
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

                  <div className="flex items-center justify-center gap-3 rounded-md bg-muted/40 border p-4 text-sm font-mono">
                    <span className="text-muted-foreground">{currentVersion ?? 'unpinned'}</span>
                    <ArrowRight className="h-4 w-4 text-muted-foreground shrink-0" />
                    <span className="font-semibold">{selectedVersion || '—'}</span>
                  </div>
                </>
              )}
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
                disabled={noVersions || isNoOp}
              >
                {selectedVersion === currentVersion ? 'Confirm & Re-deploy' : 'Confirm & Upgrade'}
              </Button>
            </>
          ) : (
            <>
              {status !== 'streaming' && (
                <Button variant="outline" onClick={() => handleClose(false)} className="h-10 px-6">
                  Close
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
